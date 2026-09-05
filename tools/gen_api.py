"""Generate ``docs/API.md`` from the package's own docstrings.

Written rather than kept by hand for one reason: a reference that is maintained separately from
the code is a reference that is wrong within a month. Everything here comes out of the source --
signatures from ``inspect``, prose from the docstrings, the headings from the ``# -- Section ---``
comments the modules are already divided by, and the field descriptions from the ``#:`` comments
above each dataclass field.

    python tools/gen_api.py            # write docs/API.md
    python tools/gen_api.py --check    # fail if it is out of date (this is what CI runs)

Sphinx would be the other answer, and a heavier one: it needs a toolchain, a theme and a hosting
decision, and what an adapter author wants is one page they can read on GitHub. That page is what
this writes.
"""

from __future__ import annotations

import argparse
import inspect
import io
import pathlib
import re
import sys
import tokenize
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
OUT = ROOT / "docs" / "API.md"

sys.path.insert(0, str(SRC))

import iobroker  # noqa: E402
from iobroker import adapter as adapter_mod  # noqa: E402
from iobroker import connection as connection_mod  # noqa: E402
from iobroker import crypto as crypto_mod  # noqa: E402
from iobroker import exit_codes as exit_codes_mod  # noqa: E402
from iobroker import files as files_mod  # noqa: E402
from iobroker import protection as protection_mod  # noqa: E402
from iobroker import types as types_mod  # noqa: E402

#: The order the page is written in: the class an adapter subclasses first, then the values it
#: handles, then the machinery underneath. Not alphabetical -- alphabetical would open with
#: ``check_protocol``, which nobody needs to read first.
MODULES = [
    (adapter_mod, "Adapter", "The class every Python adapter subclasses."),
    (types_mod, "States and messages", "What travels between adapters."),
    (files_mod, "Files", "The file store and the helpers around its keys."),
    (connection_mod, "Connection", "Finding the databases and talking to them."),
    (crypto_mod, "Encrypted settings", "Reading the values ioBroker stores encrypted."),
    (protection_mod, "Protected settings", "Withholding another adapter's private entries."),
    (exit_codes_mod, "Exit codes", "What the controller reads out of a stopped process."),
]

#: Section comments the modules are divided by: ``# -- States ------``. The title has to start
#: with something other than a dash, or a plain rule of dashes would be read as a section called
#: "-".
SECTION = re.compile(r"^#\s*--\s*([^-\s].*?)\s*-{2,}\s*$")

#: A ``#:`` comment, which is how the dataclass fields are documented.
FIELD_COMMENT = re.compile(r"^\s*#:\s?(.*)$")

# reST roles. Everything becomes plain code in Markdown: a link would have to point somewhere, and
# a single page has nowhere to point that a reader is not already looking at.
ROLE = re.compile(r":(?:meth|attr|class|func|data|mod|exc):`~?([^`]+)`")
LITERAL = re.compile(r"``([^`]+)``")

#: ``--`` used as an em dash: preceded by a space, followed by a space or the end of the line.
DASH = re.compile(r"(?<= )--(?= |$)")

PARAM = re.compile(r"^:param\s+(\*{0,2}[\w_]+):\s*(.*)$")
RETURNS = re.compile(r"^:returns?:\s*(.*)$")
RAISES = re.compile(r"^:raises\s+([\w.]+):\s*(.*)$")


def role_text(target: str) -> str:
    """Render a cross-reference as the name a reader would look for.

    ``:meth:`~iobroker.Adapter.set_state``` is about ``set_state``; the path in front of it is
    Sphinx bookkeeping and only gets in the way on a page that has no links.
    """
    name = target.rsplit(".", 1)[-1]
    # Only a name the package actually defines as a method gets the parentheses. Guessing from the
    # spelling would turn `instance_id` into a call.
    return f"`{name}()`" if name in _METHOD_NAMES else f"`{name}`"


def inline(text: str) -> str:
    """Turn one line of reST into one line of Markdown."""
    text = ROLE.sub(lambda m: role_text(m.group(1)), text)
    text = LITERAL.sub(lambda m: f"`{m.group(1)}`", text)
    # The em dash this codebase writes as ``--``. Markdown has no reason to keep the ASCII form.
    # At the end of a line as well as inside one -- the docstrings wrap wherever they wrap.
    return DASH.sub("—", text)


def split_fields(lines: list[str]) -> tuple[list[str], list[tuple[str, str]], str | None, list[tuple[str, str]]]:
    """Separate the prose from the ``:param:`` / ``:returns:`` / ``:raises:`` block.

    Continuation lines are indented, so a field runs until the next field or the next
    unindented line -- which is exactly how reST reads them too.
    """
    body: list[str] = []
    params: list[tuple[str, str]] = []
    returns: str | None = None
    raises: list[tuple[str, str]] = []

    current: list[str] | None = None

    def flush(target: list[str] | None) -> None:
        if target is not None:
            target[:] = [" ".join(part.strip() for part in target if part.strip())]

    for line in lines:
        if match := PARAM.match(line):
            current = [match.group(2)]
            params.append((match.group(1), current))  # type: ignore[arg-type]
            continue
        if match := RETURNS.match(line):
            current = [match.group(1)]
            returns = current  # type: ignore[assignment]
            continue
        if match := RAISES.match(line):
            current = [match.group(2)]
            raises.append((match.group(1), current))  # type: ignore[arg-type]
            continue
        if current is not None and line.strip() and line.startswith(" "):
            current.append(line)
            continue
        current = None
        body.append(line)

    joined_params = [(name, " ".join(p.strip() for p in parts if p.strip())) for name, parts in params]
    joined_raises = [(name, " ".join(p.strip() for p in parts if p.strip())) for name, parts in raises]
    joined_returns = " ".join(p.strip() for p in returns if p.strip()) if returns else None

    while body and not body[-1].strip():
        body.pop()

    return body, joined_params, joined_returns, joined_raises


def render_body(lines: list[str]) -> list[str]:
    """Render the prose part, turning reST literal blocks into fenced code.

    A paragraph ending in ``::`` introduces an indented block; in Markdown that becomes a fence,
    which is the one construct worth converting rather than passing through.
    """
    out: list[str] = []
    index = 0

    while index < len(lines):
        line = lines[index]

        if line.rstrip().endswith("::"):
            out.append(inline(line.rstrip()[:-1]))
            index += 1
            while index < len(lines) and not lines[index].strip():
                index += 1

            block: list[str] = []
            indent = len(lines[index]) - len(lines[index].lstrip()) if index < len(lines) else 0
            while index < len(lines) and (not lines[index].strip() or lines[index].startswith(" " * indent)):
                block.append(lines[index][indent:] if lines[index].strip() else "")
                index += 1
            while block and not block[-1].strip():
                block.pop()

            out.extend(["", "```python", *block, "```", ""])
            continue

        out.append(inline(line))
        index += 1

    return out


def signature_of(obj: Any, name: str) -> str:
    """The call signature as a reader would type it, without ``self`` or ``cls``."""
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):  # pragma: no cover - builtins have none
        return f"{name}(...)"

    parameters = [p for key, p in sig.parameters.items() if key not in ("self", "cls")]
    rendered = ", ".join(_parameter(p) for p in parameters)
    returns = "" if sig.return_annotation is inspect.Signature.empty else f" -> {_annotation(sig.return_annotation)}"
    prefix = "async " if inspect.iscoroutinefunction(obj) else ""

    return f"{prefix}{name}({rendered}){returns}"


def _parameter(parameter: inspect.Parameter) -> str:
    """One parameter, as it is written in the source rather than as ``inspect`` repr()s it."""
    text = parameter.name
    if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
        text = f"*{text}"
    elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
        text = f"**{text}"
    if parameter.annotation is not inspect.Parameter.empty:
        text += f": {_annotation(parameter.annotation)}"
    if parameter.default is not inspect.Parameter.empty:
        text += f" = {parameter.default!r}"
    return text


def _annotation(annotation: Any) -> str:
    """An annotation as source text.

    The package uses ``from __future__ import annotations``, so ``inspect`` hands these over as
    strings and ``str()`` would print them with their quotes -- ``id: 'str'``.
    """
    if isinstance(annotation, str):
        # A forward reference is stringified with its quotes still on: the source says
        # ``-> "State"``, so the annotation arrives as the five characters `"State"`.
        return annotation.strip("\"'")
    return getattr(annotation, "__name__", str(annotation))


def sections_of(module: Any) -> dict[int, str]:
    """Where each ``# -- Section ---`` comment sits in a module, by line number.

    The modules are already divided into the groups a reader thinks in -- states, objects, files,
    messages -- and reusing that division costs nothing and cannot drift from the source.
    """
    path = pathlib.Path(inspect.getfile(module))
    found: dict[int, str] = {}

    with path.open(encoding="utf-8") as handle:
        for token in tokenize.generate_tokens(handle.readline):
            if token.type == tokenize.COMMENT and (match := SECTION.match(token.string.strip())):
                found[token.start[0]] = match.group(1)

    return found


def field_docs(cls: type) -> dict[str, str]:
    """The ``#:`` comments above each dataclass field, by field name."""
    try:
        source = inspect.getsource(cls)
    except (OSError, TypeError):  # pragma: no cover
        return {}

    docs: dict[str, str] = {}
    pending: list[str] = []

    for line in source.splitlines():
        if match := FIELD_COMMENT.match(line):
            pending.append(match.group(1).strip())
            continue
        if declaration := re.match(r"^\s{4}([a-z_][\w_]*)\s*[:=]", line):
            if pending:
                docs[declaration.group(1)] = " ".join(pending)
            pending = []
            continue
        if line.strip():
            pending = []

    return docs


def doc_lines(obj: Any) -> list[str]:
    """The object's docstring, dedented, as a list of lines."""
    return inspect.cleandoc(obj.__doc__ or "").split("\n")


def render_callable(obj: Any, name: str, level: str) -> list[str]:
    """One method or function: heading, signature, prose, then its parameters."""
    body, params, returns, raises = split_fields(doc_lines(obj))
    out = [f"{level} {name}", "", "```python", signature_of(obj, name), "```", ""]
    out += render_body(body)

    if params:
        out += ["", "**Parameters**", ""]
        out += [f"- `{n}` — {inline(text)}" for n, text in params]
    if returns:
        out += ["", f"**Returns** — {inline(returns)}"]
    for exception, text in raises:
        out += ["", f"**Raises** `{exception}` — {inline(text)}"]

    return out + [""]


def public_members(cls: type) -> list[tuple[str, Any]]:
    """The methods a user of the class calls, in the order the source declares them.

    Source order rather than alphabetical: the modules put related methods together, and reading
    `set_state` next to `get_state` is the point of that.
    """
    members = [
        (name, value)
        for name, value in vars(cls).items()
        if not name.startswith("_") and (isinstance(value, (property, classmethod, staticmethod)) or callable(value))
    ]

    def line_of(item: tuple[str, Any]) -> int:
        code = getattr(_unwrap(item[1]), "__code__", None)
        return code.co_firstlineno if code else 0

    return sorted(members, key=line_of)


def _unwrap(value: Any) -> Any:
    """The function behind a property, a classmethod or a plain method.

    ``classmethod`` and ``staticmethod`` objects are not themselves callable in current Python, so
    a class that documents a ``from_wire`` constructor loses it without this.
    """
    if isinstance(value, property):
        return value.fget
    if isinstance(value, (classmethod, staticmethod)):
        return value.__func__
    return value


_METHOD_NAMES: set[str] = set()


def collect_method_names() -> None:
    """Every method name in the package, so a cross-reference can be rendered as a call.

    ``set_state`` should read as ``set_state()`` and ``namespace`` should not; the only reliable
    way to tell them apart is to know which of the two the package actually defines.
    """
    for module, _, _ in MODULES:
        for name in getattr(module, "__all__", []):
            value = getattr(module, name, None)
            if inspect.isclass(value):
                _METHOD_NAMES.update(n for n, _ in public_members(value))
            elif callable(value):
                # Module-level functions are called the same way and read the same way:
                # ``load_db_config()`` rather than ``load_db_config``.
                _METHOD_NAMES.add(name)
    _METHOD_NAMES.update(n for n, _ in public_members(adapter_mod._Log))


def render_class(cls: type, module: Any) -> list[str]:
    """A class with its fields and its methods, grouped by the module's own sections."""
    out = [f"### `{cls.__name__}`", ""]
    out += render_body(split_fields(doc_lines(cls))[0])
    out += [""]

    fields = field_docs(cls)
    if fields:
        out += ["**Fields**", ""]
        out += [f"- `{name}` — {inline(text)}" for name, text in fields.items()]
        out += [""]

    sections = sections_of(module)
    seen: set[str] = set()

    # One level deeper only where there are sections to nest under. A small class with no
    # ``# -- Section ---`` comments would otherwise skip a heading level for no reason.
    level = "#####" if sections else "####"

    for name, value in public_members(cls):
        function = _unwrap(value)
        code = getattr(function, "__code__", None)
        if code is not None and sections:
            heading = _section_for(sections, code.co_firstlineno)
            if heading and heading not in seen:
                seen.add(heading)
                out += [f"#### {heading}", ""]

        if isinstance(value, property):
            body, _, _, _ = split_fields(doc_lines(function))
            out += [f"{level} `{name}`", "", "*property*", ""] + render_body(body) + [""]
        else:
            prefix = "classmethod " if isinstance(value, classmethod) else ""
            rendered = render_callable(function, name, level)
            if prefix:
                rendered[3] = prefix + rendered[3]
            out += rendered

    return out


def _section_for(sections: dict[int, str], line: int) -> str | None:
    """The nearest ``# -- Section ---`` comment above ``line``."""
    candidates = [start for start in sections if start < line]
    return sections[max(candidates)] if candidates else None


def render() -> str:
    """The whole page."""
    collect_method_names()

    page = io.StringIO()
    page.write("<!-- Generated by tools/gen_api.py from the docstrings. Do not edit by hand. -->\n")
    page.write("# API reference\n\n")
    page.write(
        f"Every public class, method and function of `iobroker` {iobroker.__version__}, taken from "
        "the docstrings in the source. The README is the guide; this is the reference.\n\n"
    )

    page.write("## Contents\n\n")
    for module, title, _ in MODULES:
        anchor = title.lower().replace(" ", "-")
        page.write(f"- [{title}](#{anchor})\n")
    page.write("- [Logging](#logging)\n\n")

    for module, title, blurb in MODULES:
        page.write(f"## {title}\n\n{blurb}\n\n")

        for name in getattr(module, "__all__", []):
            value = getattr(module, name)
            if inspect.isclass(value):
                page.write("\n".join(render_class(value, module)) + "\n")
            elif callable(value):
                page.write("\n".join(["### `" + name + "`", ""] + render_callable(value, name, "####")[1:]) + "\n")
            else:
                doc = _constant_doc(module, name)
                page.write(f"### `{name}`\n\n```python\n{name} = {value!r}\n```\n\n")
                if doc:
                    page.write(inline(doc) + "\n\n")

    page.write("## Logging\n\n")
    page.write(
        "Reached as `self.log` from an adapter. The class is private, its five methods are the "
        "SDK's logging API.\n\n"
    )
    page.write("\n".join(render_body(split_fields(doc_lines(adapter_mod._Log))[0])) + "\n\n")
    for name, value in public_members(adapter_mod._Log):
        page.write("\n".join(render_callable(_unwrap(value), name, "###")) + "\n")

    return page.getvalue().replace("\n\n\n\n", "\n\n").replace("\n\n\n", "\n\n")


def _constant_doc(module: Any, name: str) -> str:
    """The ``#:`` comment above a module-level constant, if it has one."""
    source = inspect.getsource(module).splitlines()
    pending: list[str] = []

    for line in source:
        if match := FIELD_COMMENT.match(line):
            pending.append(match.group(1).strip())
            continue
        if re.match(rf"^{re.escape(name)}\s*[:=]", line):
            return " ".join(pending)
        if line.strip():
            pending = []

    return ""


def main() -> int:
    """Write the page, or check that the committed one still matches the source."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write; exit non-zero when docs/API.md is out of date",
    )
    args = parser.parse_args()

    page = render()

    if args.check:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != page:
            print("docs/API.md is out of date -- run: python tools/gen_api.py", file=sys.stderr)
            return 1
        print("docs/API.md is up to date.")
        return 0

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(page, encoding="utf-8", newline="\n")
    print(f"wrote {OUT.relative_to(ROOT)} ({len(page.splitlines())} lines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
