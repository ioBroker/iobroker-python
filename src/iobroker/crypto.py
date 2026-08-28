"""Decryption of configuration values.

ioBroker stores password fields in an instance's ``native`` section encrypted with the system
secret from ``system.config``. An adapter that cannot decrypt them cannot use credentials at all,
so this is not optional for anything talking to a device or a service.

Two formats exist and both are still in the wild:

* **AES-192-CBC** -- ``$/aes-192-cbc:<iv-hex>:<ciphertext-hex>``, used when the system secret is a
  48 character hex string. This is what current installations produce.
* **Legacy XOR** -- everything else, kept because installations set up years ago still carry values
  in that form and the secret is then not hex.

The choice between them is made exactly the way js-controller makes it, since guessing differently
would silently return rubbish instead of failing.
"""

from __future__ import annotations

import re
from typing import Any

__all__ = ["decrypt", "AES_PREFIX"]

#: Marker that identifies an AES encrypted value.
AES_PREFIX = "$/aes-192-cbc:"

_HEX_SECRET = re.compile(r"^[0-9a-f]{48}$")


def decrypt(secret: str, value: str) -> str:
    """Decrypt a configuration value.

    :param secret: the system secret from ``system.config`` → ``native.secret``
    :param value: the stored value, in either format
    :returns: the plain text
    """
    if not value.startswith(AES_PREFIX) or not _HEX_SECRET.match(secret or ""):
        return _decrypt_legacy(secret, value)

    return _decrypt_aes(secret, value)


def _decrypt_aes(secret: str, value: str) -> str:
    """Decrypt the AES-192-CBC form.

    ``cryptography`` is an optional dependency, and deliberately so: it is a compiled package,
    which on a Raspberry Pi is the difference between a fast install and a long build. Adapters
    without encrypted settings should not pay for it, so the requirement is only raised at the
    moment a value actually needs it -- with a message naming the fix.
    """
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError as exc:  # pragma: no cover - depends on the installation
        raise RuntimeError(
            "This value is AES encrypted, which needs the 'cryptography' package. "
            'Add iobroker[crypto] to the adapter dependencies.'
        ) from exc

    # "$/aes-192-cbc:<iv>:<ciphertext>" -- split from the left, the payload may not contain colons
    # but the prefix itself does.
    parts = value.split(":", 2)
    iv = bytes.fromhex(parts[1])
    ciphertext = bytes.fromhex(parts[2])

    decryptor = Cipher(algorithms.AES(bytes.fromhex(secret)), modes.CBC(iv)).decryptor()
    padded = decryptor.update(ciphertext) + decryptor.finalize()

    # PKCS#7, which is what Node's createDecipheriv strips automatically.
    return padded[: -padded[-1]].decode()


def _decrypt_legacy(secret: str, value: str) -> str:
    """Decrypt the old XOR form.

    Symmetric, so the same routine encrypts. Weak by any standard, but it is what those values are
    and reading them is the only way to keep an old installation working.
    """
    if not secret:
        return value

    return "".join(chr(ord(secret[i % len(secret)]) ^ ord(c)) for i, c in enumerate(value))


def decrypt_native(secret: str, native: dict[str, Any], keys: list[str] | None) -> dict[str, Any]:
    """Decrypt the entries an adapter declared as encrypted.

    :param secret: the system secret
    :param native: the instance's ``native`` section; not modified
    :param keys: ``common.encryptedNative`` from the instance object
    :returns: a copy with those entries decrypted
    """
    result = dict(native)

    for key in keys or []:
        value = result.get(key)
        # Only strings are encrypted; a number or a bool in the list is a mistake in the
        # adapter's io-package.json rather than something to fail over.
        if isinstance(value, str) and value:
            result[key] = decrypt(secret, value)

    return result
