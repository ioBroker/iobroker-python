"""Tests for the object helpers that do not need a database.

The parts that do talk to one are exercised against a running installation instead; unit tests
here cover the logic that is easy to get subtly wrong and would then fail somewhere far away.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import time

import pytest

from iobroker.adapter import Adapter, _rss_mb


@pytest.fixture
def adapter() -> Adapter:
    return Adapter("demo", instance=0)


class TestAbsoluteIds:
    def test_prefixes_a_relative_id(self, adapter: Adapter) -> None:
        assert adapter._abs("temperature") == "demo.0.temperature"

    def test_leaves_our_own_namespace_alone(self, adapter: Adapter) -> None:
        # Prefixing twice would produce demo.0.demo.0.temperature, which silently writes to an
        # object nobody is watching.
        assert adapter._abs("demo.0.temperature") == "demo.0.temperature"

    def test_leaves_system_ids_alone(self, adapter: Adapter) -> None:
        assert adapter._abs("system.adapter.demo.0") == "system.adapter.demo.0"


class TestObjectView:
    @pytest.mark.asyncio
    async def test_rejects_designs_it_cannot_serve(self, adapter: Adapter) -> None:
        # Only the "system" design is implemented. Accepting another one and returning nothing
        # would look like "no objects exist" rather than "not supported".
        with pytest.raises(ValueError, match="system"):
            await adapter.get_object_view("custom", "state")


class TestResidentMemory:
    def test_reports_a_plausible_size_on_this_platform(self) -> None:
        """The number admin graphs as `memRss`, measured wherever the suite runs.

        The point of this test is the matrix: Linux reads procfs, Windows calls
        `GetProcessMemoryInfo`, macOS calls `libproc`, and each of those is code the other two
        never execute. A reader returning `None` used to be silent -- it left `memRss` empty in
        admin, which is how a Windows installation could run for months without a memory graph.
        """
        rss = _rss_mb()

        assert rss is not None, f"no reader answered on {sys.platform}"
        # A CPython process with redis-py loaded sits well above 5 MB, and 8 GB would mean the
        # reader answered in the wrong unit -- pages counted as bytes, say.
        assert 5 < rss < 8192, f"implausible resident size: {rss} MB"

    def test_grows_when_memory_is_held(self) -> None:
        """The number has to follow what the process is actually using."""
        before, during, after = self._measure()

        assert during > before + 32, f"64 MB held did not show: {before} -> {during}"

    @pytest.mark.skipif(
        sys.platform == "darwin",
        reason=(
            "macOS keeps freed pages resident until something needs them, so a release is not "
            "observable here -- measured on the CI runners, where before/after come back "
            "bit-identical. The growth half above still covers the reader itself."
        ),
    )
    def test_shrinks_again_when_it_is_released(self) -> None:
        """This is the half that tells a current reading from a peak.

        `ru_maxrss`, which this used to read, only ever grows: an adapter that allocated once would
        report that peak for the rest of its life, and a slow leak would be indistinguishable from
        a single large read at startup. An allocation of this size goes straight to the operating
        system, so freeing it hands the pages back -- on the platforms that hand them back at all.
        """
        _before, during, after = self._measure()

        assert after < during - 32, f"64 MB released did not show: {during} -> {after}"

    @staticmethod
    def _measure() -> tuple[float, float, float]:
        """Resident size before, while holding 64 MB, and after letting it go."""
        before = _rss_mb()
        ballast = bytearray(64 * 1024 * 1024)
        # Touch it: pages are resident once written to, not once allocated.
        ballast[::4096] = b"\x01" * len(ballast[::4096])
        during = _rss_mb()
        del ballast
        gc.collect()
        after = _rss_mb()

        assert before is not None and during is not None and after is not None
        return before, during, after


class TestCpuPercent:
    def test_answers_in_percent_of_one_core(self, adapter: Adapter) -> None:
        """Busy work between two reports has to show up as CPU.

        `cputime` is what the process accumulated; `cpu` is that divided by the wall clock that
        passed. Burning a whole interval on one thread is therefore close to 100, and the only
        thing asserted here is the part that cannot be a coincidence: it is well above zero.
        """
        adapter._cpu_percent()  # first call only sets the mark

        deadline = time.monotonic() + 0.2
        while time.monotonic() < deadline:
            pass

        percent = adapter._cpu_percent()

        assert percent is not None
        assert percent > 50, f"a busy loop reported {percent}% of one core"

    def test_an_idle_adapter_reports_almost_nothing(self, adapter: Adapter) -> None:
        adapter._cpu_percent()
        time.sleep(0.2)

        percent = adapter._cpu_percent()

        assert percent is not None
        # Sleeping is not CPU. Not asserted as exactly 0: the interpreter does some work of its
        # own, and a loaded CI machine makes the wall clock the denominator it is.
        assert percent < 20, f"an idle adapter reported {percent}% of one core"


class TestEventLoopLag:
    async def test_a_blocked_loop_is_measured(self, adapter: Adapter, monkeypatch) -> None:
        """The state that names the cause when an adapter stops responding.

        A synchronous call in a hook stops the loop, and everything else -- heartbeat, messagebox,
        subscriptions -- stops with it. The lag is what makes that visible instead of leaving a gap
        in the graphs that looks like a crash.
        """
        monkeypatch.setattr("iobroker.adapter._HEARTBEAT_STEPS", 2)
        monkeypatch.setattr("iobroker.adapter._HEARTBEAT_STEP_SECONDS", 0.05)

        async def block() -> None:
            await asyncio.sleep(0.01)
            time.sleep(0.25)  # exactly what an adapter must not do

        blocker = asyncio.create_task(block())
        lags = await adapter._wait_measuring_lag()
        await blocker

        assert len(lags) == 2
        assert max(lags) > 100, f"a quarter second of blocking measured as {lags} ms"

    async def test_an_idle_loop_reports_little(self, adapter: Adapter, monkeypatch) -> None:
        monkeypatch.setattr("iobroker.adapter._HEARTBEAT_STEPS", 2)
        monkeypatch.setattr("iobroker.adapter._HEARTBEAT_STEP_SECONDS", 0.05)

        lags = await adapter._wait_measuring_lag()

        assert len(lags) == 2
        # Timers are never exact, so this is about the order of magnitude: tens of milliseconds
        # would already mean something is holding the loop.
        assert max(lags) < 50, f"an idle loop lagged {lags} ms"

    async def test_stopping_ends_the_wait_immediately(self, adapter: Adapter) -> None:
        """A shutdown must not wait out the rest of the interval.

        The reason the period is fifteen one-second steps rather than one fifteen-second sleep:
        `stop()` during a beat used to leave the process running until the sleep expired.
        """
        adapter._stopping.set()

        started = time.monotonic()
        lags = await adapter._wait_measuring_lag()

        assert time.monotonic() - started < 1
        assert lags == []
