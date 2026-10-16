import sys

import httpx
import pytest

from generation_cut import harness


def launch_child(monkeypatch, script):
    popen = harness.subprocess.Popen
    children = []

    def launch(command, **kwargs):
        child = popen([sys.executable, "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(harness.subprocess, "Popen", launch)
    return children


def test_startup_waits_for_a_successful_reset(monkeypatch):
    children = launch_child(monkeypatch, "import time; time.sleep(60)")
    statuses = iter([503, 200])
    probes = []

    def probe(url, *, timeout):
        probes.append(url)
        return httpx.Response(next(statuses), request=httpx.Request("POST", url))

    monkeypatch.setattr(harness.httpx, "post", probe)
    with harness.ServerProc() as server:
        assert probes == [server.url + "/_reset"] * 2
    assert children[0].poll() is not None


def test_startup_reports_child_exit_and_stderr(monkeypatch):
    children = launch_child(monkeypatch, "import sys; print('broken app', file=sys.stderr); sys.exit(7)")

    def probe(url, *, timeout):
        raise httpx.ConnectError("not listening")

    monkeypatch.setattr(harness.httpx, "post", probe)
    with pytest.raises(RuntimeError, match=r"(?s)exited during startup \(code 7\).*broken app"):
        harness.ServerProc()
    assert children[0].poll() == 7


def test_startup_timeout_terminates_the_child(monkeypatch):
    children = launch_child(monkeypatch, "import time; time.sleep(60)")
    monkeypatch.setattr(harness, "SERVER_START_TIMEOUT", 0.2)
    monkeypatch.setattr(harness.httpx, "post", lambda url, **kwargs: httpx.Response(
        503, request=httpx.Request("POST", url),
    ))

    with pytest.raises(RuntimeError, match="not ready within 0.2 s") as exc:
        harness.ServerProc()
    assert isinstance(exc.value.__cause__, httpx.HTTPStatusError)
    assert children[0].poll() is not None
