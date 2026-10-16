import asyncio, json, os, random, signal, socket, subprocess, sys, time
from tempfile import TemporaryFile
import httpx

PY = sys.executable
OPEN_LOAD_LIMITS = httpx.Limits(max_connections=2000, max_keepalive_connections=100)
SERVER_START_TIMEOUT = 10.0


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


class ServerProc:
    def __init__(self):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        with TemporaryFile() as stderr:
            self.p = subprocess.Popen(
                [PY, "-m", "uvicorn", "generation_cut.server:app", "--port", str(self.port), "--log-level", "error"],
                stdout=subprocess.DEVNULL, stderr=stderr, start_new_session=True,
            )
            try:
                self._wait_until_ready()
            except BaseException as exc:
                self.close()
                if isinstance(exc, RuntimeError):
                    stderr.seek(0)
                    details = stderr.read().decode(errors="replace").strip()
                    if details:
                        raise RuntimeError(f"{exc}\n{details}") from exc
                raise

    def _wait_until_ready(self):
        deadline = time.monotonic() + SERVER_START_TIMEOUT
        last_error = None
        while self.p.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                httpx.post(self.url + "/_reset", timeout=min(1, remaining)).raise_for_status()
            except httpx.HTTPError as exc:
                last_error = exc
            else:
                if self.p.poll() is None:
                    return
                break
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
        status = self.p.poll()
        if status is not None:
            raise RuntimeError(f"Benchmark server exited during startup (code {status}).")
        raise RuntimeError(f"Benchmark server was not ready within {SERVER_START_TIMEOUT:g} s.") from last_error

    def config(self, **cfg):
        httpx.post(self.url + "/_config", json=cfg, timeout=5).raise_for_status()

    def stats(self):
        return httpx.get(self.url + "/_stats", timeout=5).json()

    def close(self):
        # Graceful stop would wait for the queued backlog.
        if self.p.poll() is None:
            try:
                if os.name == "nt":
                    self.p.kill()
                else:
                    os.killpg(self.p.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.p.wait(5)

    def __enter__(self): return self
    def __exit__(self, *a): self.close()


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(-(-q * len(xs) // 1)) - 1))] if xs else float("nan")


async def run_closed(srv, policy, n, conc=8):
    """n logical requests with `conc` workers (used for single-fault matrix)."""
    lim = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    async with httpx.AsyncClient(base_url=srv.url, limits=lim) as c:
        q = asyncio.Queue()
        for _ in range(n): q.put_nowait(1)
        res = []

        async def w():
            while not q.empty():
                q.get_nowait(); res.append(await policy.call(c))
        await asyncio.gather(*[w() for _ in range(conc)])
    return res


async def run_open(srv, policy, rate, duration, seed, *, strict_window=False, record_completion=False, config=None):
    """Poisson arrivals at `rate`/s for `duration` s, each an independent logical request.
    Returns [(arrival_time, Outcome)] for requests that finished before duration+drain."""
    rng = random.Random(seed)
    out, tasks = [], []
    async with httpx.AsyncClient(base_url=srv.url, limits=OPEN_LOAD_LIMITS) as c:
        t0 = time.monotonic()
        if config is not None:
            response = await c.post("/_config", json=config, timeout=5)
            response.raise_for_status()
            server_start = response.json()["monotonic_start"]
            if not t0 <= server_start <= time.monotonic():
                raise RuntimeError("Recovery requires a server on the same monotonic clock.")
            t0 = server_start
        t = 0.0

        async def one(ta):
            o = await policy.call(c)
            out.append((ta, o, time.monotonic() - t0) if record_completion else (ta, o))
        while t < duration:
            t += rng.expovariate(rate)
            if strict_window and t >= duration:
                break
            await asyncio.sleep(max(0, t0 + t - time.monotonic()))
            tasks.append(asyncio.create_task(one(t)))
        await asyncio.gather(*tasks)
    return out
