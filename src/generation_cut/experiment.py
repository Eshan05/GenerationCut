import argparse, asyncio, json, time
from pathlib import Path

from .harness import ServerProc, run_closed, run_open, pct
from .policies import standard, Policy

RATES_30 = ["http429", "http500", "hang", "truncate", "garbage"]
BASELINE_REPORT = Path(__file__).resolve().parents[2] / "docs" / "experiment.json"
DEFAULT_OUT = "results/raw/baseline-reproduction.json"


def matrix(args):
    rows = []
    for fault in RATES_30:
        for pol in standard(1):
            with ServerProc() as srv:      # fresh process per run: no leftover handlers or backlog from the previous run
                srv.config(rates={fault: 0.3}, seed=7, hang_s=3.0, retry_after_s=0.05)   # hang 3 s > 1 s client timeout
                res = asyncio.run(run_closed(srv, pol, args.n))
                lat = [o.latency for o in res]
                rows.append(dict(fault=fault, policy=pol.name, success=sum(o.ok for o in res) / len(res),
                                 attempts=sum(o.attempts for o in res) / len(res), p50=pct(lat, .5), p99=pct(lat, .99)))
                print(rows[-1], flush=True)
    return rows


def storm(args):
    """Open-loop load below capacity; a short total outage (500s, still costing capacity); watch recovery."""
    out = []
    for seed in range(args.seeds):
        for pol in standard(seed):
            with ServerProc() as srv:
                srv.config(rates={}, seed=seed, service_s=0.04, slots=1, outage_start=args.outage[0], outage_end=args.outage[1])
                res = asyncio.run(run_open(srv, pol, args.rate, args.duration, seed))
                st = srv.stats()
                sec = lambda ta: int(ta)
                buckets = {}
                for ta, o in res: buckets.setdefault(sec(ta), []).append(o.ok)
                frac = [(sum(buckets[s]) / len(buckets[s]) if buckets.get(s) else 0.0) for s in range(int(args.duration))]
                rec = None   # first second after outage start+... where success >= 0.9 for 3 consecutive seconds
                for s in range(int(args.outage[1]), int(args.duration) - 2):
                    if all(f >= 0.9 for f in frac[s:s + 3]): rec = s - args.outage[1]; break
                out.append(dict(policy=pol.name, seed=seed, success_frac_per_s=frac, recovery_s=rec,
                                overall_success=sum(o.ok for _, o in res) / len(res),
                                server_received=st["received"], logical=len(res),
                                amplification=st["received"] / len(res), peak_arrivals_per_s=max(st["arrivals_per_s"].values())))
                print({k: v for k, v in out[-1].items() if k != "success_frac_per_s"}, flush=True)
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--rate", type=float, default=16)
    ap.add_argument("--duration", type=float, default=40)
    ap.add_argument("--outage", type=float, nargs=2, default=[5, 9])
    ap.add_argument("--out", default=DEFAULT_OUT, help=f"Generated report path (default: {DEFAULT_OUT}).")
    ap.add_argument("--skip-matrix", action="store_true")
    a = ap.parse_args(argv)
    if a.n < 1 or a.seeds < 1:
        ap.error("--n and --seeds must be positive integers.")
    if not (0 < a.rate < float("inf") and 0 < a.duration < float("inf")):
        ap.error("--rate and --duration must be finite and positive.")
    start, end = a.outage
    if not 0 <= start < end <= a.duration:
        ap.error("--outage must be an increasing window within --duration.")
    out = Path(a.out)
    if out.resolve() == BASELINE_REPORT.resolve():
        ap.error("The saved baseline is read-only. Choose a different --out path.")
    out.parent.mkdir(parents=True, exist_ok=True)
    r = {"config": vars(a)}
    if not a.skip_matrix: r["matrix"] = matrix(a)
    r["storm"] = storm(a)
    temp = out.with_name(f".{out.name}.tmp")
    temp.write_text(json.dumps(r, indent=1) + "\n", encoding="utf-8")
    temp.replace(out)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
