import json
import sys
from pathlib import Path


def main(path):
    d = json.loads(Path(path).read_text(encoding="utf-8"))
    config = d["config"]
    print("config:", config)
    if "matrix" in d:
        pols = list(dict.fromkeys(r["policy"] for r in d["matrix"]))
        print(f"\nSINGLE FAULT ({config['n']} requests each): success / attempts per request / p99 s")
        print("%-10s" % "fault" + "".join("%-30s" % p for p in pols))
        for f in dict.fromkeys(r["fault"] for r in d["matrix"]):
            row = "%-10s" % f
            for p in pols:
                r = next(x for x in d["matrix"] if x["fault"] == f and x["policy"] == p)
                row += "%-30s" % ("%.3f / %.2f / %.2f" % (r["success"], r["attempts"], r["p99"]))
            print(row)
    start, end = config["outage"]
    print(f"\nRETRY STORM (offered {config['rate']:g} req/s for {config['duration']:g} s, "
          f"outage {start:g}-{end:g} s): per seed")
    pols = list(dict.fromkeys(r["policy"] for r in d["storm"]))
    for p in pols:
        rs = [r for r in d["storm"] if r["policy"] == p]
        print("%-20s success %s | amplification %s | peak arrivals/s %s | recovery_s %s" % (
            p, [round(r["overall_success"], 3) for r in rs], [round(r["amplification"], 2) for r in rs],
            [r["peak_arrivals_per_s"] for r in rs], [r["recovery_s"] for r in rs]))
    r = next((x for x in d["storm"] if x["policy"] == "immediate_x4" and x["seed"] == 0), None)
    if r is not None:
        print("\nsuccess fraction per arrival second, immediate_x4 seed 0:",
              [round(x, 2) for x in r["success_frac_per_s"]])

if __name__ == "__main__":
    main(sys.argv[1])
