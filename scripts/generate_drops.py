"""Write synthetic vendor drops into a landing folder, including one file with defects
and one that breaks the contract. No real vendor data."""
from __future__ import annotations

import csv
import json
import random
import sys
from datetime import date, timedelta
from pathlib import Path


def pos_rows(rnd, day: date, n: int):
    for _ in range(n):
        yield {"Store": f"S{rnd.randint(1, 40):03d}", "ItemSku": f"SKU-{rnd.randint(1000, 1200)}",
               "sale_date": day.isoformat(), "Qty": rnd.randint(1, 30),
               "SalesAmt": round(rnd.uniform(2, 400), 2), "channel": rnd.choice(["store", "online", "pickup"]),
               "promo_id": rnd.choice(["", "", "P100", "P101"])}


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main(landing: str = "data/landing", today: date | None = None, seed: int = 3) -> list[Path]:
    rnd = random.Random(seed)
    today = today or date.today()
    root = Path(landing)
    root.mkdir(parents=True, exist_ok=True)
    day = today - timedelta(days=1)

    clean = list({(r["Store"], r["ItemSku"], r["channel"]): r for r in pos_rows(rnd, day, 300)}.values())
    write_csv(root / f"pos_sales_{day:%Y%m%d}_a.csv", clean)

    messy = list({(r["Store"], r["ItemSku"], r["channel"]): r for r in pos_rows(rnd, day, 200)}.values())
    messy[0]["Qty"] = "two"          # bad int
    messy[1]["SalesAmt"] = "-5"      # below min
    messy[2]["channel"] = "kiosk"    # not allowed
    messy.append(dict(messy[3]))     # duplicate key
    write_csv(root / f"pos_sales_{day:%Y%m%d}_b.csv", messy)

    broken = [{k: v for k, v in r.items() if k != "SalesAmt"} for r in pos_rows(rnd, day, 20)]
    write_csv(root / f"pos_sales_{day:%Y%m%d}_c.csv", broken)  # missing required column

    promos = [{"promoId": f"P{100 + i}", "name": f"Promo {i}", "startDate": day.isoformat(),
               "endDate": (day + timedelta(days=14)).isoformat(), "discountPct": rnd.choice([10, 15, 20]),
               "type": rnd.choice(["bogo", "percent_off", "bundle"])} for i in range(12)]
    (root / f"promotions_{day:%Y%m%d}.json").write_text(json.dumps({"data": promos}, indent=2))
    return sorted(root.iterdir())


if __name__ == "__main__":
    for p in main(*sys.argv[1:2]):
        print(p)
