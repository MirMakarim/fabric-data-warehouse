"""
Generate a fictional procurement dataset for the Fabric data warehouse project.

All companies, materials and numbers are invented. The data contains a few
deliberate quality issues (duplicates, padded strings, invalid quantities,
mixed currencies) so the silver layer has real cleansing work to do.

Usage:
    python scripts/generate_sample_data.py            # writes to data/raw/
    python scripts/generate_sample_data.py --lines 50000
"""
import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)
OUT = Path(__file__).resolve().parents[1] / "data" / "raw"

PREFIX = ["Nord", "Alpen", "Rhein", "Baltic", "Delta", "Vector", "Orion", "Kestrel", "Atlas", "Summit",
          "Lumen", "Polar", "Granite", "Helix", "Meridian", "Apex", "Cobalt", "Falcon", "Nova", "Pioneer"]
CORE = ["tech", "parts", "metal", "plast", "tronic", "pack", "logistik", "werk", "systems", "chem"]
SUFFIX = ["GmbH", "AG", "s.r.o.", "Sp. z o.o.", "S.A.", "Ltd", "KG", "B.V."]
COUNTRIES = ["DE", "DE", "DE", "PL", "CZ", "FR", "IT", "NL", "CN", "MX", "US", "IN"]
CATEGORIES = {
    "Electronics": ["Sensor module", "LED driver", "Microcontroller", "Wiring harness", "PCB assembly"],
    "Raw materials": ["Aluminium sheet", "Steel coil", "Copper wire", "Polycarbonate granulate", "Glass lens blank"],
    "Plastics": ["Housing cover", "Clip set", "Gasket", "Lens frame", "Connector shell"],
    "Packaging": ["Carton box", "Foam insert", "Pallet", "ESD bag", "Label roll"],
    "Logistics": ["Road freight", "Sea freight", "Express parcel", "Warehousing fee", "Customs handling"],
    "Services": ["Tool maintenance", "Calibration", "IT support", "Cleaning service", "Training"],
}
PLANTS = [
    ("P100", "Plant Lippstadt", "DE", "EMEA"), ("P110", "Plant Hamm", "DE", "EMEA"),
    ("P200", "Plant Brno", "CZ", "EMEA"), ("P210", "Plant Craiova", "RO", "EMEA"),
    ("P300", "Plant Querétaro", "MX", "Americas"), ("P310", "Plant Detroit", "US", "Americas"),
    ("P400", "Plant Shanghai", "CN", "Asia Pacific"), ("P410", "Plant Pune", "IN", "Asia Pacific"),
]


def suppliers(n=120):
    names = set()
    while len(names) < n:
        names.add(f"{RNG.choice(PREFIX)}{RNG.choice(CORE)} {RNG.choice(SUFFIX)}")
    cats = list(CATEGORIES)
    df = pd.DataFrame({
        "supplier_id": [f"S{1000 + i}" for i in range(n)],
        "supplier_name": sorted(names),
        "country": RNG.choice(COUNTRIES, n),
        "category": RNG.choice(cats, n),
        "payment_terms_days": RNG.choice([30, 45, 60, 90], n, p=[0.35, 0.25, 0.3, 0.1]),
        "is_single_source": RNG.random(n) < 0.25,
    })
    # quality issue: padded names
    pad = RNG.choice(n, 8, replace=False)
    df.loc[pad, "supplier_name"] = "  " + df.loc[pad, "supplier_name"] + " "
    return df


def materials():
    rows, i = [], 0
    for cat, items in CATEGORIES.items():
        for item in items:
            for variant in ("A", "B", "C"):
                rows.append((f"M{20000 + i}", f"{item} {variant}", cat,
                             "EA" if cat not in ("Raw materials",) else "KG"))
                i += 1
    return pd.DataFrame(rows, columns=["material_id", "material_name", "category", "unit"])


def purchase_orders(sup, mat, lines):
    start = date(2024, 1, 1)
    days = (date(2025, 12, 31) - start).days
    sup_by_cat = {c: g["supplier_id"].tolist() for c, g in sup.groupby("category")}
    mat_s = mat.sample(lines, replace=True, random_state=7).reset_index(drop=True)
    po_dates = [start + timedelta(days=int(d)) for d in RNG.integers(0, days, lines)]
    base_price = {m: round(float(RNG.uniform(0.5, 120)), 2) for m in mat["material_id"]}
    df = pd.DataFrame({
        "po_number": [f"45{RNG.integers(10_000_000, 99_999_999)}" for _ in range(lines)],
        "po_line": RNG.choice([10, 20, 30], lines),
        "po_date": [d.isoformat() for d in po_dates],
        "supplier_id": [RNG.choice(sup_by_cat.get(c, sup["supplier_id"].tolist())) for c in mat_s["category"]],
        "material_id": mat_s["material_id"],
        "plant_id": RNG.choice([p[0] for p in PLANTS], lines),
        "quantity": RNG.integers(1, 500, lines),
        "unit_price": [round(base_price[m] * float(RNG.uniform(0.9, 1.1)), 2) for m in mat_s["material_id"]],
        "currency": RNG.choice(["EUR", "EUR", "EUR", "USD", "CNY"], lines),
        "requested_delivery_date": [(d + timedelta(days=int(RNG.integers(7, 60)))).isoformat() for d in po_dates],
    })
    df = df.drop_duplicates(["po_number", "po_line"]).reset_index(drop=True)
    # quality issues: invalid quantities and exact duplicate rows
    bad = RNG.choice(len(df), 25, replace=False)
    df.loc[bad, "quantity"] = RNG.choice([0, -5, -1], 25)
    df = pd.concat([df, df.sample(60, random_state=3)], ignore_index=True)
    return df


def goods_receipts(po):
    po = po.drop_duplicates(["po_number", "po_line"])
    po = po[po["quantity"] > 0].sample(frac=0.93, random_state=11)
    req = pd.to_datetime(po["requested_delivery_date"])
    delay = RNG.choice([-5, -2, 0, 1, 3, 7, 14], len(po), p=[0.10, 0.15, 0.60, 0.05, 0.04, 0.03, 0.03])
    return pd.DataFrame({
        "po_number": po["po_number"].values,
        "po_line": po["po_line"].values,
        "receipt_date": (req + pd.to_timedelta(delay, unit="D")).dt.date.astype(str).values,
        "received_qty": po["quantity"].values,
    })


def invoices(po, gr):
    keys = gr[["po_number", "po_line"]].merge(po.drop_duplicates(["po_number", "po_line"]), on=["po_number", "po_line"])
    keys = keys.merge(gr[["po_number", "po_line", "receipt_date"]], on=["po_number", "po_line"])
    inv_date = pd.to_datetime(keys["receipt_date"]) + pd.to_timedelta(RNG.integers(0, 15, len(keys)), unit="D")
    amount = (keys["quantity"] * keys["unit_price"] * RNG.choice([1, 1, 1, 1, 1.02, 0.98], len(keys))).round(2)
    return pd.DataFrame({
        "invoice_id": [f"INV-{i:07d}" for i in range(1, len(keys) + 1)],
        "po_number": keys["po_number"], "po_line": keys["po_line"],
        "invoice_date": inv_date.dt.date.astype(str), "invoice_amount": amount, "currency": keys["currency"],
    })


def fx_rates():
    months = pd.date_range("2024-01-01", "2026-06-01", freq="MS")
    usd = 1.08 + np.cumsum(RNG.normal(0, 0.008, len(months)))
    cny = 7.80 + np.cumsum(RNG.normal(0, 0.04, len(months)))
    rows = [("EUR", m.date().isoformat(), 1.0) for m in months]
    rows += [("USD", m.date().isoformat(), round(r, 4)) for m, r in zip(months, usd)]
    rows += [("CNY", m.date().isoformat(), round(r, 4)) for m, r in zip(months, cny)]
    return pd.DataFrame(rows, columns=["currency", "month_start", "units_per_eur"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, default=15000)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    sup, mat = suppliers(), materials()
    po = purchase_orders(sup, mat, args.lines)
    gr = goods_receipts(po)
    inv = invoices(po, gr)
    plants = pd.DataFrame(PLANTS, columns=["plant_id", "plant_name", "country", "region"])
    for name, df in [("suppliers", sup), ("materials", mat), ("plants", plants), ("purchase_orders", po),
                     ("goods_receipts", gr), ("invoices", inv), ("fx_rates", fx_rates())]:
        df.to_csv(OUT / f"{name}.csv", index=False)
        print(f"{name:16s} {len(df):>7,} rows")


if __name__ == "__main__":
    main()
