from datetime import date

from ingest.contracts import POS_SALES
from ingest.load import SQLiteLoader, merge_sql
from ingest.manifest import Manifest
from ingest.process import process_file
from generate_drops import main as generate

TODAY = date(2026, 3, 15)


def run_all(tmp_path, loader, manifest):
    from ingest.contracts import CONTRACTS
    import fnmatch

    out = []
    for p in sorted((tmp_path / "landing").glob("*")):
        for c in CONTRACTS.values():
            if fnmatch.fnmatch(p.name, c.pattern):
                out.append(process_file(p, c, loader, manifest, tmp_path / "archive",
                                        tmp_path / "rejected", today=TODAY))
    return out


def test_end_to_end_and_idempotent(tmp_path):
    generate(str(tmp_path / "landing"), today=TODAY)
    loader, manifest = SQLiteLoader(str(tmp_path / "wh.db")), Manifest(str(tmp_path / "m.db"))
    results = run_all(tmp_path, loader, manifest)
    status = sorted(r["status"] for r in results)
    assert status == ["loaded", "loaded", "loaded", "rejected"]
    assert len(list((tmp_path / "rejected").iterdir())) == 1
    n = loader.count("raw_pos_sales")
    assert loader.count("raw_pos_sales_quarantine") == 4

    # the vendor re-delivers the same files: nothing is loaded twice
    generate(str(tmp_path / "landing"), today=TODAY)
    again = run_all(tmp_path, loader, manifest)
    assert {r["status"] for r in again if r["file"].endswith(("_a.csv", "_b.csv"))} == {"skipped_duplicate"}
    assert loader.count("raw_pos_sales") == n


def test_merge_sql_uses_business_key():
    sql = merge_sql(POS_SALES, "raw_pos_sales_stg")
    assert sql.startswith("MERGE INTO raw_pos_sales t USING raw_pos_sales_stg s")
    assert "t.store_id = s.store_id AND t.sku = s.sku AND t.sale_date = s.sale_date" in sql
    assert "UPDATE SET store_id" not in sql
