from datetime import date

import pandas as pd
import pytest

from ingest.contracts import POS_SALES, PROMOTIONS
from ingest.readers import read_file, snake
from ingest.validate import FileRejected, validate
from generate_drops import main as generate

TODAY = date(2026, 3, 15)


@pytest.fixture
def landing(tmp_path):
    generate(str(tmp_path), today=TODAY)
    return tmp_path


def test_snake_case_and_aliases(landing):
    assert snake("ItemSku") == "item_sku" and snake("Sales Amt") == "sales_amt"
    df = read_file(next(landing.glob("pos_sales_*_a.csv")), POS_SALES)
    assert {"store_id", "sku", "units", "net_sales", "_file_hash"} <= set(df.columns)


def test_clean_file_passes(landing):
    r = validate(read_file(next(landing.glob("pos_sales_*_a.csv")), POS_SALES), POS_SALES, TODAY)
    assert r.stats["quarantined"] == 0 and r.stats["valid"] > 100
    assert r.valid["units"].dtype == "Int64"


def test_row_defects_are_quarantined_with_reasons(landing):
    r = validate(read_file(next(landing.glob("pos_sales_*_b.csv")), POS_SALES), POS_SALES, TODAY)
    reasons = ";".join(r.quarantine["dq_reasons"])
    for expected in ["units:bad_int", "net_sales:below_min", "channel:not_allowed", "duplicate_key"]:
        assert expected in reasons
    assert r.stats["quarantined"] == 4


def test_missing_column_rejects_file(landing):
    with pytest.raises(FileRejected, match="missing required columns"):
        validate(read_file(next(landing.glob("pos_sales_*_c.csv")), POS_SALES), POS_SALES, TODAY)


def test_reject_ratio_gate():
    df = pd.DataFrame({"store_id": ["S1", "S2"], "sku": ["a", "b"], "sale_date": ["2026-03-14"] * 2,
                       "units": ["x", "1"], "net_sales": ["1", "1"], "channel": ["store", "store"]})
    with pytest.raises(FileRejected, match="reject ratio"):
        validate(df, POS_SALES, TODAY)


def test_stale_file_rejected(landing):
    df = read_file(next(landing.glob("pos_sales_*_a.csv")), POS_SALES)
    with pytest.raises(FileRejected, match="stale"):
        validate(df, POS_SALES, date(2026, 12, 31))


def test_nested_json_feed(landing):
    r = validate(read_file(next(landing.glob("promotions_*.json")), PROMOTIONS), PROMOTIONS, TODAY)
    assert r.stats == {"rows": 12, "valid": 12, "quarantined": 0, "reject_ratio": 0.0}
