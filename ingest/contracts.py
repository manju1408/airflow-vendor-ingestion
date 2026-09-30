"""Data contracts for each vendor feed."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str  # string | int | float | date | timestamp
    required: bool = True
    allowed: tuple | None = None
    min_value: float | None = None


@dataclass(frozen=True)
class Contract:
    feed: str
    pattern: str
    columns: tuple[Column, ...]
    key: tuple[str, ...]
    target_table: str
    max_reject_ratio: float = 0.05
    freshness_column: str | None = None
    max_age_days: int | None = None
    aliases: dict = field(default_factory=dict)

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]


POS_SALES = Contract(
    feed="pos_sales",
    pattern="pos_sales_*.csv",
    columns=(
        Column("store_id", "string"),
        Column("sku", "string"),
        Column("sale_date", "date"),
        Column("units", "int", min_value=0),
        Column("net_sales", "float", min_value=0),
        Column("channel", "string", allowed=("store", "online", "pickup")),
        Column("promo_id", "string", required=False),
    ),
    key=("store_id", "sku", "sale_date", "channel"),
    target_table="raw_pos_sales",
    freshness_column="sale_date",
    max_age_days=45,
    aliases={"store": "store_id", "item_sku": "sku", "qty": "units", "sales_amt": "net_sales"},
)

PROMOTIONS = Contract(
    feed="promotions",
    pattern="promotions_*.json",
    columns=(
        Column("promo_id", "string"),
        Column("name", "string"),
        Column("start_date", "date"),
        Column("end_date", "date"),
        Column("discount_pct", "float", min_value=0),
        Column("type", "string", allowed=("bogo", "percent_off", "bundle")),
    ),
    key=("promo_id",),
    target_table="raw_promotions",
)

CONTRACTS = {c.feed: c for c in (POS_SALES, PROMOTIONS)}
