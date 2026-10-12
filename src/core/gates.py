"""Declarative quality gates. orchestration.py builds Dagster assets and checks from these.

Kept free of Dagster/dbt imports so tests can load it without a dbt manifest.
`path_key` indexes into core.config.get_s3_paths().
"""

from dataclasses import dataclass

EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"

# GX passes column expectations on an empty table, so every suite needs a row-count floor.
MAX_ROWS = 2_000_000


def non_empty(max_rows: int = MAX_ROWS) -> dict:
    return {
        "expectation": "ExpectTableRowCountToBeBetween",
        "kwargs": {"min_value": 1, "max_value": max_rows},
    }


def not_null(column: str) -> dict:
    return {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": column}}


def matches(column: str, regex: str = EMAIL_REGEX) -> dict:
    return {
        "expectation": "ExpectColumnValuesToMatchRegex",
        "kwargs": {"column": column, "regex": regex},
    }


def between(column: str, **bounds) -> dict:
    return {"expectation": "ExpectColumnValuesToBeBetween", "kwargs": {"column": column, **bounds}}


@dataclass(frozen=True)
class SourceGate:
    """Pre-flight gate on a landing file. `name` must match the dbt source name."""

    name: str
    path_key: str
    data_format: str
    expectations: list[dict]


@dataclass(frozen=True)
class TargetGate:
    """Post-write @asset_check on a dbt model's output. `name` must match the model name."""

    name: str
    path_key: str
    expectations: list[dict]
    blocking: bool = True
    data_format: str = "delta"
    additional_deps: tuple[str, ...] = ()
    check_name: str | None = None

    @property
    def dagster_check_name(self) -> str:
        return self.check_name or f"check_{self.name}_target"


# Suites must cover the columns that utils/generate_corrupt_data.py damages.
SOURCE_GATES = [
    SourceGate(
        name="sales_landing",
        path_key="sales_landing",
        data_format="csv",
        expectations=[non_empty(), not_null("id"), not_null("customer_id")],
    ),
    SourceGate(
        name="customers_landing",
        path_key="customers_landing",
        data_format="json",
        expectations=[non_empty(), not_null("id"), matches("email")],
    ),
]

TARGET_GATES = [
    TargetGate(
        name="stg_sales_bronze",
        path_key="sales_bronze",
        expectations=[
            non_empty(),
            not_null("id"),
            not_null("product"),
            between("price", min_value=0),
        ],
        additional_deps=("inject_corrupt_bronze",),
    ),
    TargetGate(
        name="stg_customers_bronze",
        path_key="customers_bronze",
        expectations=[non_empty(), not_null("customer_id"), matches("email")],
        additional_deps=("inject_corrupt_bronze",),
    ),
    TargetGate(
        name="sales_silver",
        path_key="sales_silver",
        expectations=[non_empty(), between("price", min_value=0, max_value=10000)],
    ),
    TargetGate(
        name="customers_silver",
        path_key="customers_silver",
        expectations=[non_empty(), not_null("email")],
    ),
    TargetGate(
        name="sales_summary",
        path_key="sales_summary",
        expectations=[
            non_empty(),
            between("avg_ticket", min_value=0, max_value=100000),
            not_null("total_revenue"),
        ],
        blocking=False,
        check_name="check_sales_summary",
    ),
]
