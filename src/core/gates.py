"""Declarative spec for every quality gate in the pipeline.

Pure data, with no Dagster or dbt imports. Two reasons:

1. orchestration.py builds the assets and asset_checks from this table instead
   of repeating a near-identical 15-line block per gate.
2. tests can import it directly. Importing orchestration.py is not an option:
   @dbt_assets reads dbt/target/manifest.json at import time and that path is
   gitignored, so the import fails anywhere dbt has not run, CI included.

`path_key` indexes into core.config.get_s3_paths(), which is the single source
of truth for S3 layout. The gates used to hard-code the same URIs a second
time, so a layout change had to be made in two places or the gate would
end up validating a path nothing writes to.
"""

from dataclasses import dataclass

EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"

# Upper bound only guards against a runaway generator; the value that matters
# is min_value=1. GX evaluates value-based expectations vacuously on an empty
# table (0 rows passes ExpectColumnValuesToBeBetween), so without a row-count
# floor, a run that produces nothing at all clears every gate.
MAX_ROWS = 2_000_000


def non_empty(max_rows: int = MAX_ROWS) -> dict:
    """Table must have at least one row."""
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
    """Pre-flight gate: a Dagster asset validating a raw landing file.

    `name` doubles as the Dagster asset name and the dbt source name, which is
    what makes the gate a real upstream dependency of the Bronze model rather
    than a check running alongside it.
    """

    name: str
    path_key: str
    data_format: str
    expectations: list[dict]


@dataclass(frozen=True)
class TargetGate:
    """Post-write gate: an @asset_check on a dbt model's materialized output."""

    name: str
    path_key: str
    expectations: list[dict]
    blocking: bool = True
    data_format: str = "delta"
    # Assets that must run before this check when a job selects both. Used by
    # quality_test_bronze_job so the corruptor overwrites Bronze before the
    # gate reads it, instead of racing it.
    additional_deps: tuple[str, ...] = ()
    check_name: str | None = None

    @property
    def dagster_check_name(self) -> str:
        return self.check_name or f"check_{self.name}_target"


# Every suite must cover the columns the corruptors in utils/generate_corrupt_data.py
# actually damage. When they did not, the quality-test jobs reported success on data
# it had corrupted on purpose.
SOURCE_GATES = [
    SourceGate(
        name="sales_landing",
        path_key="sales_landing",
        data_format="csv",
        # `id` is what generate_dirty_data and corrupt_landing_zone null out.
        # Checking only customer_id (always 1 in the corrupt file) let bad
        # input reach Bronze, so this gate never actually fired.
        expectations=[non_empty(), not_null("id"), not_null("customer_id")],
    ),
    SourceGate(
        name="customers_landing",
        path_key="customers_landing",
        data_format="json",
        # corrupt_landing_zone writes 'invalid-email-N'; without the regex the
        # landing gate passed and only the Bronze check caught it, too late.
        expectations=[non_empty(), not_null("id"), matches("email")],
    ),
]

TARGET_GATES = [
    TargetGate(
        name="stg_sales_bronze",
        path_key="sales_bronze",
        # corrupt_bronze_layer injects NULL product and price -10.5 while
        # keeping `id` populated, so an id-only suite passed on corrupt data.
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
        # corrupt_bronze_layer nulls customer_id but writes a *valid* e-mail,
        # so the regex-only suite passed.
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
        # Non-blocking: nothing in the DAG consumes Gold downstream.
        blocking=False,
        check_name="check_sales_summary",
    ),
]
