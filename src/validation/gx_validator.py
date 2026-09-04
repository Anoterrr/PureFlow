"""Generic validation engine using Great Expectations (GX)."""

import contextlib
import os
from typing import Any

import great_expectations as gx
from great_expectations.core.validation_definition import ValidationDefinition

from core.connection import ConnectionFactory
from core.logger import logger
from core.quality import get_gx_context, get_or_create_suite


def validate_data(
    path: str,
    expectations: list[dict[str, Any]],
    data_format: str = "parquet",
    suite_name: str = "dynamic_suite",
    context: Any | None = None,
) -> tuple[bool, str, str | None]:
    """
    Generic execution engine for validation.
    Returns (success: bool, report_url: str, error_message: Optional[str]).
    """
    factory = ConnectionFactory()

    if context is None:
        context = get_gx_context()

    logger.info("🛡️ [Validator] Validating data at %s...", path)

    # Reads via DuckDB (fast S3/Parquet/Delta access) then hands off to GX's
    # Pandas engine, which is the stable one.
    import duckdb

    success = True
    error_msg = None

    web_report_url = "http://localhost:8082/index.html"
    base_docs_path = os.path.abspath("gx/uncommitted/data_docs/local_site")

    try:
        fmt = data_format.lower()
        if fmt == "delta":
            read_func = "delta_scan"
        else:
            read_func = (
                "read_csv_auto"
                if fmt == "csv"
                else ("read_parquet" if fmt == "parquet" else "read_json_auto")
            )

        with duckdb.connect() as conn:
            factory.setup_s3_auth(conn)
            logger.info("⚡ [Validator] Fetching data via DuckDB...")
            # read_func comes from the fixed whitelist above, not external input
            df = conn.execute(f"SELECT * FROM {read_func}(?)", [path]).df()  # nosec B608

        if df.empty:
            logger.warning("⚠️ [Validator] Data is empty at %s", path)

        # Unique datasource name per run to avoid persistence conflicts in the context
        import uuid

        run_id = str(uuid.uuid4())[:8]
        datasource_name = f"ds_{suite_name}_{run_id}"

        datasource = context.data_sources.add_pandas(name=datasource_name)

        suite = get_or_create_suite(context, suite_name)

        # Wipe and rebuild so only the expectations passed in for this call apply
        for expectation in list(suite.expectations):
            suite.delete_expectation(expectation)

        for check in expectations:
            exp_name = check.get("expectation")
            kwargs = check.get("kwargs", {})

            try:
                exp_class = getattr(gx.expectations, exp_name)
                suite.add_expectation(exp_class(**kwargs))
            except (AttributeError, TypeError) as e:
                logger.warning("⚠️ [Validator] Could not add expectation %s: %s", exp_name, str(e))

        asset_name = f"data_{suite_name}"
        asset = datasource.add_dataframe_asset(name=asset_name)
        batch_def = asset.add_batch_definition_whole_dataframe(f"batch_{suite_name}")
        batch_parameters = {"dataframe": df}

        val_name = f"val_{suite_name}_{run_id}"
        try:
            with contextlib.suppress(Exception):
                context.validation_definitions.delete(val_name)

            val_def = context.validation_definitions.add(
                ValidationDefinition(name=val_name, data=batch_def, suite=suite)
            )
        except Exception as e:
            raise RuntimeError(f"Failed to manage GX ValidationDefinition: {str(e)}") from e

        logger.info("⚡ [Validator] Running GX validation execution (Pandas Engine)...")
        result = val_def.run(batch_parameters=batch_parameters)
        success = result.success
        if not success:
            logger.error("❌ [Validator] Validation FAILED.")
            error_msg = "Data quality validation failed (check GX report for details)."

        context.build_data_docs()

    except Exception as e:
        logger.error("❌ [Validator] Validation technical failure: %s", str(e))
        success = False
        error_msg = f"Technical validation failure: {str(e)}"
    finally:
        # Keep the context lean between calls
        with contextlib.suppress(Exception):
            context.validation_definitions.delete(val_name)
            context.data_sources.delete(datasource_name)

    report_path = os.path.join(base_docs_path, "validations", suite_name)
    latest_report_file = os.path.join(base_docs_path, "index.html")
    try:
        if os.path.exists(report_path):
            html_files = []
            for root, _, files in os.walk(report_path):
                for file in files:
                    if file.endswith(".html"):
                        html_files.append(os.path.join(root, file))

            if html_files:
                latest_report_file = max(html_files, key=os.path.getmtime)
    except Exception as e:
        logger.warning("Could not find latest report: %s", str(e))

    if os.path.exists(latest_report_file):
        relative_path = os.path.relpath(latest_report_file, base_docs_path)
        web_report_url = f"http://localhost:8082/{relative_path}"

    return success, web_report_url, error_msg
