"""Reproducible experiments behind the article's Results and Discussion section.

Run inside the dagster container, with the stack up:

    docker compose exec dagster python /app/scripts/run_experiments.py

Two scenarios over the same dbt models:
    A  baseline_no_gates_job   no quality gate at all (no pre-flight, no checks)
    B  pureflow_pipeline_job   every gate in src/core/gates.py

E1 (overhead): clean dataset, 1,000,000 sales and 100,000 customers. One warm-up
run per scenario, then REPS measured runs, interleaved A/B so any drift in the
machine affects both scenarios equally.

E2 (detection and propagation): four defective datasets. Each scenario runs on
its own partition date, so neither ever reads the other's output. Defects are
counted in every layer straight from the Delta tables, not taken from the logs.
D4 is the one that exercises a *post-write* gate inside the real pipeline: its
defects pass the pre-flight rules and only the Bronze check can see them, so it
shows whether a failing Bronze check actually keeps Silver and Gold from being
written.

Per-step durations are parsed from Dagster's STEP_SUCCESS events, so the
overhead can be split between the gates' own work and everything else.

Results go to docs/experiments/results.json. Every partition the experiments
wrote is deleted at the end: the dashboard shows the most recent Gold partition,
and the experiment dates are in the future on purpose, so leaving them behind
would put a defective test batch on screen.
"""

import json
import platform
import re
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, "/app/src")

import dagster  # noqa: E402
import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
from pyarrow import fs  # noqa: E402

from core.config import (  # noqa: E402
    S3_BUCKET_BRONZE,
    S3_BUCKET_GOLD,
    S3_BUCKET_LANDING,
    S3_BUCKET_SILVER,
    get_delta_storage_options,
    get_s3_paths,
)
from core.connection import ConnectionFactory  # noqa: E402
from core.gates import EMAIL_REGEX  # noqa: E402
from utils.generate_clean_data import generate_clean_big_data  # noqa: E402
from utils.generate_corrupt_data import corrupt_landing_zone  # noqa: E402
from utils.generate_dirty_data import generate_dirty_big_data  # noqa: E402
from utils.generators import generate_base_customers, generate_base_sales  # noqa: E402

BUCKETS = (S3_BUCKET_LANDING, S3_BUCKET_BRONZE, S3_BUCKET_SILVER, S3_BUCKET_GOLD)

REPS = 5
ORCHESTRATION = "/app/src/orchestration.py"
OUT = Path("/app/docs/experiments/results.json")

# One partition date per experiment, far from any real run, so nothing collides.
DATE_E1 = "2030-01-01"
DATES_E2 = {
    "D1": {"A": "2030-02-01", "B": "2030-02-02"},
    "D2": {"A": "2030-03-01", "B": "2030-03-02"},
    "D4": {"A": "2030-04-01", "B": "2030-04-02"},
}

# D4 sizes and defects. Kept apart from the pre-flight rules on purpose: every
# id and customer_id is present, so the landing gates pass and the defects only
# become visible to the Bronze post-write gate (product not null, price >= 0).
D4_SALES, D4_CUSTOMERS = 5000, 1000
D4_NULL_PRODUCT, D4_NEGATIVE_PRICE = 25, 40

STEP_DONE = re.compile(r'Finished execution of step "([^"]+)" in ([\d.]+)(ms|s)')


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def run_job(job: str, date: str) -> dict:
    """Runs a Dagster job pinned to `date` and returns wall time and outcome."""
    cfg = {"resources": {"execution_date_resource": {"config": {"date": date}}}}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(cfg, f)
        cfg_path = f.name
    cmd = ["dagster", "job", "execute", "-f", ORCHESTRATION, "-j", job, "-c", cfg_path]
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603
    elapsed = time.perf_counter() - start
    out = proc.stdout + proc.stderr
    # Log lines read "... - <job> - <run_id> - <step_key> - STEP_FAILURE - ...",
    # so the step is the field right before the event name.
    failed = set()
    for line in out.splitlines():
        fields = [f.strip() for f in line.split(" - ")]
        if "STEP_FAILURE" in fields and fields.index("STEP_FAILURE") > 0:
            failed.add(fields[fields.index("STEP_FAILURE") - 1])
    failed = sorted(failed)
    steps = {
        m.group(1): round(float(m.group(2)) / (1000 if m.group(3) == "ms" else 1), 3)
        for m in STEP_DONE.finditer(out)
    }
    return {
        "seconds": round(elapsed, 3),
        "success": "RUN_SUCCESS" in out,
        "failed_steps": failed,
        "step_seconds": steps,
    }


def generate_bronze_only_defects(execution_date: str) -> None:
    """D4: clean ids everywhere, defects only in columns the Bronze gate checks."""
    customers = pd.DataFrame(generate_base_customers(D4_CUSTOMERS))
    customers["created_at"] = execution_date
    sales = pd.DataFrame(
        generate_base_sales(D4_SALES, D4_CUSTOMERS, (10.0, 5000.0), execution_date)
    )
    sales["product"] = sales["product"].astype(object)
    sales.loc[: D4_NULL_PRODUCT - 1, "product"] = None
    sales.loc[D4_SALES - D4_NEGATIVE_PRICE :, "price"] *= -1

    p = get_s3_paths(execution_date)
    c = conn()
    try:
        c.register("customers", customers)
        c.register("sales", sales)
        c.execute(f"COPY customers TO '{p['customers_landing']}' (FORMAT 'JSON', ARRAY TRUE)")
        c.execute(f"COPY sales TO '{p['sales_landing']}' (FORMAT 'CSV', HEADER TRUE)")
    finally:
        c.close()


def conn():
    c = ConnectionFactory.get_duckdb_conn(":memory:")
    ConnectionFactory.setup_s3_auth(c)
    return c


def scalar(c, sql: str):
    return c.execute(sql).fetchone()[0]


def delta_exists(c, path: str) -> bool:
    try:
        scalar(c, f"SELECT count(*) FROM delta_scan('{path}')")
        return True
    except Exception:
        return False


def landing_defects(c, date: str) -> dict:
    p = get_s3_paths(date)
    s, cu = f"read_csv_auto('{p['sales_landing']}')", f"read_json_auto('{p['customers_landing']}')"
    return {
        "sales_rows": scalar(c, f"SELECT count(*) FROM {s}"),
        "sales_id_null": scalar(c, f"SELECT count(*) FROM {s} WHERE id IS NULL"),
        "sales_price_negative": scalar(c, f"SELECT count(*) FROM {s} WHERE price < 0"),
        "sales_product_null": scalar(c, f"SELECT count(*) FROM {s} WHERE product IS NULL"),
        "customers_rows": scalar(c, f"SELECT count(*) FROM {cu}"),
        "customers_id_null": scalar(c, f"SELECT count(*) FROM {cu} WHERE id IS NULL"),
        "customers_email_invalid": scalar(
            c, f"SELECT count(*) FROM {cu} WHERE NOT regexp_matches(email, '{EMAIL_REGEX}')"
        ),
    }


def layer_defects(c, date: str) -> dict:
    """Defective records that reached each curated layer. None = layer never written."""
    p = get_s3_paths(date)
    res = {}
    for layer, sp, cp in [
        ("bronze", p["sales_bronze"], p["customers_bronze"]),
        ("silver", p["sales_silver"], p["customers_silver"]),
    ]:
        if not delta_exists(c, sp):
            res[layer] = None
            continue
        s, cu = f"delta_scan('{sp}')", f"delta_scan('{cp}')"
        res[layer] = {
            "sales_rows": scalar(c, f"SELECT count(*) FROM {s}"),
            "sales_id_null": scalar(c, f"SELECT count(*) FROM {s} WHERE id IS NULL"),
            "sales_price_negative": scalar(c, f"SELECT count(*) FROM {s} WHERE price < 0"),
            "sales_product_null": scalar(c, f"SELECT count(*) FROM {s} WHERE product IS NULL"),
            "customers_rows": scalar(c, f"SELECT count(*) FROM {cu}"),
            "customers_id_null": scalar(c, f"SELECT count(*) FROM {cu} WHERE customer_id IS NULL"),
            "customers_email_invalid": scalar(
                c, f"SELECT count(*) FROM {cu} WHERE NOT regexp_matches(email, '{EMAIL_REGEX}')"
            ),
        }
    if delta_exists(c, p["sales_summary"]):
        g = f"delta_scan('{p['sales_summary']}')"
        res["gold"] = {
            "rows": scalar(c, f"SELECT count(*) FROM {g}"),
            "orders": scalar(c, f"SELECT coalesce(sum(total_orders), 0) FROM {g}"),
            "orders_without_region": scalar(
                c, f"SELECT coalesce(sum(total_orders), 0) FROM {g} WHERE region IS NULL"
            ),
        }
    else:
        res["gold"] = None
    return res


def quarantined_rows(c, date: str) -> dict:
    """Rows copied to quarantine for `date`, per gate. Quarantine files are Parquet."""
    out = {}
    for bucket in BUCKETS:
        try:
            files = c.execute(
                f"SELECT file FROM glob('s3://{bucket}/quarantine/dt={date}/*/*')"
            ).fetchall()
        except Exception:
            files = []
        for (f,) in files:
            reason = f.split("reason=")[1].split("/")[0]
            out[reason] = scalar(c, f"SELECT count(*) FROM read_parquet('{f}')")
    return out


def e1_overhead(c) -> dict:
    log(f"E1: gerando dataset limpo em dt={DATE_E1}")
    t0 = time.perf_counter()
    generate_clean_big_data(execution_date=DATE_E1)
    gen_seconds = round(time.perf_counter() - t0, 3)
    log("E1: aquecimento (nao contabilizado)")
    run_job("baseline_no_gates_job", DATE_E1)
    run_job("pureflow_pipeline_job", DATE_E1)
    a, b = [], []
    for i in range(REPS):
        ra = run_job("baseline_no_gates_job", DATE_E1)
        rb = run_job("pureflow_pipeline_job", DATE_E1)
        a.append(ra)
        b.append(rb)
        log(f"E1 rodada {i + 1}/{REPS}: A={ra['seconds']}s B={rb['seconds']}s ok={rb['success']}")
    sa, sb = [r["seconds"] for r in a], [r["seconds"] for r in b]

    def mean_steps(runs):
        keys = sorted({k for r in runs for k in r["step_seconds"]})
        return {
            k: round(statistics.mean(r["step_seconds"].get(k, 0.0) for r in runs), 2) for k in keys
        }

    return {
        "generation_seconds": gen_seconds,
        "a_seconds": sa,
        "b_seconds": sb,
        "a_mean": round(statistics.mean(sa), 2),
        "a_stdev": round(statistics.stdev(sa), 2),
        "b_mean": round(statistics.mean(sb), 2),
        "b_stdev": round(statistics.stdev(sb), 2),
        "overhead_seconds": round(statistics.mean(sb) - statistics.mean(sa), 2),
        "overhead_pct": round((statistics.mean(sb) / statistics.mean(sa) - 1) * 100, 1),
        "b_runs_all_gates_passed": all(r["success"] for r in b),
        "a_step_mean_seconds": mean_steps(a),
        "b_step_mean_seconds": mean_steps(b),
        "layers": layer_defects(c, DATE_E1),
    }


def e2_detection(c) -> list:
    datasets = {
        "D1": ("Landing suja (gerador de dados sujos)", generate_dirty_big_data),
        "D2": ("Landing corrompida (corruptor)", corrupt_landing_zone),
        "D4": ("Defeitos visiveis so na Bronze", generate_bronze_only_defects),
    }
    results = []
    for key, (label, make) in datasets.items():
        entry = {"dataset": key, "label": label}
        for scenario, job in (("A", "baseline_no_gates_job"), ("B", "pureflow_pipeline_job")):
            d = DATES_E2[key][scenario]
            log(f"E2 {key} cenario {scenario}: dt={d}")
            make(execution_date=d)
            entry.setdefault("injected", landing_defects(c, d))
            run = run_job(job, d)
            entry[scenario] = {
                **run,
                "date": d,
                "layers": layer_defects(c, d),
                "quarantined": quarantined_rows(c, d),
            }
        results.append(entry)

    # D3: corruption written straight into Bronze. Only the post-write gate can
    # see it, and it needs a Bronze that already exists, which E1 left in place.
    log(f"E2 D3: corrompendo a Bronze de dt={DATE_E1}")
    run = run_job("quality_test_bronze_job", DATE_E1)
    results.append(
        {
            "dataset": "D3",
            "label": "Bronze corrompida diretamente",
            "injected": layer_defects(c, DATE_E1)["bronze"],
            "B": {**run, "date": DATE_E1, "quarantined": quarantined_rows(c, DATE_E1)},
        }
    )
    return results


def purge_partitions(dates: list[str]) -> int:
    """Deletes every object written under the given partition dates, in all buckets."""
    opts = get_delta_storage_options()
    scheme, host = opts["endpoint_url"].split("://")
    s3 = fs.S3FileSystem(
        access_key=opts["access_key_id"],
        secret_key=opts["secret_access_key"],
        endpoint_override=host,
        scheme=scheme,
        region=opts["region"],
    )
    removed = 0
    for bucket in BUCKETS:
        for info in s3.get_file_info(fs.FileSelector(bucket, recursive=True)):
            if info.type == fs.FileType.Directory and any(
                info.path.endswith(f"dt={d}") or info.path.endswith(f"dt={d}.parquet")
                for d in dates
            ):
                s3.delete_dir(info.path)
                removed += 1
            elif info.type == fs.FileType.File and any(
                info.path.endswith(f"dt={d}.parquet") for d in dates
            ):
                s3.delete_file(info.path)
                removed += 1
    return removed


def main() -> None:
    c = conn()
    started = datetime.now(UTC).isoformat(timespec="seconds")
    e1 = e1_overhead(c)
    e2 = e2_detection(c)
    result = {
        "meta": {
            "started_utc": started,
            "finished_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "machine": f"{platform.system()} {platform.machine()}",
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "dagster": dagster.__version__,
            "repetitions": REPS,
        },
        "e1_overhead": e1,
        "e2_detection": e2,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"resultados em {OUT}")
    dates = [DATE_E1] + [d for v in DATES_E2.values() for d in v.values()]
    log(f"limpeza: {purge_partitions(dates)} partições de teste removidas")


if __name__ == "__main__":
    main()
