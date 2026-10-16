"""Experiments behind the article's results. Run inside the dagster container:

    docker compose exec dagster python /app/scripts/run_experiments.py

Scenarios: A = baseline_no_gates_job, B = pureflow_pipeline_job.
E1 measures overhead on a clean dataset (1M sales, 100k customers): one warm-up,
then REPS interleaved A/B runs. E2 measures detection on defective datasets,
counting defects per layer straight from the Delta tables. E3 makes the quarantine
write fail; the storage outage during a write is driven from the host by
scripts/run_write_fault.py.

`--only e2,e3` reruns some experiments and keeps the rest of results.json.
Results go to docs/experiments/results.json. Test partitions are deleted at the end.
"""

import argparse
import json
import os
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
from deltalake import DeltaTable  # noqa: E402
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
EXPERIMENT_DEFS = "/app/scripts/experiment_defs.py"
OUT = Path("/app/docs/experiments/results.json")

# One partition date per experiment, far from any real run, so nothing collides.
DATE_E1 = "2030-01-01"
DATES_E2 = {
    "D1": {"A": "2030-02-01", "B": "2030-02-02"},
    "D2": {"A": "2030-03-01", "B": "2030-03-02"},
    "D4": {"A": "2030-04-01", "B": "2030-04-02"},
    "D3": {"A": "2030-05-01", "B": "2030-05-02"},
}
DATES_E3 = {"F2": "2030-06-01", "F3": "2030-06-02"}
DATE_WRITE_FAULT = "2030-07-01"

# D4 passes the landing gates; only the Bronze check (product, price) can see its defects.
D4_SALES, D4_CUSTOMERS = 5000, 1000
D4_NULL_PRODUCT, D4_NEGATIVE_PRICE = 25, 40

QUARANTINE_ERROR = re.compile(r"Failed to quarantine [^\n]*")
STEP_DONE = re.compile(r'Finished execution of step "([^"]+)" in ([\d.]+)(ms|s)')


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def run_job(job: str, date: str, defs_file: str = ORCHESTRATION, fault: str | None = None) -> dict:
    """Runs a Dagster job pinned to `date` and returns wall time and outcome."""
    cfg = {"resources": {"execution_date_resource": {"config": {"date": date}}}}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(cfg, f)
        cfg_path = f.name
    cmd = ["dagster", "job", "execute", "-f", defs_file, "-j", job, "-c", cfg_path]
    env = {**os.environ, "PUREFLOW_FAULT": fault} if fault else None
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)  # noqa: S603
    elapsed = time.perf_counter() - start
    out = proc.stdout + proc.stderr
    # "... - <job> - <run_id> - <step_key> - STEP_FAILURE - ..."
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
        "quarantine_write_failed": "Failed to quarantine" in out,
        "errors": sorted({m.strip() for m in QUARANTINE_ERROR.findall(out)})[:3],
    }


def write_landing(
    execution_date: str,
    n_sales: int = D4_SALES,
    n_customers: int = D4_CUSTOMERS,
    null_product: int = 0,
    negative_price: int = 0,
) -> None:
    """Landing files with clean ids; optional defects only the Bronze gate can see."""
    customers = pd.DataFrame(generate_base_customers(n_customers))
    customers["created_at"] = execution_date
    sales = pd.DataFrame(generate_base_sales(n_sales, n_customers, (10.0, 5000.0), execution_date))
    sales["product"] = sales["product"].astype(object)
    if null_product:
        sales.loc[: null_product - 1, "product"] = None
    if negative_price:
        sales.loc[n_sales - negative_price :, "price"] *= -1

    p = get_s3_paths(execution_date)
    c = conn()
    try:
        c.register("customers", customers)
        c.register("sales", sales)
        c.execute(f"COPY customers TO '{p['customers_landing']}' (FORMAT 'JSON', ARRAY TRUE)")
        c.execute(f"COPY sales TO '{p['sales_landing']}' (FORMAT 'CSV', HEADER TRUE)")
    finally:
        c.close()


def generate_bronze_only_defects(execution_date: str) -> None:
    """D4: defects only in columns the Bronze gate checks."""
    write_landing(execution_date, null_product=D4_NULL_PRODUCT, negative_price=D4_NEGATIVE_PRICE)


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

    results.append(d3_bronze_corruption(c))
    return results


def d3_bronze_corruption(c) -> dict:
    """Bronze is overwritten outside the pipeline, then Silver and Gold are rebuilt from it."""
    entry = {"dataset": "D3", "label": "Bronze corrompida diretamente"}
    jobs = {
        "A": ("baseline_no_gates_job", "baseline_downstream_job"),
        "B": ("pureflow_pipeline_job", "gated_downstream_job"),
    }
    for scenario, (full_job, downstream_job) in jobs.items():
        d = DATES_E2["D3"][scenario]
        log(f"E2 D3 cenario {scenario}: dt={d}")
        write_landing(d)
        clean = run_job(full_job, d)
        corrupt = run_job("corrupt_bronze_job", d, EXPERIMENT_DEFS)
        entry.setdefault("injected", layer_defects(c, d)["bronze"])
        run = run_job(downstream_job, d, EXPERIMENT_DEFS)
        entry[scenario] = {
            **run,
            "date": d,
            "clean_run_success": clean["success"],
            "corrupt_run_success": corrupt["success"],
            "layers": layer_defects(c, d),
            "quarantined": quarantined_rows(c, d),
        }
    return entry


def landing_rows(c, date: str) -> dict:
    p = get_s3_paths(date)
    return {
        "sales": scalar(c, f"SELECT count(*) FROM read_csv_auto('{p['sales_landing']}')"),
        "customers": scalar(c, f"SELECT count(*) FROM read_json_auto('{p['customers_landing']}')"),
    }


def e3_quarantine_failure(c) -> list:
    """F2 and F3: D2 and D4 again, with the storage rejecting every quarantine write."""
    cases = {
        "F2": ("Falha na quarentena do pre-voo (dados de D2)", corrupt_landing_zone),
        "F3": ("Falha na quarentena do pos-escrita (dados de D4)", generate_bronze_only_defects),
    }
    results = []
    for key, (label, make) in cases.items():
        d = DATES_E3[key]
        log(f"E3 {key}: dt={d}")
        make(execution_date=d)
        run = run_job("pureflow_pipeline_job", d, EXPERIMENT_DEFS, fault="quarantine")
        results.append(
            {
                "case": key,
                "label": label,
                **run,
                "date": d,
                "landing_rows_after": landing_rows(c, d),
                "layers": layer_defects(c, d),
                "quarantined": quarantined_rows(c, d),
            }
        )
    return results


def table_state(c, path: str) -> dict:
    """Committed version, visible rows, and data files no committed version references."""
    try:
        opts = get_delta_storage_options()
        table = DeltaTable(path, storage_options=opts)
        committed = set()
        for v in range(table.version() + 1):
            committed |= {
                u.rsplit("/", 1)[-1] for u in DeltaTable(path, v, storage_options=opts).file_uris()
            }
        on_storage = {
            f.rsplit("/", 1)[-1]
            for (f,) in c.execute(f"SELECT file FROM glob('{path}/*.parquet')").fetchall()
        }
        return {
            "readable": True,
            "version": table.version(),
            "rows": scalar(c, f"SELECT count(*) FROM delta_scan('{path}')"),
            "uncommitted_files": len(on_storage - committed),
        }
    except Exception as e:
        return {"readable": False, "error": str(e)[:300]}


def tables_state(c, date: str) -> dict:
    p = get_s3_paths(date)
    keys = ("sales_bronze", "customers_bronze", "sales_silver", "customers_silver", "sales_summary")
    return {k: table_state(c, p[k]) for k in keys}


def delete_silver_stage(date: str) -> None:
    s3 = s3_filesystem()
    for model in ("sales_silver", "customers_silver"):
        key = f"{S3_BUCKET_SILVER}/_stage/{model}/dt={date}.parquet"
        if s3.get_file_info(key).type == fs.FileType.File:
            s3.delete_file(key)


def write_fault_phase(phase: str) -> dict:
    """Container side of scripts/run_write_fault.py. The new Landing has 1M sales, so the
    Silver write lasts long enough to be interrupted."""
    c = conn()
    d = DATE_WRITE_FAULT
    if phase == "write-fault-prepare":
        write_landing(d)
        run = run_job("pureflow_pipeline_job", d)
        delete_silver_stage(d)
        write_landing(d, n_sales=1_000_000, n_customers=D4_CUSTOMERS)
        return {"clean_run_success": run["success"], "tables": tables_state(c, d)}
    if phase == "write-fault-run":
        return run_job("pureflow_pipeline_job", d)
    if phase == "write-fault-inspect":
        return {"tables": tables_state(c, d)}
    if phase == "write-fault-recover":
        run = run_job("pureflow_pipeline_job", d)
        return {**run, "tables": tables_state(c, d)}
    if phase == "write-fault-cleanup":
        return {"removed": purge_partitions([d])}
    raise ValueError(phase)


def s3_filesystem() -> fs.S3FileSystem:
    opts = get_delta_storage_options()
    scheme, host = opts["endpoint_url"].split("://")
    return fs.S3FileSystem(
        access_key=opts["access_key_id"],
        secret_key=opts["secret_access_key"],
        endpoint_override=host,
        scheme=scheme,
        region=opts["region"],
    )


def purge_partitions(dates: list[str]) -> int:
    """Deletes every object written under the given partition dates, in all buckets."""
    s3 = s3_filesystem()
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default="e1,e2,e3", help="comma-separated: e1,e2,d3,e3")
    parser.add_argument("--phase", help="write-fault phase, driven by run_write_fault.py")
    args = parser.parse_args()
    if args.phase:
        print(json.dumps(write_fault_phase(args.phase)), flush=True)
        return
    only = set(args.only.split(","))

    c = conn()
    started = datetime.now(UTC).isoformat(timespec="seconds")
    result = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {}
    if "e1" in only:
        result["e1_overhead"] = e1_overhead(c)
    if "e2" in only:
        result["e2_detection"] = e2_detection(c)
    elif "d3" in only:
        others = [e for e in result.get("e2_detection", []) if e["dataset"] != "D3"]
        result["e2_detection"] = [*others, d3_bronze_corruption(c)]
    if "e3" in only:
        result.setdefault("e3_failures", {})["quarantine"] = e3_quarantine_failure(c)
    result["meta"] = {
        **result.get("meta", {}),
        f"run_{'_'.join(sorted(only))}_utc": [
            started,
            datetime.now(UTC).isoformat(timespec="seconds"),
        ],
        "machine": f"{platform.system()} {platform.machine()}",
        "python": platform.python_version(),
        "duckdb": duckdb.__version__,
        "dagster": dagster.__version__,
        "repetitions": REPS,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"resultados em {OUT}")
    dates = [DATE_E1] + [d for v in DATES_E2.values() for d in v.values()] + list(DATES_E3.values())
    log(f"limpeza: {purge_partitions(dates)} partições de teste removidas")


if __name__ == "__main__":
    main()
