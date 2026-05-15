import csv
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
REPORT_PATH = PROJECT_ROOT / "report"
WAREHOUSE_DIR = PROJECT_ROOT / ".spark-warehouse"

REPORT_PATH.mkdir(parents=True, exist_ok=True)

conda_prefix = os.environ.get("CONDA_PREFIX")
if conda_prefix:
    java_home = Path(conda_prefix)
    if (java_home / "bin" / "java").exists():
        os.environ.setdefault("JAVA_HOME", str(java_home))


def create_spark(app_name: str, shuffle_partitions: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


def load_source_tables(spark: SparkSession, use_parquet: bool):
    if use_parquet:
        patients = spark.read.parquet(str((SILVER_PATH / "patients").resolve()))
        admissions = spark.read.parquet(str((SILVER_PATH / "admissions").resolve()))
        diagnoses = spark.read.parquet(str((SILVER_PATH / "diagnoses_icd").resolve()))
        d_icd_diagnoses = spark.read.parquet(str((SILVER_PATH / "d_icd_diagnoses").resolve()))
        labevents = spark.read.parquet(str((SILVER_PATH / "labevents").resolve()))
        d_labitems = spark.read.parquet(str((SILVER_PATH / "d_labitems").resolve()))
    else:
        raw_hosp = RAW_PATH / "hosp"
        patients = spark.read.option("header", True).option("inferSchema", True).csv(
            str((raw_hosp / "patients.csv").resolve())
        )
        admissions = spark.read.option("header", True).option("inferSchema", True).csv(
            str((raw_hosp / "admissions.csv").resolve())
        )
        diagnoses = spark.read.option("header", True).option("inferSchema", True).csv(
            str((raw_hosp / "diagnoses_icd.csv").resolve())
        )
        d_icd_diagnoses = spark.read.option("header", True).option("inferSchema", True).csv(
            str((raw_hosp / "d_icd_diagnoses.csv").resolve())
        )
        labevents = spark.read.option("header", True).option("inferSchema", True).csv(
            str((raw_hosp / "labevents.csv").resolve())
        )
        d_labitems = spark.read.option("header", True).option("inferSchema", True).csv(
            str((raw_hosp / "d_labitems.csv").resolve())
        )

    return patients, admissions, diagnoses, d_icd_diagnoses, labevents, d_labitems


def run_graph_build_benchmark(
    spark: SparkSession,
    use_parquet: bool,
    use_broadcast: bool,
    use_cache: bool,
    use_repartition: bool,
) -> int:
    patients, admissions, diagnoses, d_icd_diagnoses, labevents, d_labitems = load_source_tables(
        spark, use_parquet
    )

    if use_repartition:
        admissions = admissions.repartition("subject_id", "hadm_id")
        diagnoses = diagnoses.repartition("hadm_id", "icd_code")
        labevents = labevents.repartition("hadm_id", "itemid")

    if use_cache:
        patients = patients.cache()
        admissions = admissions.cache()
        diagnoses = diagnoses.cache()
        d_icd_diagnoses = d_icd_diagnoses.cache()
        labevents = labevents.cache()
        d_labitems = d_labitems.cache()

    admissions_small = admissions.select("subject_id", "hadm_id")
    patients_small = patients.select("subject_id")
    icd_dim = d_icd_diagnoses
    lab_dim = d_labitems

    if use_broadcast:
        patients_small = broadcast(patients_small)
        icd_dim = broadcast(icd_dim)
        lab_dim = broadcast(lab_dim)

    patient_admission_edges = admissions.join(patients_small, on="subject_id").select(
        "subject_id", "hadm_id"
    )
    admission_disease_edges = diagnoses.join(admissions_small, on="hadm_id").join(
        icd_dim,
        on=["icd_code", "icd_version"],
    )
    admission_labtest_edges = labevents.join(admissions_small, on="hadm_id").join(
        lab_dim,
        on="itemid",
    )

    return (
        patient_admission_edges.count()
        + admission_disease_edges.count()
        + admission_labtest_edges.count()
    )


def write_results(rows):
    output_file = REPORT_PATH / "benchmark_results.csv"
    with output_file.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=["config", "runtime_sec", "rows", "notes"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved benchmark results: {output_file}")


results = []


print("=" * 60)
print("SPARK OPTIMIZATION BENCHMARK")
print("=" * 60)


print("\nCONFIG 1: Baseline CSV")
spark1 = create_spark("Benchmark-Baseline-CSV", "200")
start_time = time.time()
rows = run_graph_build_benchmark(
    spark1,
    use_parquet=False,
    use_broadcast=False,
    use_cache=False,
    use_repartition=False,
)
runtime = time.time() - start_time
results.append(
    {
        "config": "Baseline CSV",
        "runtime_sec": round(runtime, 4),
        "rows": rows,
        "notes": "Raw CSV, default-style shuffle",
    }
)
spark1.stop()


print("\nCONFIG 2: Parquet + tuned shuffle")
spark2 = create_spark("Benchmark-Parquet-Shuffle", "16")
start_time = time.time()
rows = run_graph_build_benchmark(
    spark2,
    use_parquet=True,
    use_broadcast=False,
    use_cache=False,
    use_repartition=False,
)
runtime = time.time() - start_time
results.append(
    {
        "config": "Parquet + shuffle=16",
        "runtime_sec": round(runtime, 4),
        "rows": rows,
        "notes": "Columnar format, smaller shuffle parallelism",
    }
)
spark2.stop()


print("\nCONFIG 3: Parquet + broadcast join")
spark3 = create_spark("Benchmark-Parquet-Broadcast", "16")
start_time = time.time()
rows = run_graph_build_benchmark(
    spark3,
    use_parquet=True,
    use_broadcast=True,
    use_cache=False,
    use_repartition=False,
)
runtime = time.time() - start_time
results.append(
    {
        "config": "Parquet + broadcast join",
        "runtime_sec": round(runtime, 4),
        "rows": rows,
        "notes": "Broadcast the smaller ICD mapping table",
    }
)
spark3.stop()


print("\nCONFIG 4: Parquet + broadcast + repartition + cache")
spark4 = create_spark("Benchmark-All-Optimizations", "16")
start_time = time.time()
rows = run_graph_build_benchmark(
    spark4,
    use_parquet=True,
    use_broadcast=True,
    use_cache=True,
    use_repartition=True,
)
runtime = time.time() - start_time
results.append(
    {
        "config": "Parquet + broadcast + repartition + cache",
        "runtime_sec": round(runtime, 4),
        "rows": rows,
        "notes": "Repartition on icd_code, cache both inputs",
    }
)
spark4.stop()


write_results(results)

print("\n" + "=" * 60)
print("OPTIMIZATION RESULTS")
print("=" * 60)

baseline_runtime = results[0]["runtime_sec"]
for result in results:
    if result["config"] == results[0]["config"]:
        speedup_text = "1.00x"
    else:
        speedup_text = f"{baseline_runtime / result['runtime_sec']:.2f}x"
    print(
        f"{result['config']}: {result['runtime_sec']} sec | rows={result['rows']} | speedup={speedup_text}"
    )

print("\nBenchmark completed. Review report/benchmark_results.csv for comparison.")