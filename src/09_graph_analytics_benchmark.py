"""Benchmark Spark-based graph analytics workloads:
- Disease co-occurrence aggregation
- Patient similarity (Jaccard on disease sets)

Matches the 4 optimization configs used in src/07_benchmark_optimization.py
and writes results to report/benchmark_results_graph_analytics.csv
"""
import csv
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, collect_set, size


PROJECT_ROOT = Path(__file__).resolve().parents[1]
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


def run_disease_cooccurrence(spark: SparkSession, use_parquet: bool, use_broadcast: bool, use_cache: bool):
    if use_parquet:
        diagnoses = spark.read.parquet(str((SILVER_PATH / "diagnoses_icd").resolve()))
    else:
        raise RuntimeError("Graph analytics benchmark requires silver Parquet inputs")

    # Build admission->disease lists then explode pairs
    adm_disease = diagnoses.select(col("hadm_id"), col("icd_code")).dropDuplicates()

    if use_cache:
        adm_disease = adm_disease.cache()

    # collect diseases per admission
    adm_sets = (
        adm_disease.groupBy("hadm_id").agg(collect_set("icd_code").alias("diseases"))
    )

    # For demo dataset small size, approximate by counting overlapping diseases per admission pair
    from pyspark.sql.functions import explode

    exploded = (
        adm_sets.select(col("hadm_id"), explode(col("diseases")).alias("icd"))
    )
    # join exploded to itself on hadm_id to form pairs within same admission
    pairs_within = (
        exploded.alias("e1")
        .join(exploded.alias("e2"), on="hadm_id")
        .filter(col("e1.icd") < col("e2.icd"))
        .groupBy(col("e1.icd"), col("e2.icd")).count()
    )

    return pairs_within.count()


def run_patient_similarity(spark: SparkSession, use_parquet: bool, use_broadcast: bool, use_cache: bool):
    if use_parquet:
        diagnoses = spark.read.parquet(str((SILVER_PATH / "diagnoses_icd").resolve()))
    else:
        raise RuntimeError("Graph analytics benchmark requires silver Parquet inputs")

    # Build patient->disease sets
    pat_adm = diagnoses.select(col("subject_id"), col("icd_code")).dropDuplicates()
    pat_sets = pat_adm.groupBy("subject_id").agg(collect_set("icd_code").alias("diseases"))

    if use_cache:
        pat_sets = pat_sets.cache()

    # naive pairwise Jaccard on small dataset: compute for subject_id pairs
    a = pat_sets.alias("a")
    b = pat_sets.alias("b")

    joined = a.join(b, on=(col("a.subject_id") < col("b.subject_id")))

    from pyspark.sql.functions import size, array_intersect

    scored = joined.select(
        col("a.subject_id").alias("p1"),
        col("b.subject_id").alias("p2"),
        size(array_intersect(col("a.diseases"), col("b.diseases"))).alias("shared"),
        (size(col("a.diseases")) + size(col("b.diseases")) - size(array_intersect(col("a.diseases"), col("b.diseases")))).alias("union_size"),
    )

    nonzero = scored.filter(col("shared") > 0).count()
    return nonzero


def write_results(rows, filename="benchmark_results_graph_analytics.csv"):
    output_file = REPORT_PATH / filename
    with output_file.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=["config", "runtime_sec", "metric_count", "notes"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved benchmark results: {output_file}")


def run_all():
    configs = [
        ("Baseline Parquet (shuffle=200)", True, False, False, False, "200"),
        ("Parquet + shuffle=16", True, False, False, False, "16"),
        ("Parquet + broadcast", True, True, False, False, "16"),
        ("Parquet + broadcast + cache + repartition", True, True, True, True, "16"),
    ]

    results = []

    for name, use_parquet, use_broadcast, use_cache, use_repartition, shuffle in configs:
        print(f"\nCONFIG: {name}")
        spark = create_spark(f"GA-{name}", shuffle)
        start = time.time()
        coocc = run_disease_cooccurrence(spark, use_parquet, use_broadcast, use_cache)
        sim = run_patient_similarity(spark, use_parquet, use_broadcast, use_cache)
        elapsed = time.time() - start
        results.append({"config": name, "runtime_sec": round(elapsed, 4), "metric_count": coocc + sim, "notes": "coocc+similarity"})
        spark.stop()

    write_results(results)
    print("\nCompleted graph analytics benchmarks.")


if __name__ == "__main__":
    run_all()
