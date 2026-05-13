import os
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
WAREHOUSE_DIR = PROJECT_ROOT / ".spark-warehouse"
OUTPUT_DIR = PROJECT_ROOT / "report" / "profiling"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

conda_prefix = os.environ.get("CONDA_PREFIX")
if conda_prefix:
    java_home = Path(conda_prefix)
    if (java_home / "bin" / "java").exists():
        os.environ.setdefault("JAVA_HOME", str(java_home))

spark = SparkSession.builder \
    .appName("MIMIC-IV Data Profiling") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR)) \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

tables = {
    "patients": "hosp/patients.csv",
    "admissions": "hosp/admissions.csv",
    "diagnoses_icd": "hosp/diagnoses_icd.csv",
    "d_icd_diagnoses": "hosp/d_icd_diagnoses.csv",
    "labevents": "hosp/labevents.csv",
    "d_labitems": "hosp/d_labitems.csv",
    "chartevents": "icu/chartevents.csv",
    "d_items": "icu/d_items.csv",
    "datetimeevents": "icu/datetimeevents.csv",
    "icustays": "icu/icustays.csv",
    "inputevents": "icu/inputevents.csv",
    "outputevents": "icu/outputevents.csv",
}

table_summary = []
null_summary = []

for name, file in tables.items():
    print(f"\n===== {name} =====")

    path = str((DATA_PATH / file).resolve())

    df = spark.read.option("header", True).option("inferSchema", True).csv(path)

    row_count = df.count()
    col_count = len(df.columns)

    print("Rows:", row_count)
    print("Columns:", col_count)
    df.printSchema()

    table_summary.append({
        "table": name,
        "rows": row_count,
        "columns": col_count
    })

    nulls = df.select([
        count(when(col(c).isNull() | (col(c).cast("string") == ""), c)).alias(c)
        for c in df.columns
    ])

    nulls.show(truncate=False)

    null_pd = nulls.toPandas()

    for column in null_pd.columns:
        null_summary.append({
            "table": name,
            "column": column,
            "null_count": int(null_pd[column][0])
        })

spark.stop()

pd.DataFrame(table_summary).to_csv(
    OUTPUT_DIR / "table_summary.csv",
    index=False
)

pd.DataFrame(null_summary).to_csv(
    OUTPUT_DIR / "null_statistics.csv",
    index=False
)

print("Profiling results saved.")