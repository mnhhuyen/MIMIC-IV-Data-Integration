import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GRAPH_PATH = PROJECT_ROOT / "data" / "graph"
WAREHOUSE_DIR = PROJECT_ROOT / ".spark-warehouse"

GRAPH_PATH.mkdir(parents=True, exist_ok=True)

conda_prefix = os.environ.get("CONDA_PREFIX")
if conda_prefix:
    java_home = Path(conda_prefix)
    if (java_home / "bin" / "java").exists():
        os.environ.setdefault("JAVA_HOME", str(java_home))

spark = SparkSession.builder \
    .appName("Build MIMIC-IV Graph Tables") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR)) \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()


def read_silver(name):
    return spark.read.parquet(str((SILVER_PATH / name).resolve()))


def write_csv(df, name):
    output_path = str((GRAPH_PATH / name).resolve())
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Saved: data/graph/{name}")


print("Reading silver tables...")

patients = read_silver("patients")
admissions = read_silver("admissions")
diagnoses_icd = read_silver("diagnoses_icd")
d_icd = read_silver("d_icd_diagnoses")
labevents = read_silver("labevents")
d_labitems = read_silver("d_labitems")

icustays = read_silver("icustays")
d_items = read_silver("d_items")
chartevents = read_silver("chartevents")
datetimeevents = read_silver("datetimeevents")
inputevents = read_silver("inputevents")
outputevents = read_silver("outputevents")


print("Building node tables...")

patient_nodes = patients.select(
    col("subject_id").cast("string").alias("id"),
    col("gender"),
    col("anchor_age"),
    col("anchor_year"),
    col("anchor_year_group")
).dropDuplicates(["id"])

admission_nodes = admissions.select(
    col("hadm_id").cast("string").alias("id"),
    col("admission_type"),
    col("admission_location"),
    col("discharge_location"),
    col("insurance"),
    col("language"),
    col("marital_status"),
    col("race"),
    col("hospital_expire_flag")
).dropDuplicates(["id"])

disease_nodes = d_icd.select(
    concat_ws("_", col("icd_version").cast("string"), col("icd_code")).alias("id"),
    col("disease_name").alias("name"),
    col("disease_name_norm").alias("name_norm"),
    col("icd_code"),
    col("icd_version")
).dropDuplicates(["id"])

labtest_nodes = d_labitems.select(
    col("itemid").cast("string").alias("id"),
    col("lab_name").alias("name"),
    col("lab_name_norm").alias("name_norm"),
    col("fluid"),
    col("category")
).dropDuplicates(["id"])

icu_stay_nodes = icustays.select(
    col("stay_id").cast("string").alias("id"),
    col("first_careunit"),
    col("last_careunit"),
    col("los")
).dropDuplicates(["id"])

icu_item_nodes = d_items.select(
    col("itemid").cast("string").alias("id"),
    col("item_name").alias("name"),
    col("item_name_norm").alias("name_norm"),
    col("abbreviation"),
    col("linksto"),
    col("category"),
    col("unitname"),
    col("param_type")
).dropDuplicates(["id"])


print("Building edge tables...")

patient_admission_edges = admissions.select(
    col("subject_id").cast("string").alias("source"),
    col("hadm_id").cast("string").alias("target")
).dropna(subset=["source", "target"]).dropDuplicates()

admission_disease_edges = diagnoses_icd.join(
    d_icd,
    on=["icd_code", "icd_version"],
    how="inner"
).select(
    col("hadm_id").cast("string").alias("source"),
    concat_ws("_", col("icd_version").cast("string"), col("icd_code")).alias("target"),
    col("seq_num")
).dropna(subset=["source", "target"]).dropDuplicates()

admission_labtest_edges = labevents.select(
    col("hadm_id").cast("string").alias("source"),
    col("itemid").cast("string").alias("target"),
    col("valuenum"),
    col("valueuom"),
    col("flag"),
    col("charttime")
).dropna(subset=["source", "target"]).dropDuplicates()

admission_icu_edges = icustays.select(
    col("hadm_id").cast("string").alias("source"),
    col("stay_id").cast("string").alias("target")
).dropna(subset=["source", "target"]).dropDuplicates()

icu_chart_edges = chartevents.select(
    col("stay_id").cast("string").alias("source"),
    col("itemid").cast("string").alias("target"),
    col("value"),
    col("valuenum"),
    col("valueuom"),
    col("warning"),
    col("charttime")
).dropna(subset=["source", "target"]).dropDuplicates()

icu_datetime_edges = datetimeevents.select(
    col("stay_id").cast("string").alias("source"),
    col("itemid").cast("string").alias("target"),
    col("datetime_value"),
    col("valueuom"),
    col("warning"),
    col("charttime")
).dropna(subset=["source", "target"]).dropDuplicates()

icu_input_edges = inputevents.select(
    col("stay_id").cast("string").alias("source"),
    col("itemid").cast("string").alias("target"),
    col("amount"),
    col("amountuom"),
    col("rate"),
    col("rateuom"),
    col("ordercategoryname"),
    col("patientweight"),
    col("starttime"),
    col("endtime")
).dropna(subset=["source", "target"]).dropDuplicates()

icu_output_edges = outputevents.select(
    col("stay_id").cast("string").alias("source"),
    col("itemid").cast("string").alias("target"),
    col("value"),
    col("valueuom"),
    col("charttime")
).dropna(subset=["source", "target"]).dropDuplicates()


print("Writing graph CSV files...")

write_csv(patient_nodes, "nodes_patient")
write_csv(admission_nodes, "nodes_admission")
write_csv(disease_nodes, "nodes_disease")
write_csv(labtest_nodes, "nodes_labtest")
write_csv(icu_stay_nodes, "nodes_icu_stay")
write_csv(icu_item_nodes, "nodes_icu_item")

write_csv(patient_admission_edges, "edges_patient_admission")
write_csv(admission_disease_edges, "edges_admission_disease")
write_csv(admission_labtest_edges, "edges_admission_labtest")
write_csv(admission_icu_edges, "edges_admission_icu")
write_csv(icu_chart_edges, "edges_icu_chart")
write_csv(icu_datetime_edges, "edges_icu_datetime")
write_csv(icu_input_edges, "edges_icu_input")
write_csv(icu_output_edges, "edges_icu_output")

print("Graph tables created successfully.")

spark.stop()