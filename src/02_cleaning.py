import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, regexp_replace, to_timestamp
from pyspark.sql.types import DoubleType, LongType, IntegerType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = PROJECT_ROOT / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
WAREHOUSE_DIR = PROJECT_ROOT / ".spark-warehouse"

SILVER_PATH.mkdir(parents=True, exist_ok=True)

conda_prefix = os.environ.get("CONDA_PREFIX")
if conda_prefix:
    java_home = Path(conda_prefix)
    if (java_home / "bin" / "java").exists():
        os.environ.setdefault("JAVA_HOME", str(java_home))

spark = SparkSession.builder \
    .appName("MIMIC-IV Data Cleaning") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR)) \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()


def read_csv(relative_path):
    path = str((RAW_PATH / relative_path).resolve())
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def normalize_text(df, source_col, target_col):
    return df.withColumn(
        target_col,
        lower(
            trim(
                regexp_replace(
                    regexp_replace(col(source_col).cast("string"), r"[^a-zA-Z0-9\s]", " "),
                    r"\s+",
                    " "
                )
            )
        )
    )


def write_parquet(df, name):
    output_path = str((SILVER_PATH / name).resolve())
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved: data/silver/{name}")


print("Reading raw tables...")

patients = read_csv("hosp/patients.csv")
admissions = read_csv("hosp/admissions.csv")
diagnoses_icd = read_csv("hosp/diagnoses_icd.csv")
d_icd_diagnoses = read_csv("hosp/d_icd_diagnoses.csv")
labevents = read_csv("hosp/labevents.csv")
d_labitems = read_csv("hosp/d_labitems.csv")

chartevents = read_csv("icu/chartevents.csv")
d_items = read_csv("icu/d_items.csv")
datetimeevents = read_csv("icu/datetimeevents.csv")
icustays = read_csv("icu/icustays.csv")
inputevents = read_csv("icu/inputevents.csv")
outputevents = read_csv("icu/outputevents.csv")


print("Cleaning patients...")

patients_clean = patients.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("gender"),
    col("anchor_age").cast(IntegerType()).alias("anchor_age"),
    col("anchor_year").cast(IntegerType()).alias("anchor_year"),
    col("anchor_year_group"),
    to_timestamp(col("dod")).alias("dod")
).dropDuplicates(["subject_id"])


print("Cleaning admissions...")

admissions_clean = admissions.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    to_timestamp(col("admittime")).alias("admittime"),
    to_timestamp(col("dischtime")).alias("dischtime"),
    to_timestamp(col("deathtime")).alias("deathtime"),
    col("admission_type"),
    col("admit_provider_id"),
    col("admission_location"),
    col("discharge_location"),
    col("insurance"),
    col("language"),
    col("marital_status"),
    col("race"),
    to_timestamp(col("edregtime")).alias("edregtime"),
    to_timestamp(col("edouttime")).alias("edouttime"),
    col("hospital_expire_flag").cast(IntegerType()).alias("hospital_expire_flag")
).dropna(subset=["subject_id", "hadm_id"]) \
 .dropDuplicates(["hadm_id"])


print("Cleaning diagnosis dictionary...")

d_icd_clean = normalize_text(
    d_icd_diagnoses,
    "long_title",
    "disease_name_norm"
).select(
    col("icd_code"),
    col("icd_version").cast(IntegerType()).alias("icd_version"),
    col("long_title").alias("disease_name"),
    col("disease_name_norm")
).dropna(subset=["icd_code", "icd_version", "disease_name"]) \
 .dropDuplicates(["icd_code", "icd_version"])


print("Cleaning diagnoses...")

diagnoses_clean = diagnoses_icd.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("seq_num").cast(IntegerType()).alias("seq_num"),
    col("icd_code"),
    col("icd_version").cast(IntegerType()).alias("icd_version")
).dropna(subset=["subject_id", "hadm_id", "icd_code", "icd_version"]) \
 .dropDuplicates()


print("Cleaning lab item dictionary...")

d_labitems_clean = normalize_text(
    d_labitems,
    "label",
    "lab_name_norm"
).select(
    col("itemid").cast(LongType()).alias("itemid"),
    col("label").alias("lab_name"),
    col("lab_name_norm"),
    col("fluid"),
    col("category")
).dropna(subset=["itemid", "lab_name"]) \
 .dropDuplicates(["itemid"])


print("Cleaning labevents...")

labevents_clean = labevents.select(
    col("labevent_id").cast(LongType()).alias("labevent_id"),
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("specimen_id").cast(LongType()).alias("specimen_id"),
    col("itemid").cast(LongType()).alias("itemid"),
    col("order_provider_id"),
    to_timestamp(col("charttime")).alias("charttime"),
    to_timestamp(col("storetime")).alias("storetime"),
    col("value"),
    col("valuenum").cast(DoubleType()).alias("valuenum"),
    col("valueuom"),
    col("ref_range_lower").cast(DoubleType()).alias("ref_range_lower"),
    col("ref_range_upper").cast(DoubleType()).alias("ref_range_upper"),
    col("flag"),
    col("priority"),
    col("comments")
).dropna(subset=["labevent_id", "subject_id", "itemid"]) \
 .dropDuplicates(["labevent_id"])


print("Cleaning ICU stays...")

icustays_clean = icustays.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("stay_id").cast(LongType()).alias("stay_id"),
    col("first_careunit"),
    col("last_careunit"),
    to_timestamp(col("intime")).alias("intime"),
    to_timestamp(col("outtime")).alias("outtime"),
    col("los").cast(DoubleType()).alias("los")
).dropna(subset=["subject_id", "hadm_id", "stay_id"]) \
 .dropDuplicates(["stay_id"])


print("Cleaning ICU item dictionary...")

d_items_clean = normalize_text(
    d_items,
    "label",
    "item_name_norm"
).select(
    col("itemid").cast(LongType()).alias("itemid"),
    col("label").alias("item_name"),
    col("item_name_norm"),
    col("abbreviation"),
    col("linksto"),
    col("category"),
    col("unitname"),
    col("param_type"),
    col("lownormalvalue").cast(DoubleType()).alias("lownormalvalue"),
    col("highnormalvalue").cast(DoubleType()).alias("highnormalvalue")
).dropna(subset=["itemid", "item_name"]) \
 .dropDuplicates(["itemid"])


print("Cleaning chartevents...")

chartevents_clean = chartevents.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("stay_id").cast(LongType()).alias("stay_id"),
    col("caregiver_id").cast(LongType()).alias("caregiver_id"),
    to_timestamp(col("charttime")).alias("charttime"),
    to_timestamp(col("storetime")).alias("storetime"),
    col("itemid").cast(LongType()).alias("itemid"),
    col("value"),
    col("valuenum").cast(DoubleType()).alias("valuenum"),
    col("valueuom"),
    col("warning").cast(IntegerType()).alias("warning")
).dropna(subset=["subject_id", "hadm_id", "stay_id", "itemid"]) \
 .dropDuplicates()


print("Cleaning datetimeevents...")

datetimeevents_clean = datetimeevents.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("stay_id").cast(LongType()).alias("stay_id"),
    col("caregiver_id").cast(LongType()).alias("caregiver_id"),
    to_timestamp(col("charttime")).alias("charttime"),
    to_timestamp(col("storetime")).alias("storetime"),
    col("itemid").cast(LongType()).alias("itemid"),
    to_timestamp(col("value")).alias("datetime_value"),
    col("valueuom"),
    col("warning").cast(IntegerType()).alias("warning")
).dropna(subset=["subject_id", "hadm_id", "stay_id", "itemid"]) \
 .dropDuplicates()


print("Cleaning inputevents...")

inputevents_clean = inputevents.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("stay_id").cast(LongType()).alias("stay_id"),
    col("caregiver_id").cast(LongType()).alias("caregiver_id"),
    to_timestamp(col("starttime")).alias("starttime"),
    to_timestamp(col("endtime")).alias("endtime"),
    to_timestamp(col("storetime")).alias("storetime"),
    col("itemid").cast(LongType()).alias("itemid"),
    col("amount").cast(DoubleType()).alias("amount"),
    col("amountuom"),
    col("rate").cast(DoubleType()).alias("rate"),
    col("rateuom"),
    col("orderid").cast(LongType()).alias("orderid"),
    col("linkorderid").cast(LongType()).alias("linkorderid"),
    col("ordercategoryname"),
    col("secondaryordercategoryname"),
    col("ordercomponenttypedescription"),
    col("ordercategorydescription"),
    col("patientweight").cast(DoubleType()).alias("patientweight"),
    col("totalamount").cast(DoubleType()).alias("totalamount"),
    col("totalamountuom"),
    col("isopenbag").cast(IntegerType()).alias("isopenbag"),
    col("continueinnextdept").cast(IntegerType()).alias("continueinnextdept"),
    col("statusdescription"),
    col("originalamount").cast(DoubleType()).alias("originalamount"),
    col("originalrate").cast(DoubleType()).alias("originalrate")
).dropna(subset=["subject_id", "hadm_id", "stay_id", "itemid"]) \
 .dropDuplicates()


print("Cleaning outputevents...")

outputevents_clean = outputevents.select(
    col("subject_id").cast(LongType()).alias("subject_id"),
    col("hadm_id").cast(LongType()).alias("hadm_id"),
    col("stay_id").cast(LongType()).alias("stay_id"),
    col("caregiver_id").cast(LongType()).alias("caregiver_id"),
    to_timestamp(col("charttime")).alias("charttime"),
    to_timestamp(col("storetime")).alias("storetime"),
    col("itemid").cast(LongType()).alias("itemid"),
    col("value").cast(DoubleType()).alias("value"),
    col("valueuom")
).dropna(subset=["subject_id", "hadm_id", "stay_id", "itemid"]) \
 .dropDuplicates()


print("Writing cleaned tables to Parquet...")

write_parquet(patients_clean, "patients")
write_parquet(admissions_clean, "admissions")
write_parquet(d_icd_clean, "d_icd_diagnoses")
write_parquet(diagnoses_clean, "diagnoses_icd")
write_parquet(d_labitems_clean, "d_labitems")
write_parquet(labevents_clean, "labevents")
write_parquet(icustays_clean, "icustays")
write_parquet(d_items_clean, "d_items")
write_parquet(chartevents_clean, "chartevents")
write_parquet(datetimeevents_clean, "datetimeevents")
write_parquet(inputevents_clean, "inputevents")
write_parquet(outputevents_clean, "outputevents")

print("Cleaning completed successfully.")

spark.stop()