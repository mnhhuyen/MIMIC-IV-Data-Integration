import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GRAPH_PATH = PROJECT_ROOT / "data" / "graph"
REPORT_PATH = PROJECT_ROOT / "report" / "graph"
WAREHOUSE_DIR = PROJECT_ROOT / ".spark-warehouse"

REPORT_PATH.mkdir(parents=True, exist_ok=True)

conda_prefix = os.environ.get("CONDA_PREFIX")
if conda_prefix:
    java_home = Path(conda_prefix)
    if (java_home / "bin" / "java").exists():
        os.environ.setdefault("JAVA_HOME", str(java_home))

spark = SparkSession.builder \
    .appName("Validate MIMIC-IV Medical Event Graph") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR)) \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()


def read_graph_csv(name):
    path = str((GRAPH_PATH / name).resolve())
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def save_report(df, name):
    path = str((REPORT_PATH / name).resolve())
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f"Saved report: {REPORT_PATH / name}")


def union_reports(reports):
    if not reports:
        return None

    result = reports[0]
    for df in reports[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result


def check_duplicate_ids(df, table_name):
    return df.groupBy("id") \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) \
        .withColumn("table", lit(table_name)) \
        .select("table", "id", "count")


def get_existing_columns(df, candidate_columns):
    return [c for c in candidate_columns if c in df.columns]


def check_duplicate_edges(df, table_name, key_columns):
    existing_keys = get_existing_columns(df, key_columns)

    return df.groupBy(existing_keys) \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) \
        .withColumn("table", lit(table_name)) \
        .select(["table"] + existing_keys + ["count"])


def check_repeated_relation_edges(df, table_name):
    return df.groupBy("source", "target") \
        .agg(count("*").alias("event_count")) \
        .filter(col("event_count") > 1) \
        .withColumn("table", lit(table_name)) \
        .select("table", "source", "target", "event_count")


def check_missing_source(edge_df, source_node_df, table_name):
    return edge_df.select("source").dropDuplicates() \
        .join(
            source_node_df.select(col("id").alias("source")),
            on="source",
            how="left_anti"
        ) \
        .withColumn("table", lit(table_name)) \
        .select("table", "source")


def check_missing_target(edge_df, target_node_df, table_name):
    return edge_df.select("target").dropDuplicates() \
        .join(
            target_node_df.select(col("id").alias("target")),
            on="target",
            how="left_anti"
        ) \
        .withColumn("table", lit(table_name)) \
        .select("table", "target")


print("Reading graph tables...")

nodes_patient = read_graph_csv("nodes_patient")
nodes_admission = read_graph_csv("nodes_admission")
nodes_disease = read_graph_csv("nodes_disease")
nodes_labtest = read_graph_csv("nodes_labtest")
nodes_icu_stay = read_graph_csv("nodes_icu_stay")
nodes_icu_item = read_graph_csv("nodes_icu_item")

edges_patient_admission = read_graph_csv("edges_patient_admission")
edges_admission_disease = read_graph_csv("edges_admission_disease")
edges_admission_labtest = read_graph_csv("edges_admission_labtest")
edges_admission_icu = read_graph_csv("edges_admission_icu")
edges_icu_chart = read_graph_csv("edges_icu_chart")
edges_icu_datetime = read_graph_csv("edges_icu_datetime")
edges_icu_input = read_graph_csv("edges_icu_input")
edges_icu_output = read_graph_csv("edges_icu_output")


node_tables = {
    "nodes_patient": nodes_patient,
    "nodes_admission": nodes_admission,
    "nodes_disease": nodes_disease,
    "nodes_labtest": nodes_labtest,
    "nodes_icu_stay": nodes_icu_stay,
    "nodes_icu_item": nodes_icu_item,
}

edge_tables = {
    "edges_patient_admission": edges_patient_admission,
    "edges_admission_disease": edges_admission_disease,
    "edges_admission_labtest": edges_admission_labtest,
    "edges_admission_icu": edges_admission_icu,
    "edges_icu_chart": edges_icu_chart,
    "edges_icu_datetime": edges_icu_datetime,
    "edges_icu_input": edges_icu_input,
    "edges_icu_output": edges_icu_output,
}


edge_duplicate_keys = {
    "edges_patient_admission": [
        "source",
        "target",
    ],

    "edges_admission_disease": [
        "source",
        "target",
        "seq_num",
    ],

    "edges_admission_labtest": [
        "source",
        "target",
        "charttime",
        "valuenum",
        "valueuom",
        "flag",
    ],

    "edges_admission_icu": [
        "source",
        "target",
    ],

    "edges_icu_chart": [
        "source",
        "target",
        "charttime",
        "value",
        "valuenum",
        "valueuom",
        "warning",
    ],

    "edges_icu_datetime": [
        "source",
        "target",
        "charttime",
        "datetime_value",
        "valueuom",
        "warning",
    ],

    "edges_icu_input": [
        "source",
        "target",
        "starttime",
        "endtime",
        "amount",
        "amountuom",
        "rate",
        "rateuom",
        "ordercategoryname",
    ],

    "edges_icu_output": [
        "source",
        "target",
        "charttime",
        "value",
        "valueuom",
    ],
}


print("\n========== BASIC COUNTS ==========")

count_rows = []

for name, df in node_tables.items():
    row_count = df.count()
    null_id_count = df.filter(col("id").isNull()).count()
    count_rows.append((name, "node", row_count, null_id_count))

for name, df in edge_tables.items():
    row_count = df.count()
    null_endpoint_count = df.filter(
        col("source").isNull() | col("target").isNull()
    ).count()
    count_rows.append((name, "edge", row_count, null_endpoint_count))

summary_df = spark.createDataFrame(
    count_rows,
    ["table", "type", "row_count", "null_id_or_endpoint_count"]
)

summary_df.show(truncate=False)
save_report(summary_df, "graph_validation_summary")


print("\n========== CHECK DUPLICATE NODE IDS ==========")

duplicate_node_reports = [
    check_duplicate_ids(df, name)
    for name, df in node_tables.items()
]

duplicate_nodes_df = union_reports(duplicate_node_reports)

duplicate_nodes_df.show(50, truncate=False)
save_report(duplicate_nodes_df, "duplicate_node_ids")


print("\n========== CHECK TRUE DUPLICATE EDGES ==========")

duplicate_edge_reports = []

for name, df in edge_tables.items():
    key_columns = edge_duplicate_keys[name]
    duplicate_edge_reports.append(
        check_duplicate_edges(df, name, key_columns)
    )

duplicate_edges_df = union_reports(duplicate_edge_reports)

duplicate_edges_df.show(50, truncate=False)
save_report(duplicate_edges_df, "duplicate_edges_true")


print("\n========== CHECK REPEATED MEDICAL EVENT RELATIONS ==========")

repeated_relation_reports = []

event_level_tables = [
    "edges_admission_labtest",
    "edges_icu_chart",
    "edges_icu_datetime",
    "edges_icu_input",
    "edges_icu_output",
]

for name in event_level_tables:
    repeated_relation_reports.append(
        check_repeated_relation_edges(edge_tables[name], name)
    )

repeated_relations_df = union_reports(repeated_relation_reports)

repeated_relations_df.show(50, truncate=False)
save_report(repeated_relations_df, "repeated_medical_event_relations")


print("\n========== CHECK BROKEN REFERENCES ==========")

reference_checks = [
    (
        "edges_patient_admission",
        edges_patient_admission,
        nodes_patient,
        nodes_admission,
    ),
    (
        "edges_admission_disease",
        edges_admission_disease,
        nodes_admission,
        nodes_disease,
    ),
    (
        "edges_admission_labtest",
        edges_admission_labtest,
        nodes_admission,
        nodes_labtest,
    ),
    (
        "edges_admission_icu",
        edges_admission_icu,
        nodes_admission,
        nodes_icu_stay,
    ),
    (
        "edges_icu_chart",
        edges_icu_chart,
        nodes_icu_stay,
        nodes_icu_item,
    ),
    (
        "edges_icu_datetime",
        edges_icu_datetime,
        nodes_icu_stay,
        nodes_icu_item,
    ),
    (
        "edges_icu_input",
        edges_icu_input,
        nodes_icu_stay,
        nodes_icu_item,
    ),
    (
        "edges_icu_output",
        edges_icu_output,
        nodes_icu_stay,
        nodes_icu_item,
    ),
]

missing_source_reports = []
missing_target_reports = []

for table_name, edge_df, source_df, target_df in reference_checks:
    missing_source_reports.append(
        check_missing_source(edge_df, source_df, table_name)
    )
    missing_target_reports.append(
        check_missing_target(edge_df, target_df, table_name)
    )

missing_sources_df = union_reports(missing_source_reports)
missing_targets_df = union_reports(missing_target_reports)

print("\nMissing source nodes:")
missing_sources_df.show(50, truncate=False)
save_report(missing_sources_df, "missing_source_nodes")

print("\nMissing target nodes:")
missing_targets_df.show(50, truncate=False)
save_report(missing_targets_df, "missing_target_nodes")


print("\n========== VALIDATION RESULT ==========")

total_duplicate_nodes = duplicate_nodes_df.count()
total_true_duplicate_edges = duplicate_edges_df.count()
total_repeated_medical_relations = repeated_relations_df.count()
total_missing_sources = missing_sources_df.count()
total_missing_targets = missing_targets_df.count()

print(f"Duplicate node IDs: {total_duplicate_nodes}")
print(f"True duplicate edges: {total_true_duplicate_edges}")
print(f"Repeated medical event relations: {total_repeated_medical_relations}")
print(f"Missing source nodes: {total_missing_sources}")
print(f"Missing target nodes: {total_missing_targets}")

if (
    total_duplicate_nodes == 0
    and total_true_duplicate_edges == 0
    and total_missing_sources == 0
    and total_missing_targets == 0
):
    print("Graph validation passed successfully.")
    print("Repeated medical event relations are expected in time-series clinical data.")
else:
    print("Graph validation completed with issues. Check report/graph/.")

spark.stop()