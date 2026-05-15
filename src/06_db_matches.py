import re
from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors


# =========================================================
# PATHS
# =========================================================

PROJECT_ROOT = Path("/home/jkl0909/NgYeu/mimic-iv")

MIMIC_PATH = PROJECT_ROOT / "data" / "silver" / "d_icd_diagnoses"
DBPEDIA_PATH = PROJECT_ROOT / "neo4j" / "import" / "dbpedia.csv"
OUTPUT_PATH = PROJECT_ROOT / "neo4j" / "import" / "disease_dbpedia_matches.csv"


# =========================================================
# CONFIG
# =========================================================

TFIDF_THRESHOLD = 0.70


# =========================================================
# CLEANING FUNCTIONS
# =========================================================

def normalize_text(text):
    if pd.isna(text):
        return ""

    text = str(text).lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalize_icd(code):
    if pd.isna(code):
        return ""

    code = str(code).strip().upper()
    code = code.replace(".", "")
    code = code.replace(" ", "")

    return code


# =========================================================
# LOAD DATA
# =========================================================

print("Loading MIMIC diagnoses...")
mimic = pd.read_parquet(MIMIC_PATH)

print("Loading DBpedia...")
dbpedia = pd.read_csv(DBPEDIA_PATH)


# =========================================================
# SELECT COLUMNS
# =========================================================

mimic = mimic[
    [
        "icd_code",
        "icd_version",
        "disease_name"
    ]
].drop_duplicates()

dbpedia = dbpedia[
    [
        "disease",
        "diseaseName",
        "icd9",
        "icd10"
    ]
].drop_duplicates()


# =========================================================
# NORMALIZE DATA
# =========================================================

print("Normalizing MIMIC data...")

mimic["icd_version"] = mimic["icd_version"].astype(str)
mimic["mimic_name_clean"] = mimic["disease_name"].apply(normalize_text)
mimic["icd_clean"] = mimic["icd_code"].apply(normalize_icd)

mimic["disease_id"] = (
    mimic["icd_version"]
    + "_"
    + mimic["icd_clean"]
)

print("Normalizing DBpedia data...")

dbpedia["dbpedia_name_clean"] = dbpedia["diseaseName"].apply(normalize_text)
dbpedia["icd9_clean"] = dbpedia["icd9"].apply(normalize_icd)
dbpedia["icd10_clean"] = dbpedia["icd10"].apply(normalize_icd)

# Giảm lặp vì DBpedia có thể lặp disease do symptom/treatment/category
dbpedia = dbpedia.drop_duplicates(
    subset=[
        "disease",
        "diseaseName",
        "icd9_clean",
        "icd10_clean"
    ]
).reset_index(drop=True)


# =========================================================
# 1. EXACT ICD MATCHING
# =========================================================

print("Building ICD mapping table...")

dbpedia_icd9 = dbpedia[
    dbpedia["icd9_clean"] != ""
].copy()

dbpedia_icd9["icd_version"] = "9"
dbpedia_icd9["icd_clean"] = dbpedia_icd9["icd9_clean"]

dbpedia_icd10 = dbpedia[
    dbpedia["icd10_clean"] != ""
].copy()

dbpedia_icd10["icd_version"] = "10"
dbpedia_icd10["icd_clean"] = dbpedia_icd10["icd10_clean"]

dbpedia_icd = pd.concat(
    [
        dbpedia_icd9,
        dbpedia_icd10
    ],
    ignore_index=True
)

dbpedia_icd = dbpedia_icd[
    [
        "icd_version",
        "icd_clean",
        "disease",
        "diseaseName"
    ]
].drop_duplicates(
    subset=[
        "icd_version",
        "icd_clean"
    ]
)

print("Running exact ICD matching...")

exact_merge = mimic.merge(
    dbpedia_icd,
    on=[
        "icd_version",
        "icd_clean"
    ],
    how="left"
)

exact_matched = exact_merge[
    exact_merge["disease"].notna()
].copy()

exact_result = pd.DataFrame({
    "disease_id": exact_matched["disease_id"],
    "mimic_icd_code": exact_matched["icd_code"],
    "mimic_icd_version": exact_matched["icd_version"],
    "mimic_name": exact_matched["disease_name"],
    "dbpedia_uri": exact_matched["disease"],
    "dbpedia_name": exact_matched["diseaseName"],
    "score": 1.0,
    "method": "icd_exact",
    "match_status": "matched"
})

print(f"Exact ICD matched: {len(exact_result)}")


# =========================================================
# 2. TF-IDF MATCHING FOR UNMATCHED
# =========================================================

unmatched = exact_merge[
    exact_merge["disease"].isna()
].copy().reset_index(drop=True)

print(f"Need TF-IDF matching: {len(unmatched)}")

dbpedia_tfidf = dbpedia[
    dbpedia["dbpedia_name_clean"] != ""
].copy().reset_index(drop=True)

unmatched_valid = unmatched[
    unmatched["mimic_name_clean"] != ""
].copy().reset_index(drop=True)

tfidf_rows = []

if len(unmatched_valid) > 0 and len(dbpedia_tfidf) > 0:

    print("Building TF-IDF vectors...")

    vectorizer = TfidfVectorizer(
        analyzer="char_wb",
        ngram_range=(3, 5),
        lowercase=False,
        min_df=1
    )

    dbpedia_matrix = vectorizer.fit_transform(
        dbpedia_tfidf["dbpedia_name_clean"]
    )

    mimic_matrix = vectorizer.transform(
        unmatched_valid["mimic_name_clean"]
    )

    print("Running nearest-neighbor search...")

    nn = NearestNeighbors(
        n_neighbors=1,
        metric="cosine",
        algorithm="brute",
        n_jobs=-1
    )

    nn.fit(dbpedia_matrix)

    distances, indices = nn.kneighbors(mimic_matrix)

    for i, row in unmatched_valid.iterrows():

        best_idx = indices[i][0]
        cosine_distance = distances[i][0]
        score = 1.0 - cosine_distance

        best = dbpedia_tfidf.iloc[best_idx]

        if score >= TFIDF_THRESHOLD:

            tfidf_rows.append({
                "disease_id": row["disease_id"],
                "mimic_icd_code": row["icd_code"],
                "mimic_icd_version": row["icd_version"],
                "mimic_name": row["disease_name"],
                "dbpedia_uri": best["disease"],
                "dbpedia_name": best["diseaseName"],
                "score": round(float(score), 4),
                "method": "tfidf_char_ngram",
                "match_status": "matched"
            })

        else:

            tfidf_rows.append({
                "disease_id": row["disease_id"],
                "mimic_icd_code": row["icd_code"],
                "mimic_icd_version": row["icd_version"],
                "mimic_name": row["disease_name"],
                "dbpedia_uri": None,
                "dbpedia_name": None,
                "score": round(float(score), 4),
                "method": "none",
                "match_status": "unmatched"
            })


# Các dòng unmatched nhưng không có mimic_name_clean
unmatched_empty = unmatched[
    unmatched["mimic_name_clean"] == ""
].copy()

for _, row in unmatched_empty.iterrows():
    tfidf_rows.append({
        "disease_id": row["disease_id"],
        "mimic_icd_code": row["icd_code"],
        "mimic_icd_version": row["icd_version"],
        "mimic_name": row["disease_name"],
        "dbpedia_uri": None,
        "dbpedia_name": None,
        "score": 0.0,
        "method": "none",
        "match_status": "unmatched"
    })


tfidf_result = pd.DataFrame(tfidf_rows)


# =========================================================
# SAVE RESULTS
# =========================================================

matches_df = pd.concat(
    [
        exact_result,
        tfidf_result
    ],
    ignore_index=True
)

matches_df.to_csv(
    OUTPUT_PATH,
    index=False
)


# =========================================================
# STATS
# =========================================================

total = len(matches_df)

matched = (
    matches_df["match_status"]
    == "matched"
).sum()

unmatched_count = (
    matches_df["match_status"]
    == "unmatched"
).sum()

icd_exact_count = (
    matches_df["method"]
    == "icd_exact"
).sum()

tfidf_count = (
    matches_df["method"]
    == "tfidf_char_ngram"
).sum()

print("\n===================================")
print("ENTITY RESOLUTION COMPLETE")
print("===================================")

print(f"Total diseases      : {total}")
print(f"Matched diseases    : {matched}")
print(f"Unmatched           : {unmatched_count}")
print(f"ICD exact matches   : {icd_exact_count}")
print(f"TF-IDF matches      : {tfidf_count}")
print(f"TF-IDF threshold    : {TFIDF_THRESHOLD}")

print(f"\nSaved to:")
print(OUTPUT_PATH)

print("\nSample matches:")
print(
    matches_df[
        matches_df["match_status"] == "matched"
    ][
        [
            "mimic_name",
            "dbpedia_name",
            "score",
            "method"
        ]
    ].head(20)
)