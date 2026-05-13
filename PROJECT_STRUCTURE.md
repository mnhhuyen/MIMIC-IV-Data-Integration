# MIMIC-IV Knowledge Graph Project - Complete Structure

**Last Updated:** Auto-generated documentation  
**Project Root:** `./mimic-iv` (or your local path)  
**Neo4j Server:** `neo4j://localhost:7687` | **Browser:** `http://localhost:7474`  
**Credentials:** Configure in your `.env` file (not in source code)

---

## 📁 Directory Structure Overview

```
mimic-iv/
├── data/                              # 3-Layer Data Pipeline
│   ├── raw/                           # Layer 0: MIMIC-IV raw CSV files
│   │   └── mimic-iv-clinical-database-demo-2.2/
│   │       ├── hosp/
│   │       │   ├── patients.csv              (100 patients)
│   │       │   ├── admissions.csv            (275 hospital admissions)
│   │       │   ├── diagnoses_icd.csv         (ICD-10 diagnosis codes)
│   │       │   ├── d_icd_diagnoses.csv       (ICD mapping reference)
│   │       │   ├── labevents.csv             (Lab test results)
│   │       │   └── d_labitems.csv            (Lab item definitions)
│   │       └── icu/
│   │           ├── icustays.csv              (ICU admissions)
│   │           ├── chartevents.csv           (Vital signs)
│   │           ├── datetimeevents.csv        (Date measurements)
│   │           ├── inputevents.csv           (Medications/fluids)
│   │           ├── outputevents.csv          (Output measurements)
│   │           └── d_items.csv               (ICU item definitions)
│   │
│   ├── silver/                        # Layer 1: Cleaned Parquet files (from 02_cleaning.py)
│   │   ├── patients/
│   │   ├── admissions/
│   │   ├── diagnoses_icd/
│   │   ├── d_icd_diagnoses/
│   │   ├── labevents/
│   │   ├── d_labitems/
│   │   ├── icustays/
│   │   ├── chartevents/
│   │   ├── datetimeevents/
│   │   ├── inputevents/
│   │   ├── outputevents/
│   │   └── d_items/
│   │
│   └── graph/                         # Layer 2: Graph CSV exports for Neo4j (from 03_build_graph_tables.py)
│       ├── nodes_patient/             (100 patient nodes)
│       ├── nodes_admission/           (275 admission nodes)
│       ├── nodes_disease/             (200+ disease nodes)
│       ├── nodes_icu_stay/            (150 ICU stay nodes)
│       ├── nodes_labtest/             (100 lab test nodes)
│       ├── nodes_icu_item/            (200 ICU item nodes)
│       ├── edges_patient_admission/   (100 edges: Patient→Admission)
│       ├── edges_admission_disease/   (400+ edges: Admission→Disease)
│       ├── edges_admission_labtest/   (600+ edges: Admission→LabTest)
│       ├── edges_admission_icu/       (275 edges: Admission→ICUStay)
│       ├── edges_icu_chart/           (1000+ edges: ICUStay→Item via CHART)
│       ├── edges_icu_datetime/        (500+ edges: ICUStay→Item via DATETIME)
│       ├── edges_icu_input/           (300+ edges: ICUStay→Item via INPUT)
│       ├── edges_icu_output/          (200+ edges: ICUStay→Item via OUTPUT)
│       └── edges_icu_item/            (Aggregated ICU item relationships)
│
├── neo4j/                             # Docker volumes & Neo4j data
│   ├── data/                          # Persisted Neo4j database (container: neo4j-mimic)
│   │   ├── databases/
│   │   ├── transactions/
│   │   └── [Neo4j binary store files]
│   │
│   └── import/                        # Staging area for LOAD CSV (stores converted CSVs)
│
├── src/                               # Source code: ETL pipeline & analytics
│   ├── 01_data_profile.py             # STEP 1: Data quality analysis
│   ├── 02_cleaning.py                 # STEP 2: Normalization & type-casting
│   ├── 03_build_graph_tables.py       # STEP 3: Transform to graph nodes/edges
│   ├── 04_validate_graph.py           # STEP 4: Validation & integrity checks
│   ├── 05_prepare_import_csv.py       # Helper: Old parquet-to-CSV converter
│   ├── 06_graph_commented.cypher      # STEP 5: Neo4j import script (fully commented)
│   │
│   └── recommend_from_copilot/        # Generated AI-assisted guides
│       ├── 08_graph_analytics.cypher  # Advanced Cypher analytics queries
│       ├── graph_analytics.py         # Python API for graph operations
│       ├── ANALYTICS_GUIDE.md         # Step-by-step implementation guide
│       └── QUICK_REFERENCE.md         # Algorithm explanations & comparisons
│
├── notebooks/                         # Jupyter analysis notebooks
│   └── mimic_data.ipynb               # Data exploration & visualization
│
├── report/                            # Generated output reports
│   ├── profiling/                     # Output from 01_data_profile.py
│   │   ├── table_summary.csv          # Row/column counts per table
│   │   └── null_statistics.csv        # Null value counts by column
│   │
│   └── graph/                         # Output from 04_validate_graph.py
│       ├── duplicate_edges/           # Detected duplicate relationships
│       ├── duplicate_edges_true/      # Verified duplicates
│       ├── duplicate_node_ids/        # Node ID conflicts
│       ├── graph_validation_summary/  # Overall validation report
│       ├── missing_source_nodes/      # Orphaned edges (missing source)
│       ├── missing_target_nodes/      # Orphaned edges (missing target)
│       └── repeated_medical_event_relations/  # Repeated events analysis
│
├── .spark-warehouse/                  # Spark temporary working directory (auto-created)
├── PROJECT_STRUCTURE.md               # This file
└── readme.md                          # (empty - to be created)
```

---

## 🔄 Data Pipeline Flow

### **Stage 1: Data Profiling** → `report/profiling/`
**Script:** `src/01_data_profile.py`
- **Input:** `data/raw/mimic-iv-clinical-database-demo-2.2/` (12 CSV tables)
- **Process:**
  - Read MIMIC-IV CSV files with Apache Spark
  - Compute schema, row count, column count for each table
  - Count null values per column
  - Generate data quality metrics
- **Output:**
  - `report/profiling/table_summary.csv` - Table statistics
  - `report/profiling/null_statistics.csv` - Data quality metrics

**Execution:**
```bash
export JAVA_HOME=/path/to/miniconda3/lib/jvm  # From conda setup
python src/01_data_profile.py
```

---

### **Stage 2: Data Cleaning & Normalization** → `data/silver/`
**Script:** `src/02_cleaning.py`
- **Input:** `data/raw/` CSV files
- **Process:**
  - **Type Casting:**
    - `subject_id`, `hadm_id`, `stay_id` → LongType (64-bit integers)
    - `itemid`, `icd_code` → StringType (preserves leading zeros)
    - Numeric values → IntegerType or DoubleType
    - Timestamps → `to_timestamp()` format
  
  - **Text Normalization:**
    - Disease names: lowercase → trim → remove special chars → collapse spaces
    - Lab item names: same normalization
    - Stored in `_norm` columns (e.g., `disease_name` → `disease_name_norm`)
  
  - **Deduplication:**
    - Per-table unique keys: `subject_id`, `hadm_id`, `itemid`, etc.
    - Removes duplicate records

- **Output:**
  - 12 Parquet folders in `data/silver/` (one per input table)
  - Each folder contains partitioned Parquet files (Spark format)

**Execution:**
```bash
python src/02_cleaning.py
```

---

### **Stage 3: Graph Table Construction** → `data/graph/`
**Script:** `src/03_build_graph_tables.py`
- **Input:** `data/silver/` (Parquet files from stage 2)
- **Process:**

#### **Node Types (6):**

1. **nodes_patient/** 
   - Columns: `id` (subject_id), `gender`, `anchor_age`, `anchor_year`, `anchor_year_group`
   - Count: 100 unique patients
   - Purpose: Patient demographic node in graph

2. **nodes_admission/**
   - Columns: `id` (hadm_id), `admission_type`, `admission_location`, `discharge_location`, `insurance`, `language`, `marital_status`, `race`, `hospital_expire_flag`
   - Count: 275 unique admissions
   - Purpose: Hospital visit node

3. **nodes_disease/**
   - Columns: `id` (concat version_icd_code), `name`, `name_norm`, `icd_code`, `icd_version`
   - Count: 200+ unique diseases
   - Purpose: ICD-10 diagnosis node
   - Key: Split `id` on `_` to get `icd_code`

4. **nodes_labtest/**
   - Columns: `id` (itemid), `name`, `name_norm`, `fluid`, `category`
   - Count: 100+ lab items
   - Purpose: Laboratory test item node

5. **nodes_icu_stay/**
   - Columns: `id` (stay_id), `first_careunit`, `last_careunit`, `los` (length of stay)
   - Count: 150 ICU admissions
   - Purpose: ICU stay node

6. **nodes_icu_item/**
   - Columns: `id` (itemid), `name`, `name_norm`, `label`, `abbreviation`, `category`, `unitname`, `param_type`
   - Count: 200+ ICU measurement items
   - Purpose: Vital sign / measurement item node

#### **Edge Types (9):**

1. **edges_patient_admission/**
   - Columns: `source` (patient subject_id), `target` (admission hadm_id)
   - Type: `HAS_ADMISSION`
   - Count: 100+ edges (patient→admission)

2. **edges_admission_disease/**
   - Columns: `source` (hadm_id), `target` (version_icd_code), `seq_num`
   - Type: `HAS_DISEASE`
   - Count: 400+ edges (admission→disease)
   - Note: `target` is concatenated; split on `_` in Cypher to extract icd_code

3. **edges_admission_labtest/**
   - Columns: `source` (hadm_id), `target` (itemid), `valuenum`, `valueuom`, `flag`, `charttime`
   - Type: `HAS_LABTEST`
   - Count: 600+ edges (admission→labtest)
   - Properties: value, unit, flag status, timestamp

4. **edges_admission_icu/**
   - Columns: `source` (hadm_id), `target` (stay_id)
   - Type: `HAS_ICU_STAY`
   - Count: 275 edges (admission→icustay)

5. **edges_icu_chart/**
   - Columns: `source` (stay_id), `target` (itemid), `value`, `valuenum`, `valueuom`, `warning`, `charttime`
   - Type: `HAS_CHART`
   - Count: 1000+ edges (icustay→item)
   - Properties: vital sign readings, warnings, timestamps

6. **edges_icu_datetime/**
   - Columns: `source` (stay_id), `target` (itemid), `datetime_value`, `valueuom`, `warning`, `charttime`
   - Type: `HAS_DATETIME`
   - Count: 500+ edges (icustay→item)
   - Properties: date-time measurements

7. **edges_icu_input/**
   - Columns: `source` (stay_id), `target` (itemid), `amount`, `amountuom`, `rate`, `rateuom`, `ordercategoryname`, `patientweight`, `starttime`, `endtime`
   - Type: `HAS_INPUT`
   - Count: 300+ edges (icustay→item)
   - Properties: medication/fluid amounts, rates, order category

8. **edges_icu_output/**
   - Columns: `source` (stay_id), `target` (itemid), `value`, `valueuom`, `charttime`
   - Type: `HAS_OUTPUT`
   - Count: 200+ edges (icustay→item)
   - Properties: output measurements

9. **edges_icu_item/**
   - Aggregated ICU item relationships
   - Properties: combined measurements across input/output/chart

- **Output:**
  - 15 CSV folders in `data/graph/` (6 nodes + 9 edges)
  - Each folder contains Spark partitioned CSV files

**Execution:**
```bash
python src/03_build_graph_tables.py
```

---

### **Stage 4: Graph Validation** → `report/graph/`
**Script:** `src/04_validate_graph.py`
- **Input:** `data/graph/` (CSV folders from stage 3)
- **Process:**
  - Foreign key integrity: Verify all source/target nodes exist
  - Uniqueness constraints: Check for duplicate node IDs
  - Relationship validation: Detect duplicate edges
  - Data quality: Null counts, missing values
  - Edge distribution analysis
  
- **Output:**
  - `report/graph/duplicate_edges/` - Duplicate relationships found
  - `report/graph/missing_source_nodes/` - Orphaned edges (missing source)
  - `report/graph/missing_target_nodes/` - Orphaned edges (missing target)
  - `report/graph/graph_validation_summary/` - Overall validation report

**Execution:**
```bash
python src/04_validate_graph.py
```

---

### **Stage 5: Neo4j Import** → Neo4j Database
**Script:** `src/06_graph_commented.cypher`
- **Input:** `neo4j/import/` (CSV files)
- **Process:**
  - **Create Constraints:** 6 unique constraints on key properties
  - **Load Nodes:** 6 LOAD CSV statements (Patient, Admission, Disease, LabTest, ICUStay, ICUItem)
  - **Load Edges:** 8-9 LOAD CSV statements
  - **Create Indexes:** For fast lookups
  - **Build Derived Graphs:**
    - Disease Co-occurrence Network: Disease ↔ Disease (CO_OCCURS_WITH)
    - Patient Similarity Graph: Patient ↔ Patient (SIMILAR_TO)
  
- **Output:**
  - Neo4j database with 6 node types, 8-9 relationship types
  - 2 derived relationship types for analytics

**Execution:**
```bash
# Option 1: Copy-paste into Neo4j Browser (http://100.101.211.84:7474)
# Option 2: Use cypher-shell (if installed)
cypher-shell -a neo4j://localhost:7687 -u neo4j -p YOUR_PASSWORD < src/06_graph_commented.cypher
```

---

## 🧬 Graph Schema

### **Node Types (6)**
```
Patient (100)
├── gender: string
├── anchor_age: integer
├── anchor_year: integer
└── anchor_year_group: string

Admission (275)
├── admission_type: string
├── admission_location: string
├── discharge_location: string
├── insurance: string
├── language: string
├── marital_status: string
├── race: string
└── hospital_expire_flag: integer

Disease (200+)
├── icd_code: string
├── icd_version: integer
├── name: string
└── name_norm: string

LabTest (100+)
├── name: string
├── name_norm: string
├── fluid: string
└── category: string

ICUStay (150)
├── first_careunit: string
├── last_careunit: string
└── los: double

ICUItem (200+)
├── name: string
├── name_norm: string
├── label: string
├── abbreviation: string
├── category: string
└── unitname: string
```

### **Relationship Types (8)**
```
Patient -[HAS_ADMISSION]-> Admission
Admission -[HAS_DISEASE]-> Disease {seq_num}
Admission -[HAS_ICU_STAY]-> ICUStay
Admission -[HAS_LABTEST]-> LabTest {valuenum, valueuom, flag, charttime}
ICUStay -[HAS_CHART]-> ICUItem {valuenum, valueuom, warning, charttime}
ICUStay -[HAS_DATETIME]-> ICUItem {datetime_value, valueuom, warning, charttime}
ICUStay -[HAS_INPUT]-> ICUItem {amount, amountuom, rate, rateuom, ordercategoryname, patientweight, starttime, endtime}
ICUStay -[HAS_OUTPUT]-> ICUItem {value, valueuom, charttime}
```

### **Derived Relationships (2)**
```
Disease -[CO_OCCURS_WITH]- Disease {weight: co-occurrence count}
Patient -[SIMILAR_TO]-> Patient {similarity: Jaccard coefficient, shared_diseases: count}
```

### **Constraints (6)**
```
CREATE CONSTRAINT unique_patient_id ON (p:Patient) ASSERT p.subject_id IS UNIQUE
CREATE CONSTRAINT unique_admission_id ON (a:Admission) ASSERT a.hadm_id IS UNIQUE
CREATE CONSTRAINT unique_disease_id ON (d:Disease) ASSERT d.icd_code IS UNIQUE
CREATE CONSTRAINT unique_labtest_id ON (lt:LabTest) ASSERT lt.itemid IS UNIQUE
CREATE CONSTRAINT unique_icu_stay_id ON (is:ICUStay) ASSERT is.stay_id IS UNIQUE
CREATE CONSTRAINT unique_icu_item_id ON (ii:ICUItem) ASSERT ii.itemid IS UNIQUE
```

---

## 📊 Notebooks & Analytics

### **Jupyter Notebook**
**File:** `notebooks/mimic_data.ipynb`
- Data exploration & visualization
- Sample queries and analysis
- Graph statistics and distribution plots

---

## 🚀 Execution Workflow

### **Prerequisite Setup**
```bash
cd ./mimic-iv

# Set JAVA_HOME for Spark
export JAVA_HOME=/home/jkl0909/miniconda3/lib/jvm

# Activate conda environment (if applicable)
conda activate fords  # Or your Python environment
```

### **Full Pipeline Execution**

**Step 1: Profile Raw Data**
```bash
python src/01_data_profile.py
# Outputs: report/profiling/table_summary.csv, null_statistics.csv
```

**Step 2: Clean & Normalize Data**
```bash
python src/02_cleaning.py
# Outputs: data/silver/ (12 Parquet folders)
```

**Step 3: Build Graph Tables**
```bash
python src/03_build_graph_tables.py
# Outputs: data/graph/ (6 node + 9 edge folders)
```

**Step 4: Validate Graph**
```bash
python src/04_validate_graph.py
# Outputs: report/graph/ (validation reports)
```
**Step 5: Prepare CSV Data**
```bash
python src/05_prepare_import_csv.py
# Outputs: neo4j/import (csv files)
```
**Step 5: Import to Neo4j**

Option A - Browser UI:
1. Open http://localhost:7474
2. Login: `neo4j` / `YOUR_PASSWORD` (configure in `.env`)
3. Open `src/06_graph_commented.cypher` in your editor
4. Copy entire content into Neo4j Browser
5. Execute all statements

Option B - Command Line:
```bash
cat src/06_graph_commented.cypher | cypher-shell -u neo4j -p YOUR_PASSWORD
```

### **Verification Queries**

```cypher
# Check node counts
MATCH (n) RETURN labels(n)[0] as node_type, count(*) as count ORDER BY count DESC;

# Check edge counts
MATCH ()-[r]->() RETURN type(r) as relationship_type, count(*) as count ORDER BY count DESC;

# Sample patient path (5 hops)
MATCH (p:Patient {subject_id: 10000032})-[*1..5]-(n) 
RETURN p, n LIMIT 50;

# Disease co-occurrence (top 10)
MATCH (d1:Disease)-[r:CO_OCCURS_WITH]-(d2:Disease)
WHERE d1.icd_code < d2.icd_code
RETURN d1.long_title, d2.long_title, r.weight
ORDER BY r.weight DESC LIMIT 10;

# Patient similarity (top 10 similar to patient 10000032)
MATCH (p:Patient {subject_id: 10000032})-[r:SIMILAR_TO]-(similar:Patient)
RETURN similar.subject_id, r.similarity, r.shared_diseases
ORDER BY r.similarity DESC LIMIT 10;
```

---

## 📋 File Summary Table

| File | Input | Output | Purpose |
|------|-------|--------|---------|
| `01_data_profile.py` | `data/raw/` CSVs | `report/profiling/` | Data quality analysis |
| `02_cleaning.py` | `data/raw/` CSVs | `data/silver/` (Parquet) | Type-casting, normalization, dedup |
| `03_build_graph_tables.py` | `data/silver/` Parquet | `data/graph/` (CSV × 15) | Node/edge extraction |
| `04_validate_graph.py` | `data/graph/` CSVs | `report/graph/` | Integrity checks |
| `06_graph_commented.cypher` | `neo4j/import/` CSVs | Neo4j database | Import + create analytics |
| `08_graph_analytics.cypher` | Neo4j database | Query results | Advanced Cypher analytics |

---

## 🔗 Key Resources

- **Neo4j Server:** http://localhost:7474
- **Bolt Protocol:** neo4j://localhost:7687
- **Credentials:** Configure in your `.env` file
- **Docker Container:** `neo4j-mimic` (neo4j:5 with GDS plugin)

---

## 📈 Data Volume Summary

| Component | Count | Notes |
|-----------|-------|-------|
| Patients | 100 | Demographics: gender, age, year |
| Admissions | 275 | Hospital visits with insurance, race, language |
| Diseases (ICD-10) | 200+ | Unique diagnoses with normalized names |
| Lab Tests | 100+ | Items with fluid, category |
| ICU Stays | 150 | ICU admissions with length of stay |
| ICU Items | 200+ | Vital signs, measurements |
| **Total Nodes** | **~850** | |
| Admission-Disease Edges | 400+ | With sequence numbers |
| Lab Test Edges | 600+ | With measurements |
| ICU Measurement Edges | 2000+ | Chart, datetime, input, output |
| **Total Edges** | **~3200+** | |

---

## ✅ Next Steps

1. **Execute full pipeline:** Run all 4 scripts in order (01→04)
2. **Import to Neo4j:** Execute `06_graph_commented.cypher` in Browser
3. **Verify import:** Run verification queries above
4. **Run analytics:** Execute Cypher or Python queries for insights
5. **Update README:** Create comprehensive readme.md for documentation

---

*This structure enables a complete clinical knowledge graph for disease co-occurrence analysis, patient similarity, and diagnostic pathway exploration.*
