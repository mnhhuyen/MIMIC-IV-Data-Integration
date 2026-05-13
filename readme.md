# MIMIC-IV Clinical Knowledge Graph

A complete data pipeline for building a clinical knowledge graph from the MIMIC-IV dataset, enabling disease co-occurrence analysis, patient similarity computation, and diagnostic pathway exploration.

## 🎯 Project Overview

This project transforms raw MIMIC-IV clinical data into a Neo4j knowledge graph with 6 node types, 9+ relationship types, and derived analytics relationships. The pipeline is organized into 4 distinct processing stages:

1. **Data Profiling** - Analyze raw data quality (01_data_profile.py)
2. **Data Cleaning** - Normalize, type-cast, and deduplicate (02_cleaning.py)
3. **Graph Building** - Extract nodes and edges (03_build_graph_tables.py)
4. **Validation** - Quality checks and integrity verification (04_validate_graph.py)
5. **Neo4j Import** - Load into graph database and create analytics (06_graph_commented.cypher)

## 📊 What's Included

### Data Layers
- **Raw (Layer 0):** 12 MIMIC-IV CSV tables (~100 patients, ~275 admissions)
- **Silver (Layer 1):** 12 cleaned & normalized Parquet folders
- **Graph (Layer 2):** 15 node/edge CSV folders ready for Neo4j

### Graph Structure
```
Patient (100) → Admission (275) → Disease (200+)
                              ↓
                           LabTest (100+)
                              ↓
                           ICUStay (150) → ICUItem (200+)
```

**Relationships:**
- 8 Primary: HAS_ADMISSION, HAS_DISEASE, HAS_ICU_STAY, HAS_LABTEST, HAS_CHART, HAS_DATETIME, HAS_INPUT, HAS_OUTPUT
- 2 Derived: CO_OCCURS_WITH (disease pairs), SIMILAR_TO (patient pairs)

## 🚀 Quick Start

### Prerequisites

**1. Install Java (required for Spark)**
```bash
conda install openjdk
java -version
export JAVA_HOME=/path/to/miniconda3/lib/jvm
```

**2. Configure Credentials**
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and set your Neo4j credentials
# IMPORTANT: .env is in .gitignore - never commit credentials!
nano .env
```
### Run Full Pipeline
```bash
cd /path/to/mimic-iv

# Step 1: Profile raw data
python src/01_data_profile.py
# Output: report/profiling/table_summary.csv, null_statistics.csv

# Step 2: Clean & normalize
python src/02_cleaning.py
# Output: data/silver/ (12 Parquet folders)

# Step 3: Build graph tables
python src/03_build_graph_tables.py
# Output: data/graph/ (15 CSV folders)

# Step 4: Validate
python src/04_validate_graph.py
# Output: report/graph/ (validation reports)
```

### Import to Neo4j

**Option A: Browser UI (Recommended)**
1. Open http://localhost:7474 (or your Neo4j server address)
2. Login: neo4j / YOUR_PASSWORD (from your `.env` configuration)
3. Copy entire content of `src/06_graph_commented.cypher`
4. Paste into Browser query editor
5. Execute

**Option B: Command Line**
```bash
cypher-shell -u neo4j -p YOUR_PASSWORD < src/06_graph_commented.cypher
```

## 📚 Documentation

- **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** - Complete folder breakdown with all file descriptions
- **[src/recommend_from_copilot/ANALYTICS_GUIDE.md](src/recommend_from_copilot/ANALYTICS_GUIDE.md)** - Implementation guide
- **[src/recommend_from_copilot/QUICK_REFERENCE.md](src/recommend_from_copilot/QUICK_REFERENCE.md)** - Quick reference

## 🔗 Neo4j Access

| Property | Value |
|----------|-------|
| **Browser** | http://localhost:7474 |
| **Bolt** | neo4j://localhost:7687 |
| **Username** | neo4j 
| **Password** | [Configure in .env file] |
| **Docker Container** | neo4j-mimic |

## 📁 File Structure

```
mimic-iv/
├── data/
│   ├── raw/              # Original MIMIC-IV CSVs
│   ├── silver/           # Cleaned Parquet files
│   └── graph/            # Graph CSV files (For example 6 nodes + 9 edges)
├── neo4j/
│   ├── data/             # Neo4j database volume
│   └── import/           # CSV staging area
├── src/
│   ├── 01_data_profile.py
│   ├── 02_cleaning.py
│   ├── 03_build_graph_tables.py
│   ├── 04_validate_graph.py
│   ├── 06_graph_commented.cypher
│   └── recommend_from_copilot/
├── notebooks/
│   └── mimic_data.ipynb
├── report/
│   ├── profiling/
│   └── graph/
├── PROJECT_STRUCTURE.md  # Detailed documentation
└── readme.md             # This file
```

## 🔍 Key Features

- **Automated ETL Pipeline** - 4-stage data transformation with Apache Spark
- **Text Normalization** - Disease and lab item names normalized for consistency
- **Graph Validation** - Comprehensive integrity checks and quality reports
- **Neo4j Integration** - Fully commented Cypher scripts for ease of understanding
- **Derived Analytics** - Automatic computation of co-occurrence and similarity relationships
- **Jupyter Notebooks** - Interactive data exploration and visualization

## 📊 Data Quality

Profiling reports available in `report/profiling/`:
- **table_summary.csv** - Row/column counts per table
- **null_statistics.csv** - Null value percentages by column

Validation reports available in `report/graph/`:
- Duplicate edge detection
- Missing node validation
- Relationship integrity checks

## 🛠️ Advanced Analytics

### Cypher Queries
Advanced analytics queries available in `src/recommend_from_copilot/08_graph_analytics.cypher`:
- PageRank on disease networks
- Community detection among patients
- Shortest path diagnostic chains
- Centrality measures

## 📈 Data Volume

| Component | Count |
|-----------|-------|
| Patients | 100 |
| Hospital Admissions | 275 |
| Diagnoses (unique) | 200+ |
| Lab Items (unique) | 100+ |
| ICU Stays | 150 |
| ICU Measurement Items | 200+ |
| **Total Nodes** | ~850 |
| **Total Relationships** | ~3,200+ |

## 🐳 Docker Setup (Neo4j)

The Neo4j container is pre-configured with:
- **Image:** neo4j:5 with Graph Data Science (GDS) plugin
- **Volume Mounts:**
  - `./neo4j/data:/data` - Persisted database
  - `./neo4j/import:/import` - CSV import area
- **Ports:** 7474 (HTTP), 7687 (Bolt)
- **Authentication:** neo4j / [your-password]

Start container:
```bash
docker run \
  --name neo4j-mimic \
  -p 7474:7474 -p 7687:7687 \
  -v ./neo4j/data:/data \
  -v ./neo4j/import:/import \
  -e NEO4J_AUTH=neo4j/your-secure-password \
  -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  -e NEO4J_PLUGINS='["graph-data-science"]' \
  -e NEO4J_dbms_security_procedures_unrestricted=gds.* \
  --restart unless-stopped \
  -d neo4j:5
```

## 🔧 Troubleshooting

### Spark/Java Issues
```bash
# Ensure JAVA_HOME is set correctly
export JAVA_HOME=/home/jkl0909/miniconda3/lib/jvm
# Or conda install
conda install openjdk
```

### Neo4j Connection Issues
```bash
# Check if Neo4j is running
docker ps | grep neo4j-mimic

# Check logs
docker logs neo4j-mimic

# Verify ports are listening
netstat -tulpn | grep -E '7474|7687'
```

### CSV Import Issues
- Ensure CSVs are in `neo4j/import/` directory
- Check file permissions
- Verify CSV format (proper quotes, escaping)

## 📝 License

MIMIC-IV data is available under the PhysioNet Credentialed Health Data License.

