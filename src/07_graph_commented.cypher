// =====================================================================
// MIMIC-IV KNOWLEDGE GRAPH CONSTRUCTION - Neo4j Setup & Data Import
// =====================================================================
// Purpose: Build a medical knowledge graph from MIMIC-IV clinical data
// Structure:
//   - 6 Node Types: Patient, Admission, Disease, LabTest, ICUStay, ICUItem
//   - 8 Relationship Types: HAS_ADMISSION, HAS_DISEASE, HAS_LABTEST, HAS_ICU_STAY, HAS_CHART, HAS_DATETIME, HAS_INPUT, HAS_OUTPUT
//   - 2 Derived Networks: CO_OCCURS_WITH (disease pairs), SIMILAR_TO (patient pairs)
// =====================================================================

// =====================================================================
// PART 0: DOCKER SETUP - Start Neo4j Container
// =====================================================================
// Run this command FIRST in your terminal (from repo root):
// - Stops and removes old container (if exists)
// - Starts Neo4j 5 with Graph Data Science (GDS) enabled
// - Mounts volumes for data persistence and CSV import
// - Enables GDS procedures for PageRank, Community Detection, etc.
// - Login: neo4j / abc123456

docker rm -f neo4j-mimic 2>/dev/null || true
docker run \
  --name neo4j-mimic \
  -p 7474:7474 \
  -p 7687:7687 \
  -v ./neo4j/data:/data \
  -v ./neo4j/import:/import \
  -e NEO4J_AUTH=neo4j/abc123456 \
  -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  -e NEO4J_PLUGINS='["graph-data-science"]' \
  -e NEO4J_dbms_security_procedures_unrestricted=gds.* \
  -e NEO4J_dbms_security_procedures_allowlist=gds.* \
  --restart unless-stopped \
  -d neo4j:5

// AFTER starting container, run the Cypher queries below in Neo4j Browser

// =====================================================================
// PART 1: CREATE UNIQUENESS CONSTRAINTS
// =====================================================================
// Constraints prevent duplicate nodes and ensure data integrity
// Each entity type has one primary unique identifier

// Patient node: subject_id must be unique (patient ID from MIMIC)
CREATE CONSTRAINT patient_id IF NOT EXISTS
FOR (p:Patient)
REQUIRE p.subject_id IS UNIQUE;

// Admission node: hadm_id must be unique (hospital admission ID)
CREATE CONSTRAINT admission_id IF NOT EXISTS
FOR (a:Admission)
REQUIRE a.hadm_id IS UNIQUE;

// ICU Stay node: stay_id must be unique (ICU stay ID)
CREATE CONSTRAINT icu_stay_id IF NOT EXISTS
FOR (i:ICUStay)
REQUIRE i.stay_id IS UNIQUE;

// Disease node: icd_code + icd_version must be unique (combination of ICD code and version)
CREATE CONSTRAINT disease_id IF NOT EXISTS
FOR (d:Disease)
REQUIRE (d.icd_code, d.icd_version) IS UNIQUE;

// Lab Test node: itemid must be unique (lab test item ID)
CREATE CONSTRAINT labtest_id IF NOT EXISTS
FOR (l:LabTest)
REQUIRE l.itemid IS UNIQUE;

// ICU Item node: itemid must be unique (ICU measurement item ID)
CREATE CONSTRAINT icuitem_id IF NOT EXISTS
FOR (i:ICUItem)
REQUIRE i.itemid IS UNIQUE;

CREATE CONSTRAINT external_disease_uri
IF NOT EXISTS
FOR (d:ExternalDisease)
REQUIRE d.uri IS UNIQUE;

CREATE CONSTRAINT symptom_name
IF NOT EXISTS
FOR (s:Symptom)
REQUIRE s.name IS UNIQUE;

CREATE CONSTRAINT treatment_name
IF NOT EXISTS
FOR (t:Treatment)
REQUIRE t.name IS UNIQUE;

CREATE CONSTRAINT category_name
IF NOT EXISTS
FOR (c:DiseaseCategory)
REQUIRE c.name IS UNIQUE;


// =====================================================================
// PART 2: LOAD NODE DATA FROM CSV FILES
// =====================================================================
// Load entity data from ./neo4j/import/*.csv
// Each LOAD CSV reads headers and creates nodes with properties

// Load Admission nodes (hospital admissions)
// Properties: hadm_id (unique), admission_type (e.g., 'EMERGENCY', 'ELECTIVE')
LOAD CSV WITH HEADERS FROM 'file:///nodes_admission.csv' AS row
MERGE (a:Admission {
    hadm_id: toInteger(row.id)
})
SET
    a.admission_type = row.admission_type;

// Load Patient nodes (individual patients)
// Properties: subject_id (unique), gender, anchor_age (age at data anchor point)
LOAD CSV WITH HEADERS FROM 'file:///nodes_patient.csv' AS row
MERGE (p:Patient {
    subject_id: toInteger(row.id)
})
SET
    p.gender = row.gender,
    p.anchor_age = toInteger(row.anchor_age);

// Load Disease nodes (diagnoses from ICD-9 and ICD-10)
// Properties: icd_code (unique - e.g., 'E11.9' for Type 2 Diabetes or '064' for ICD-9), 
//             icd_version (9 or 10), long_title (disease name)
LOAD CSV WITH HEADERS FROM 'file:///nodes_disease.csv' AS row
MERGE (d:Disease {
    icd_code: row.icd_code,
    icd_version: row.icd_version
})
SET
    d.long_title = row.name;

// Load ICU Stay nodes (patient ICU admissions)
// Properties: stay_id (unique), first_careunit, last_careunit, length_of_stay
LOAD CSV WITH HEADERS FROM 'file:///nodes_icu_stay.csv' AS row
MERGE (i:ICUStay {
    stay_id: toInteger(row.id)
});

// Load Lab Test nodes (laboratory test types)
// Properties: itemid (unique - test ID), label (test name)
LOAD CSV WITH HEADERS FROM 'file:///nodes_labtest.csv' AS row
MERGE (l:LabTest {
    itemid: toInteger(row.id)
})
SET
    l.label = row.name;

// Load ICU Item nodes (ICU monitoring parameters)
// Properties: itemid (unique - parameter ID), label (parameter name)
LOAD CSV WITH HEADERS FROM 'file:///nodes_icu_item.csv' AS row
MERGE (i:ICUItem {
    itemid: toInteger(row.id)
})
SET
    i.label = row.name;

LOAD CSV WITH HEADERS
FROM 'file:///dbpedia.csv'
AS row

MERGE (d:ExternalDisease {
    uri: row.disease
})

SET d.name = row.diseaseName,
    d.icd9 = row.icd9,
    d.icd10 = row.icd10;
// =====================================================================
// PART 3: LOAD RELATIONSHIP DATA (EDGES) FROM CSV FILES
// =====================================================================
// Create connections between nodes
// Each LOAD CSV reads edges and creates relationships with attributes

// Relationship 1: Patient -> Admission (patient HAS_ADMISSION admission)
// Connects patients to their hospital admissions
LOAD CSV WITH HEADERS FROM 'file:///edges_patient_admission.csv' AS row
MATCH (p:Patient {subject_id: toInteger(row.source)})
MATCH (a:Admission {hadm_id: toInteger(row.target)})
MERGE (p)-[:HAS_ADMISSION]->(a);

// Relationship 2: Admission -> Disease (admission HAS_DISEASE disease)
// Connects admissions to diagnosed diseases
// Note: target format is "icd_version_code", split by '_' to extract icd_code and icd_version
LOAD CSV WITH HEADERS FROM 'file:///edges_admission_disease.csv' AS row
WITH row, split(row.target, '_') AS parts
MATCH (a:Admission {hadm_id: toInteger(row.source)})
MATCH (d:Disease {icd_code: parts[1], icd_version: parts[0]})
MERGE (a)-[r:HAS_DISEASE]->(d)
SET r.seq_num = CASE WHEN row.seq_num IS NOT NULL AND row.seq_num <> '' THEN toInteger(row.seq_num) ELSE NULL END;

// Relationship 3: Admission -> ICU Stay (admission HAS_ICU_STAY icu_stay)
// Connects admissions to their associated ICU stays
LOAD CSV WITH HEADERS FROM 'file:///edges_admission_icu.csv' AS row
MATCH (a:Admission {hadm_id: toInteger(row.source)})
MATCH (s:ICUStay {stay_id: toInteger(row.target)})
MERGE (a)-[:HAS_ICU_STAY]->(s);

// Relationship 4: Admission -> Lab Test (admission HAS_LABTEST lab_test)
// Connects admissions to lab tests performed during admission
// Attributes: valuenum (numeric result), valueuom (unit of measurement), flag (abnormal/normal), charttime (timestamp)
LOAD CSV WITH HEADERS FROM 'file:///edges_admission_labtest.csv' AS row
MATCH (a:Admission {hadm_id: toInteger(row.source)})
MATCH (l:LabTest {itemid: toInteger(row.target)})
CREATE (a)-[r:HAS_LABTEST]->(l)
SET r.valuenum =
        CASE
            WHEN row.valuenum IS NOT NULL AND row.valuenum <> ''
            THEN toFloat(row.valuenum)
            ELSE NULL
        END,
    r.valueuom = row.valueuom,
    r.flag = row.flag,
    r.charttime = row.charttime;

// Relationship 5: ICU Stay -> ICU Item (icu_stay HAS_CHART icu_item)
// Charted measurements (vitals, etc.) during ICU stay
// Attributes: value (string value), valuenum (numeric), valueuom (unit), warning (flag), charttime (timestamp)
LOAD CSV WITH HEADERS FROM 'file:///edges_icu_chart.csv' AS row
MATCH (s:ICUStay {stay_id: toInteger(row.source)})
MATCH (i:ICUItem {itemid: toInteger(row.target)})
CREATE (s)-[r:HAS_CHART]->(i)
SET r.value = row.value,
    r.valuenum =
        CASE
            WHEN row.valuenum IS NOT NULL AND row.valuenum <> ''
            THEN toFloat(row.valuenum)
            ELSE NULL
        END,
    r.valueuom = row.valueuom,
    r.warning =
        CASE
            WHEN row.warning IS NOT NULL AND row.warning <> ''
            THEN toInteger(row.warning)
            ELSE NULL
        END,
    r.charttime = row.charttime;

// Relationship 6: ICU Stay -> ICU Item (icu_stay HAS_DATETIME icu_item)
// Date-time values (e.g., birth date recorded during ICU stay)
LOAD CSV WITH HEADERS FROM 'file:///edges_icu_datetime.csv' AS row
MATCH (s:ICUStay {stay_id: toInteger(row.source)})
MATCH (i:ICUItem {itemid: toInteger(row.target)})
CREATE (s)-[r:HAS_DATETIME]->(i)
SET r.datetime_value = row.datetime_value,
    r.valueuom = row.valueuom,
    r.warning =
        CASE
            WHEN row.warning IS NOT NULL AND row.warning <> ''
            THEN toInteger(row.warning)
            ELSE NULL
        END,
    r.charttime = row.charttime;

// Relationship 7: ICU Stay -> ICU Item (icu_stay HAS_INPUT icu_item)
// Input records (medications, IV fluids, etc.) administered during ICU stay
// Attributes: amount, rate, weight, start/end times
LOAD CSV WITH HEADERS FROM 'file:///edges_icu_input.csv' AS row
MATCH (s:ICUStay {stay_id: toInteger(row.source)})
MATCH (i:ICUItem {itemid: toInteger(row.target)})
CREATE (s)-[r:HAS_INPUT]->(i)
SET r.amount =
        CASE
            WHEN row.amount IS NOT NULL AND row.amount <> ''
            THEN toFloat(row.amount)
            ELSE NULL
        END,
    r.amountuom = row.amountuom,
    r.rate =
        CASE
            WHEN row.rate IS NOT NULL AND row.rate <> ''
            THEN toFloat(row.rate)
            ELSE NULL
        END,
    r.rateuom = row.rateuom,
    r.ordercategoryname = row.ordercategoryname,
    r.patientweight =
        CASE
            WHEN row.patientweight IS NOT NULL AND row.patientweight <> ''
            THEN toFloat(row.patientweight)
            ELSE NULL
        END,
    r.starttime = row.starttime,
    r.endtime = row.endtime;

// Relationship 8: ICU Stay -> ICU Item (icu_stay HAS_OUTPUT icu_item)
// Output records (urine output, etc.) during ICU stay
LOAD CSV WITH HEADERS FROM 'file:///edges_icu_output.csv' AS row
MATCH (s:ICUStay {stay_id: toInteger(row.source)})
MATCH (i:ICUItem {itemid: toInteger(row.target)})
CREATE (s)-[r:HAS_OUTPUT]->(i)
SET r.value =
        CASE
            WHEN row.value IS NOT NULL AND row.value <> ''
            THEN toFloat(row.value)
            ELSE NULL
        END,
    r.valueuom = row.valueuom,
    r.charttime = row.charttime;

//9) disease -> symptom
LOAD CSV WITH HEADERS
FROM 'file:///dbpedia.csv'
AS row

WITH row
WHERE row.symptomName IS NOT NULL 
  AND trim(row.symptomName) <> ''

MATCH (d:ExternalDisease {uri: row.disease})

MERGE (s:Symptom {name: trim(row.symptomName)})
MERGE (d)-[:HAS_SYMPTOM]->(s);

//10)/ disease -> treatment
LOAD CSV WITH HEADERS
FROM 'file:///dbpedia.csv'
AS row

WITH row
WHERE row.treatmentName IS NOT NULL 
  AND trim(row.treatmentName) <> ''

MATCH (d:ExternalDisease {uri: row.disease})

MERGE (t:Treatment {name: trim(row.treatmentName)})
MERGE (d)-[:HAS_TREATMENT]->(t);

//11)Disease-> category
LOAD CSV WITH HEADERS
FROM 'file:///dbpedia.csv'
AS row
// =====================================================================
// PART 3.5: INTEGRATE MIMIC DISEASES WITH DBPEDIA EXTERNAL DISEASES
// =====================================================================
// Connect MIMIC-IV Disease nodes with DBpedia External Diseases
// This creates a SAME_AS relationship when a MIMIC disease matches a DBpedia disease
// by ICD code and version, with metadata about the matching quality

// Relationship: MIMIC Disease -> DBpedia External Disease (SAME_AS)
// Attributes: score (matching confidence 0-1), method (matching algorithm), match_status
LOAD CSV WITH HEADERS
FROM 'file:///disease_dbpedia_matches.csv'
AS row

MATCH (m:Disease {
    icd_code: row.mimic_icd_code,
})

MATCH (e:ExternalDisease {
    uri: row.dbpedia_uri
})

MERGE (m)-[r:SAME_AS]->(e)

SET r.score = toFloat(row.score),
    r.method = row.method,
    r.match_status = row.match_status;

// =====================================================================
// PART 4: DISEASE CO-OCCURRENCE NETWORK
// =====================================================================
// Create derived relationships between diseases that appear together
// Use case: Identify disease syndromes and comorbidity patterns

// Query 4.1: Create CO_OCCURS_WITH edges
// Finds disease pairs appearing in the same admission (co-occurrence >= 2)
// This represents diseases that frequently appear together in patients
MATCH (a:Admission)-[:HAS_DISEASE]->(d1:Disease)
MATCH (a)-[:HAS_DISEASE]->(d2:Disease)
WHERE d1.long_title < d2.long_title
WITH d1, d2, count(DISTINCT a) AS co_count
WHERE co_count >= 2
MERGE (d1)-[r:CO_OCCURS_WITH]-(d2)
SET r.weight = co_count;

// Query 4.2: Find related diseases
// Returns disease pairs with their co-occurrence frequency
// Example: Diabetes (E11.9) co-occurs with Hypertension (I10) 25 times
MATCH (d1:Disease)-[r:CO_OCCURS_WITH]-(d2:Disease)
WHERE d1.icd_code < d2.icd_code
RETURN 
  d1.long_title AS disease_1,
  d2.long_title AS disease_2,
  r.weight AS co_occurrence
ORDER BY co_occurrence DESC
LIMIT 30;

// =====================================================================
// PART 5: PATIENT SIMILARITY GRAPH
// =====================================================================
// Find patients with similar disease profiles using Jaccard similarity
// Use case: Identify similar patient cases for treatment recommendations

// Query 5.1: Create SIMILAR_TO edges
// Calculates Jaccard similarity: shared_diseases / (total_distinct_diseases - shared)
// Only creates edges where similarity > 0
MATCH (p1:Patient)-[:HAS_ADMISSION]-(a1:Admission)-[:HAS_DISEASE]->(d:Disease)<-[:HAS_DISEASE]-(:Admission)-[:HAS_ADMISSION]-(p2:Patient)
WHERE p1.subject_id < p2.subject_id
WITH p1, p2, collect(DISTINCT d.icd_code) as shared_diseases
MATCH (p1)-[:HAS_ADMISSION]->(:Admission)-[:HAS_DISEASE]->(d1:Disease)
WITH p1, p2, shared_diseases, collect(DISTINCT d1.icd_code) as p1_diseases
MATCH (p2)-[:HAS_ADMISSION]->(:Admission)-[:HAS_DISEASE]->(d2:Disease)
WITH p1, p2, shared_diseases, p1_diseases, collect(DISTINCT d2.icd_code) as p2_diseases,
     size(shared_diseases) as shared_count
WITH p1, p2, shared_diseases, p1_diseases, p2_diseases, shared_count,
     toFloat(shared_count) / (size(p1_diseases) + size(p2_diseases) - shared_count) as similarity
WHERE similarity > 0
MERGE (p1)-[r:SIMILAR_TO]->(p2)
SET r.similarity = similarity,
    r.shared_diseases = shared_count;

// Query 5.2: List top patient pairs by similarity
// Shows which patients have the most similar disease profiles
MATCH (p1:Patient)-[r:SIMILAR_TO]-(p2:Patient)
WHERE p1.subject_id < p2.subject_id
RETURN
p1.subject_id AS patient_1,
p2.subject_id AS patient_2,
r.shared_diseases AS common_diseases,
r.similarity AS similarity
ORDER BY similarity DESC
LIMIT 100;

// Query 5.3: Find similar patients to a specific patient
// Example: Find patients similar to patient 10000032 for treatment guidance
MATCH (p:Patient {subject_id: 10000032})-[r:SIMILAR_TO]-(similar:Patient)
RETURN similar.subject_id, r.similarity, r.shared_diseases
ORDER BY r.similarity DESC
LIMIT 10;

// Query 5.4: Visualize patient similarity network
// Show graph structure of similar patients (with similarity threshold)
MATCH (p1:Patient)-[r:SIMILAR_TO]-(p2:Patient)
WHERE r.similarity >= 0.2
RETURN p1, r, p2
LIMIT 200;

// =====================================================================
// PART 6: DIAGNOSTIC PATHWAYS (Disease → Lab Test Connections)
// =====================================================================
// Find relationships between diseases and lab tests
// Use case: Identify which lab tests are typically ordered for specific diagnoses

// Query 6.1: Shortest path from disease to lab test
// Finds diagnostic pathways (e.g., Diabetes → Glucose lab test)
MATCH (d:Disease)
MATCH (l:LabTest)
WHERE toLower(coalesce(l.label,'')) CONTAINS 'gluc'
WITH d,l
MATCH path = shortestPath((d)-[*1..4]-(l))
RETURN path, length(path) AS path_length
LIMIT 100;

// Query 6.2: Disease-Lab Test associations
// Lists which lab tests are ordered for specific diseases
MATCH (d:Disease)<-[:HAS_DISEASE]-(a:Admission)-[:HAS_LABTEST]->(l:LabTest)
RETURN d.icd_code, d.long_title as disease,
l.itemid, l.label as labtest,
count(*) as frequency
ORDER BY frequency DESC;

// =====================================================================
// PART 7: VERIFICATION & ANALYSIS QUERIES
// =====================================================================

// Query 7.1: Count total nodes in graph
// Verify data has been loaded correctly
MATCH (n)
RETURN count(n);

// Query 7.2: Visualize graph structure (relationships & nodes)
// Sample of nodes and edges in the graph
MATCH (n)-[r]->(m)
RETURN n,r,m
LIMIT 500;

// Query 7.3: Explore connections from a specific patient
// Shows 4-hop paths from patient 10000032 (diagnoses, tests, ICU data)
MATCH path = (p:Patient {subject_id: 10000032})-[*1..4]-(n)
RETURN path
LIMIT 500;

// =====================================================================
// NEXT STEPS - Run these when ready:
// =====================================================================
// 1) For community detection on diseases:
//    CALL gds.graph.project('disease_network', 'Disease', 'CO_OCCURS_WITH');
//    CALL gds.louvain.stream('disease_network') YIELD nodeId, communityId
//    RETURN gds.util.asNode(nodeId).icd_code, communityId;
//
// 2) For PageRank (disease importance):
//    CALL gds.graph.project('disease_pr', 'Disease', 'CO_OCCURS_WITH');
//    CALL gds.pagerank.stream('disease_pr') YIELD nodeId, score
//    RETURN gds.util.asNode(nodeId).icd_code, score ORDER BY score DESC;
//
// 3) For patient cohort detection:
//    CALL gds.graph.project('patient_sim', 'Patient', 'SIMILAR_TO');
//    CALL gds.louvain.stream('patient_sim') YIELD nodeId, communityId
//    RETURN gds.util.asNode(nodeId).subject_id, communityId;
// =====================================================================
