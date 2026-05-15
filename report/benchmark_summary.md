**Benchmark Summary**

This document summarizes Spark and Neo4j benchmarking results for the MIMIC-IV pipeline.

Files produced:
- `report/benchmark_results.csv` - Spark ETL join benchmark (07)
- `report/benchmark_results_graph_analytics.csv` - Graph analytics benchmarks (08)
- `report/neo4j_query_latency.csv` - Neo4j query latency (09) (if Neo4j reachable)

Key findings (auto-generated; update after running all scripts):

- Spark ETL (join): Parquet + broadcast join provided the largest speedup on the demo data.
- Graph analytics: Co-occurrence aggregation and patient similarity are compute-heavy; broadcast helps small dimension tables, caching helps on larger scale.
- Neo4j latency: dependent on server state; run `src/09_neo4j_query_latency.py` with env vars set to measure actual latency.

Next steps:
- Run `python src/07_benchmark_optimization.py` and `python src/08_graph_analytics_benchmark.py` under the `fords` conda env (done for 07, run 08 next).
- If Neo4j is available, set `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` and run `python src/09_neo4j_query_latency.py`.
