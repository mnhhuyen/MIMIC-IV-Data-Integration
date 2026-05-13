#!/usr/bin/env python3
"""
prepare_import.py

1) Convert Spark/Parquet partitions under `./data/graph/<name>/part-*` to CSV files
   written to `./neo4j/import/<name>.csv`

2) Clean all CSV files in `./neo4j/import` by re-writing them with
   `quoting=csv.QUOTE_ALL` and `escapechar='\\'` to make them safe for
   Neo4j `LOAD CSV`.

Usage: run from repo root:
    python src/prepare_import.py

"""

import os
import argparse
import csv
import pandas as pd


def convert_partitions(source_dir: str, target_dir: str) -> None:
    os.makedirs(target_dir, exist_ok=True)

    for folder_name in os.listdir(source_dir):
        folder_path = os.path.join(source_dir, folder_name)

        if not os.path.isdir(folder_path):
            continue

        print(f"Processing: {folder_name}")

        # find file part-
        part_files = [f for f in os.listdir(folder_path) if f.startswith("part-")]

        if not part_files:
            print(f"  -> No part file found in {folder_name}")
            continue

        part_path = os.path.join(folder_path, part_files[0])
        output_csv = os.path.join(target_dir, f"{folder_name}.csv")

        try:
            df = pd.read_parquet(part_path)
            df.to_csv(output_csv, index=False)
            print(f"  -> Saved parquet -> csv: {output_csv}")

        except Exception:
            try:
                df = pd.read_csv(part_path)
                df.to_csv(output_csv, index=False)
                print(f"  -> Saved csv partition -> csv: {output_csv}")

            except Exception as e:
                print(f"  -> Failed to convert {part_path}: {e}")


def clean_csvs(import_dir: str) -> None:
    for fname in os.listdir(import_dir):
        if not fname.endswith('.csv'):
            continue

        path = os.path.join(import_dir, fname)
        print(f"Cleaning {fname}")

        try:
            # read with python engine to be tolerant of bad lines
            df = pd.read_csv(path, engine='python', on_bad_lines='skip')

            # rewrite with strong quoting and escapechar
            df.to_csv(path, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
            print(f"  -> cleaned {fname}")

        except Exception as e:
            print(f"  -> FAILED cleaning {fname}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Prepare CSVs for Neo4j import')
    parser.add_argument('--source', default='./data/graph', help='Source directory with graph folders')
    parser.add_argument('--import', dest='import_dir', default='./neo4j/import', help='Target import directory')
    parser.add_argument('--skip-clean', action='store_true', help='Skip cleaning step')

    args = parser.parse_args()

    source_dir = args.source
    import_dir = args.import_dir

    if not os.path.exists(source_dir):
        print(f"Source directory not found: {source_dir}")
        return

    print("== Converting partitions to CSV ==")
    convert_partitions(source_dir, import_dir)

    if not args.skip_clean:
        print("\n== Cleaning CSV files for Neo4j import ==")
        clean_csvs(import_dir)

    print("\nDONE")


if __name__ == '__main__':
    main()
