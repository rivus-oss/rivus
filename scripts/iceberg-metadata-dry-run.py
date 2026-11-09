#!/usr/bin/env python3
"""Generate Spark SQL previews from catalog/table/location inventory. Never deletes."""
import argparse
import datetime as dt
import json
from pathlib import Path
from urllib.parse import urlsplit


def identifier(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Each catalog, namespace component, and table must be named")
    return "`" + value.replace("`", "``") + "`"


def literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path, help="JSON list of catalog, namespace (list), table, location")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = json.loads(args.inventory.read_text())
    if not isinstance(rows, list) or not rows:
        parser.error("Inventory must be a nonempty JSON list")
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    statements = ["-- Preview only: no files are deleted. Cutoff is fixed at generation time.",
                  "SET spark.sql.session.timeZone=UTC;"]
    seen = set()
    for row in rows:
        namespace = row.get("namespace")
        if not isinstance(namespace, list) or not namespace:
            parser.error("namespace must be a nonempty list, preserving nested namespaces")
        catalog = identifier(row.get("catalog"))
        table = ".".join(identifier(v) for v in namespace + [row.get("table")])
        key = (catalog, table)
        if key in seen:
            parser.error("Duplicate table in inventory")
        seen.add(key)
        location = str(row.get("location") or "").rstrip("/")
        uri = urlsplit(location)
        if (uri.scheme not in {"s3", "s3a", "s3n", "hdfs", "file"}
                or not uri.path.strip("/") or uri.query or uri.fragment
                or uri.username or uri.password
                or any(p in {".", ".."} for p in uri.path.split("/"))):
            parser.error("Each location must be a table directory URI without credentials or query parameters")
        statements.append(f"CALL {catalog}.system.remove_orphan_files(\n"
                          f"  table => {literal(table)},\n"
                          f"  older_than => TIMESTAMP '{cutoff}',\n"
                          f"  location => {literal(location + '/metadata')},\n"
                          "  dry_run => true\n);")
    args.output.write_text("\n\n".join(statements) + "\n")
    print(f"Generated {len(seen)} metadata-only previews with cutoff {cutoff} UTC: {args.output}")


if __name__ == "__main__":
    main()
