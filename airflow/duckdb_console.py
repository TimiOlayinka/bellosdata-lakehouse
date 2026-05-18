"""
BellosData DuckDB Query Console
Run: python duckdb_console.py
Cost: $0 — DuckDB reads S3 Delta tables directly from your machine.
"""
import duckdb

db = duckdb.connect()

# Install and load extensions
db.execute("INSTALL httpfs; INSTALL delta; LOAD httpfs; LOAD delta")
db.execute("SET s3_region = 'eu-west-2'")

print("=" * 60)
print("  BellosData Lake — DuckDB Query Console")
print("  Type SQL queries. Type 'exit' to quit.")
print("  Tables: delta_scan('s3://playdarch-bronze-raw/rdl/{source}/')")
print("=" * 60)
print()

# Show what's available
print("Running inventory check...")
try:
    for source in ["weather", "wind", "postcodes", "airports", "companies", "landscapes"]:
        try:
            r = db.execute(f"SELECT COUNT(*) AS n FROM delta_scan('s3://playdarch-bronze-raw/rdl/{source}/')").fetchone()
            print(f"  rdl/{source}: {r[0]:,} records")
        except Exception:
            print(f"  rdl/{source}: (no data yet)")
except Exception as e:
    print(f"  Inventory check error: {e}")

print()

# Interactive SQL loop
while True:
    try:
        sql = input("SQL> ").strip()
        if not sql:
            continue
        if sql.lower() in ("exit", "quit", "q"):
            break
        result = db.execute(sql).fetchdf()
        print(result.to_string(index=False))
        print(f"({len(result)} rows)")
        print()
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")
        print()

print("Session closed.")
