"""
BellosData DuckDB Query Service — Cloud SQL Console

A lightweight web-based SQL editor backed by DuckDB.
Pre-configured with S3 Delta lake credentials and table views.

Access: http://13.135.211.178:8082
Cost: $0 — runs on existing Lightsail instance.

Author: Awujoo (AWUJOO-041) | Genesis: 2026-05-17
"""

from flask import Flask, request, jsonify, render_template_string
import duckdb
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

BRONZE = "s3://playdarch-bronze-raw"
SILVER = "s3://playdarch-silver-curated"


def get_db():
    """Create a DuckDB connection with S3 + Delta configured."""
    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs")

    region = os.environ.get("AWS_DEFAULT_REGION", "eu-west-2")
    key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    db.execute(f"SET s3_region = '{region}'")
    db.execute(f"SET s3_access_key_id = '{key}'")
    db.execute(f"SET s3_secret_access_key = '{secret}'")
    db.execute(f"SET s3_endpoint = 's3.{region}.amazonaws.com'")
    db.execute("SET s3_url_style = 'vhost'")

    # Create views for all tables so user can just: SELECT * FROM weather
    views = {
        # Bronze (RDL)
        "weather":    f"{BRONZE}/rdl/weather/**/*.parquet",
        "wind":       f"{BRONZE}/rdl/wind/**/*.parquet",
        "postcodes":  f"{BRONZE}/rdl/postcodes/**/*.parquet",
        "airports":   f"{BRONZE}/rdl/airports/**/*.parquet",
        "companies":  f"{BRONZE}/rdl/companies/**/*.parquet",
        "landscapes": f"{BRONZE}/rdl/landscapes/**/*.parquet",
        # Silver dimensions
        "dim_date":            f"{SILVER}/odl/dim/dim_date/**/*.parquet",
        "dim_location":        f"{SILVER}/odl/dim/dim_location/**/*.parquet",
        "dim_weather_station": f"{SILVER}/odl/dim/dim_weather_station/**/*.parquet",
        "dim_airport":         f"{SILVER}/odl/dim/dim_airport/**/*.parquet",
        "dim_habitat":         f"{SILVER}/odl/dim/dim_habitat/**/*.parquet",
        "dim_company":         f"{SILVER}/odl/dim/dim_company/**/*.parquet",
        "dim_species":         f"{SILVER}/odl/dim/dim_species/**/*.parquet",
        # Silver facts
        "fact_weather":  f"{SILVER}/odl/fact/fact_weather_observation/**/*.parquet",
        "fact_wind":     f"{SILVER}/odl/fact/fact_wind_measurement/**/*.parquet",
        "fact_flight":   f"{SILVER}/odl/fact/fact_flight_movement/**/*.parquet",
        "fact_company":  f"{SILVER}/odl/fact/fact_company_filing/**/*.parquet",
        # Silver mappings
        "map_postcode_location":  f"{SILVER}/odl/map/map_postcode_to_location/**/*.parquet",
        "map_company_postcode":   f"{SILVER}/odl/map/map_company_to_postcode/**/*.parquet",
        "map_airport_location":   f"{SILVER}/odl/map/map_airport_to_location/**/*.parquet",
        "map_species_habitat":    f"{SILVER}/odl/map/map_species_to_habitat/**/*.parquet",
        "map_owner_company":      f"{SILVER}/odl/map/map_owner_to_company/**/*.parquet",
        "map_station_location":   f"{SILVER}/odl/map/map_station_to_location/**/*.parquet",
    }

    for name, path in views.items():
        try:
            db.execute(f"""
                CREATE OR REPLACE VIEW {name} AS
                SELECT * FROM read_parquet('{path}', hive_partitioning=true)
            """)
        except Exception as e:
            logger.error(f"Failed to create view {name}: {e}")

    return db


# ── HTML Template ──
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BellosData SQL Console</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
    --text: #e4e4e7; --muted: #71717a; --accent: #6366f1;
    --accent-hover: #818cf8; --success: #22c55e; --error: #ef4444;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }

  .header {
    background: linear-gradient(135deg, #1e1b4b 0%, #312e81 50%, #1e1b4b 100%);
    padding: 20px 32px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 16px;
  }
  .header h1 { font-size: 20px; font-weight: 700; }
  .header .badge {
    background: var(--accent); color: white; padding: 2px 10px;
    border-radius: 12px; font-size: 11px; font-weight: 600;
  }

  .container { max-width: 1400px; margin: 0 auto; padding: 24px; }

  .editor-wrap {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; overflow: hidden; margin-bottom: 20px;
  }
  .editor-bar {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 16px; border-bottom: 1px solid var(--border);
  }
  .editor-bar span { font-size: 13px; color: var(--muted); }

  textarea {
    width: 100%; min-height: 140px; padding: 16px; border: none;
    background: transparent; color: var(--text); resize: vertical;
    font-family: 'JetBrains Mono', monospace; font-size: 14px;
    line-height: 1.6; outline: none;
  }

  .actions { padding: 12px 16px; display: flex; gap: 10px; align-items: center; }

  button {
    padding: 8px 20px; border: none; border-radius: 8px; cursor: pointer;
    font-family: 'Inter', sans-serif; font-weight: 600; font-size: 13px;
    transition: all 0.15s ease;
  }
  .btn-run { background: var(--accent); color: white; }
  .btn-run:hover { background: var(--accent-hover); transform: translateY(-1px); }
  .btn-clear { background: var(--border); color: var(--text); }

  .status {
    font-size: 12px; color: var(--muted); margin-left: auto;
    font-family: 'JetBrains Mono', monospace;
  }
  .status.ok { color: var(--success); }
  .status.err { color: var(--error); }

  .results {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; overflow: hidden;
  }
  .results-bar {
    padding: 12px 16px; border-bottom: 1px solid var(--border);
    font-size: 13px; color: var(--muted);
  }

  table {
    width: 100%; border-collapse: collapse;
    font-family: 'JetBrains Mono', monospace; font-size: 13px;
  }
  th {
    text-align: left; padding: 10px 14px; background: #1e2030;
    color: var(--accent-hover); font-weight: 600; font-size: 12px;
    text-transform: uppercase; letter-spacing: 0.5px;
    border-bottom: 1px solid var(--border); position: sticky; top: 0;
  }
  td {
    padding: 8px 14px; border-bottom: 1px solid var(--border);
    max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  tr:hover td { background: rgba(99, 102, 241, 0.05); }

  .table-scroll { max-height: 500px; overflow: auto; }

  .sidebar {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 16px; margin-bottom: 20px;
  }
  .sidebar h3 { font-size: 13px; color: var(--accent-hover); margin-bottom: 12px; text-transform: uppercase; letter-spacing: 1px; }
  .sidebar .tbl {
    font-family: 'JetBrains Mono', monospace; font-size: 12px;
    padding: 4px 0; color: var(--muted); cursor: pointer;
  }
  .sidebar .tbl:hover { color: var(--accent-hover); }
  .sidebar .tbl.has-data { color: var(--success); }
  .sidebar .section { margin-bottom: 16px; }
  .sidebar .section-title { font-size: 11px; color: var(--muted); margin-bottom: 6px; text-transform: uppercase; }

  .layout { display: grid; grid-template-columns: 220px 1fr; gap: 20px; }

  @media (max-width: 768px) { .layout { grid-template-columns: 1fr; } }

  .error-msg {
    padding: 16px; color: var(--error);
    font-family: 'JetBrains Mono', monospace; font-size: 13px;
  }

  .examples {
    display: flex; gap: 8px; flex-wrap: wrap; padding: 0 16px 12px;
  }
  .example-btn {
    background: var(--bg); border: 1px solid var(--border); color: var(--muted);
    padding: 4px 12px; border-radius: 6px; font-size: 11px; cursor: pointer;
  }
  .example-btn:hover { border-color: var(--accent); color: var(--text); }
</style>
</head>
<body>
<div class="header">
  <h1>BellosData SQL Console</h1>
  <span class="badge">DuckDB</span>
  <span class="badge" style="background:#22c55e">$0 Cost</span>
</div>
<div class="container">
<div class="layout">
  <div>
    <div class="sidebar">
      <h3>Tables</h3>
      <div class="section">
        <div class="section-title">Bronze (RDL)</div>
        {% for t in tables.bronze %}
        <div class="tbl {{ 'has-data' if t.rows > 0 else '' }}"
             onclick="setQuery('SELECT * FROM {{ t.name }} LIMIT 20')">
          {{ t.name }} {% if t.rows > 0 %}({{ t.rows }}){% endif %}
        </div>
        {% endfor %}
      </div>
      <div class="section">
        <div class="section-title">Silver — Dimensions</div>
        {% for t in tables.dims %}
        <div class="tbl {{ 'has-data' if t.rows > 0 else '' }}"
             onclick="setQuery('SELECT * FROM {{ t.name }} LIMIT 20')">
          {{ t.name }} {% if t.rows > 0 %}({{ t.rows }}){% endif %}
        </div>
        {% endfor %}
      </div>
      <div class="section">
        <div class="section-title">Silver — Facts</div>
        {% for t in tables.facts %}
        <div class="tbl {{ 'has-data' if t.rows > 0 else '' }}"
             onclick="setQuery('SELECT * FROM {{ t.name }} LIMIT 20')">
          {{ t.name }} {% if t.rows > 0 %}({{ t.rows }}){% endif %}
        </div>
        {% endfor %}
      </div>
      <div class="section">
        <div class="section-title">Silver — Mappings</div>
        {% for t in tables.maps %}
        <div class="tbl {{ 'has-data' if t.rows > 0 else '' }}"
             onclick="setQuery('SELECT * FROM {{ t.name }} LIMIT 20')">
          {{ t.name }} {% if t.rows > 0 %}({{ t.rows }}){% endif %}
        </div>
        {% endfor %}
      </div>
    </div>
  </div>
  <div>
    <form method="POST" action="/query" id="queryForm">
    <div class="editor-wrap">
      <div class="editor-bar">
        <span>SQL Editor</span>
        <span style="font-size:11px">Ctrl+Enter to run</span>
      </div>
      <textarea name="sql" id="sql" placeholder="SELECT * FROM weather LIMIT 10">{{ sql or '' }}</textarea>
      <div class="examples">
        <div class="example-btn" onclick="setQuery('SELECT * FROM weather LIMIT 20')">Weather</div>
        <div class="example-btn" onclick="setQuery('SELECT * FROM wind LIMIT 20')">Wind</div>
        <div class="example-btn" onclick="setQuery('SHOW TABLES')">Show Tables</div>
        <div class="example-btn" onclick="setQuery('SELECT json_extract_string(json, \'$.label\') AS station, AVG(CAST(json_extract_string(json, \'$.temperature_2m\') AS DOUBLE)) AS avg_temp FROM weather GROUP BY 1')">Avg Temp</div>
      </div>
      <div class="actions">
        <button type="submit" class="btn-run">▶ Run Query</button>
        <button type="button" class="btn-clear" onclick="document.getElementById('sql').value=''">Clear</button>
        {% if status %}
        <span class="status {{ 'ok' if not error else 'err' }}">{{ status }}</span>
        {% endif %}
      </div>
    </div>
    </form>

    {% if error %}
    <div class="results">
      <div class="results-bar">Error</div>
      <div class="error-msg">{{ error }}</div>
    </div>
    {% elif columns %}
    <div class="results">
      <div class="results-bar">{{ row_count }} rows × {{ columns|length }} columns ({{ duration_ms }}ms)</div>
      <div class="table-scroll">
        <table>
          <thead><tr>{% for c in columns %}<th>{{ c }}</th>{% endfor %}</tr></thead>
          <tbody>
            {% for row in rows %}
            <tr>{% for val in row %}<td title="{{ val }}">{{ val }}</td>{% endfor %}</tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    </div>
    {% endif %}
  </div>
</div>
</div>
<script>
function setQuery(q) { document.getElementById('sql').value = q; }
document.getElementById('sql').addEventListener('keydown', function(e) {
  if (e.ctrlKey && e.key === 'Enter') { document.getElementById('queryForm').submit(); }
});
</script>
</body>
</html>"""


def get_table_inventory(db):
    """Get row counts for all known tables."""
    tables = {
        "bronze": [
            {"name": "weather"}, {"name": "wind"}, {"name": "postcodes"},
            {"name": "airports"}, {"name": "companies"}, {"name": "landscapes"},
        ],
        "dims": [
            {"name": "dim_date"}, {"name": "dim_location"}, {"name": "dim_weather_station"},
            {"name": "dim_airport"}, {"name": "dim_habitat"}, {"name": "dim_company"},
            {"name": "dim_species"},
        ],
        "facts": [
            {"name": "fact_weather"}, {"name": "fact_wind"},
            {"name": "fact_flight"}, {"name": "fact_company"},
        ],
        "maps": [
            {"name": "map_postcode_location"}, {"name": "map_company_postcode"},
            {"name": "map_airport_location"}, {"name": "map_species_habitat"},
            {"name": "map_owner_company"}, {"name": "map_station_location"},
        ],
    }

    for section in tables.values():
        for t in section:
            try:
                r = db.execute(f"SELECT COUNT(*) FROM {t['name']}").fetchone()
                t["rows"] = r[0]
            except Exception:
                t["rows"] = 0

    return tables


@app.route("/", methods=["GET"])
def index():
    db = get_db()
    tables = get_table_inventory(db)
    return render_template_string(HTML, tables=tables, sql="", columns=None, rows=None,
                                  row_count=0, error=None, status=None, duration_ms=0)


@app.route("/query", methods=["POST"])
def query():
    import time
    sql = request.form.get("sql", "").strip()
    db = get_db()
    tables = get_table_inventory(db)

    if not sql:
        return render_template_string(HTML, tables=tables, sql=sql, columns=None, rows=None,
                                      row_count=0, error="No query provided", status="Error", duration_ms=0)

    try:
        t0 = time.time()
        result = db.execute(sql)
        columns = [desc[0] for desc in result.description] if result.description else []
        rows = result.fetchall()
        duration = int((time.time() - t0) * 1000)

        # Truncate large text fields for display
        display_rows = []
        for row in rows[:500]:  # Cap at 500 rows
            display_rows.append([str(v)[:200] if v is not None else "NULL" for v in row])

        return render_template_string(HTML, tables=tables, sql=sql, columns=columns,
                                      rows=display_rows, row_count=len(rows),
                                      error=None, status=f"{len(rows)} rows in {duration}ms",
                                      duration_ms=duration)
    except Exception as e:
        return render_template_string(HTML, tables=tables, sql=sql, columns=None,
                                      rows=None, row_count=0, error=str(e),
                                      status="Error", duration_ms=0)


@app.route("/api/query", methods=["POST"])
def api_query():
    """JSON API endpoint — for IDE REST clients / curl / scripts."""
    import time
    data = request.get_json(force=True, silent=True) or {}
    sql = data.get("sql", request.form.get("sql", "")).strip()

    if not sql:
        return jsonify({"error": "No SQL provided"}), 400

    try:
        db = get_db()
        t0 = time.time()
        result = db.execute(sql)
        columns = [desc[0] for desc in result.description] if result.description else []
        rows = result.fetchall()
        duration = int((time.time() - t0) * 1000)

        return jsonify({
            "columns": columns,
            "rows": [dict(zip(columns, [str(v) for v in row])) for row in rows[:1000]],
            "row_count": len(rows),
            "duration_ms": duration,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8082, debug=False)
