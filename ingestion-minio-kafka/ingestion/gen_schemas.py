"""Generate a JSON Schema (draft 2020-12) per Kafka topic + 1 example event per topic, then self-check.

Types and nullability come from the full staged data; column descriptions come from
HomeCredit_columns_description.csv. Run after stage.py:
    .venv/Scripts/python -I ingestion/gen_schemas.py
"""
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.compute as pc  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from jsonschema import Draft202012Validator  # noqa: E402

import common as C  # noqa: E402

ISO_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
DESC_TABLE = {"application": "application_{train|test}.csv"}  # other tables: the raw file name
MAX_OBSERVED = 60
SAMPLE_PER_TOPIC = 2000


def descriptions():
    d = pd.read_csv(C.RAW_DIR / "HomeCredit_columns_description.csv", encoding="cp1252")
    return {(r.Table, r.Row): (r.Description, r.Special) for r in d.itertuples()}


def json_type(t):
    if pa.types.is_integer(t):
        return "integer"
    if pa.types.is_floating(t):
        return "number"
    if pa.types.is_boolean(t):
        return "boolean"
    return "string"


def null_counts(pf):
    nulls = defaultdict(int)
    meta = pf.metadata
    for rg in range(meta.num_row_groups):
        for ci in range(meta.num_columns):
            col = meta.row_group(rg).column(ci)
            st = col.statistics
            nulls[col.path_in_schema] += st.null_count if st is not None and st.has_null_count else 1
    return nulls


def envelope_props(src):
    return {
        "event_id": {"type": "string", "pattern": "^[0-9a-f]{32}$",
                     "description": "BLAKE2b-128 of source_table + natural key. Deterministic -> use it for de-duplication."},
        "event_type": {"const": src.event_type},
        "schema_version": {"type": "string", "pattern": r"^1\.\d+\.\d+$",
                           "description": "SemVer. A MAJOR change -> new topic (.v2)."},
        "source_system": {"const": src.source_system},
        "source_table": {"const": src.table},
        "event_time": {"type": "string", "format": "date-time", "pattern": ISO_PATTERN,
                       "description": f"Business time (simulated, +07:00). {src.time_rule}"},
        "produced_at": {"type": "string", "format": "date-time", "pattern": ISO_PATTERN,
                        "description": "Wall-clock time the producer sent the event."},
        "customer_id": {"type": "integer", "description": "SK_ID_CURR of the customer; also the Kafka message key."},
    }


def build_schema(src, desc):
    path = C.STAGED_DIR / f"{src.table}.parquet"
    pf = pq.ParquetFile(path)
    arrow = pf.schema_arrow
    nulls = null_counts(pf)
    payload_cols = [c for c in arrow.names if c not in C.ENVELOPE_COLS]
    str_cols = [c for c in payload_cols if json_type(arrow.field(c).type) == "string"]
    observed = {}
    if str_cols:
        tbl = pq.read_table(path, columns=str_cols)
        for c in str_cols:
            vals = pc.unique(tbl[c]).drop_null().to_pylist()
            if len(vals) <= MAX_OBSERVED:
                observed[c] = sorted(vals)
    props = {}
    for c in payload_cols:
        t = json_type(arrow.field(c).type)
        p = {"type": [t, "null"] if nulls[c] else t}
        d, special = desc.get((DESC_TABLE.get(src.table, src.files[0]), c), (None, None))
        if isinstance(d, str):
            p["description"] = d.strip()
        if isinstance(special, str):
            p["x-note"] = special.strip()
        if c in observed:
            p["x-observed-values"] = observed[c]
        props[c] = p
    env = envelope_props(src)
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"urn:homecredit-bigdata:schema:{src.topic}",
        "title": f"{src.event_type} ({src.source_system})",
        "description": (f"One event = one row of {', '.join(src.files)} (original column names and values "
                        f"are kept in payload). event_time: {src.time_rule}"),
        "x-kafka": {"topic": src.topic, "key": "customer_id (UTF-8 string)", "value": "JSON UTF-8",
                    "headers": ["event_type", "schema_version"]},
        "type": "object",
        "additionalProperties": False,
        "required": list(env) + ["payload"],
        "properties": {**env, "payload": {
            "type": "object", "additionalProperties": False, "required": payload_cols, "properties": props}},
    }


DLQ_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": f"urn:homecredit-bigdata:schema:{C.DLQ_TOPIC}",
    "title": "Dead-letter event",
    "description": "Written by the sink when a message is not valid JSON or violates its topic schema.",
    "x-kafka": {"topic": C.DLQ_TOPIC, "key": "original key", "value": "JSON UTF-8"},
    "type": "object",
    "additionalProperties": False,
    "required": ["original_topic", "partition", "offset", "error", "raw_value", "failed_at"],
    "properties": {
        "original_topic": {"type": "string"},
        "partition": {"type": "integer"},
        "offset": {"type": "integer"},
        "error": {"type": "string", "description": "First error (parse error or schema violation)."},
        "raw_value": {"type": "string", "description": "Original message, verbatim."},
        "failed_at": {"type": "string", "format": "date-time", "pattern": ISO_PATTERN},
    },
}


def main():
    desc = descriptions()
    (C.SCHEMA_DIR / "examples").mkdir(parents=True, exist_ok=True)
    now = datetime.now(C.TZ).isoformat(timespec="milliseconds")
    for src in C.SOURCES.values():
        schema = build_schema(src, desc)
        (C.SCHEMA_DIR / f"{src.topic}.schema.json").write_text(
            json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
        validator = Draft202012Validator(schema)

        live = pd.read_parquet(C.STAGED_DIR / f"{src.table}.parquet", filters=[("event_time", ">=", C.LIVE_START)])
        sample = live.sample(min(SAMPLE_PER_TOPIC, len(live)), random_state=C.SEED).sort_values("event_time")
        events = [json.loads(e.replace(C.PRODUCED_AT_TOKEN, now)) for e in C.serialize_events(src, sample)]
        bad = [e for e in events if not validator.is_valid(e)]
        (C.SCHEMA_DIR / "examples" / f"{src.topic}.json").write_text(
            json.dumps(events[0], ensure_ascii=False, indent=2), encoding="utf-8")
        described = sum("description" in p for p in schema["properties"]["payload"]["properties"].values())
        n_cols = len(schema["properties"]["payload"]["properties"])
        print(f"{src.topic:40s} {n_cols:>3} payload fields ({described} described) | "
              f"validated {len(events):,} sample events: {len(bad)} errors")
        if bad:
            print("   first error:", next(validator.iter_errors(bad[0])).message)
    (C.SCHEMA_DIR / f"{C.DLQ_TOPIC}.schema.json").write_text(
        json.dumps(DLQ_SCHEMA, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"-> {C.SCHEMA_DIR}")


if __name__ == "__main__":
    main()
