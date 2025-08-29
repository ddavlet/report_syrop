#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime
from subprocess import run, CalledProcessError

from flask import Flask, request, jsonify


app = Flask(__name__)


def _save_uploaded_json(file_storage, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    file_storage.save(dest_path)


def _trigger_load(json_path: Path) -> None:
    """
    Call existing loader to upsert into Postgres using env PG_DSN.
    """
    pg_dsn = os.getenv("PG_DSN", "").strip()
    args = ["python", "load_to_pg.py", "--backend", "json", "--json-path", str(json_path)]
    if pg_dsn:
        args += ["--pg-dsn", pg_dsn]
    try:
        res = run(args, check=True, capture_output=True, text=True)
        app.logger.info("Loader output: %s", res.stdout)
        if res.stderr:
            app.logger.warning("Loader stderr: %s", res.stderr)
    except CalledProcessError as e:
        app.logger.error("Loader failed: %s", e.stderr or e.stdout)
        raise


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat() + "Z"})


@app.route("/upload", methods=["POST"])
def upload():
    # Accepts multipart/form-data with field name 'file'
    if "file" not in request.files:
        return jsonify({"error": "missing file field 'file'"}), 400
    f = request.files["file"]
    if f.filename == "":
        return jsonify({"error": "empty filename"}), 400
    # Save to a stable location where the app already reads json by default
    dest = Path("/app/sales.json")
    try:
        _save_uploaded_json(f, dest)
        _trigger_load(dest)
        return jsonify({"status": "ok", "saved_to": str(dest)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    host = os.getenv("UPLOAD_HOST", "0.0.0.0")
    port = int(os.getenv("UPLOAD_PORT", "8000"))
    app.run(host=host, port=port)


