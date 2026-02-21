"""backend/db.py — Database connection manager."""
import os, sqlite3, logging
from pathlib import Path
from contextlib import contextmanager
import yaml

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).parent.parent / "config" / "sectors.yaml"

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

def get_sqlite_path():
    cfg = load_config()
    return os.environ.get("SQLITE_PATH", cfg["database"].get("sqlite_path", "./fundscope.db"))

def init_db(db_path=None):
    path = db_path or get_sqlite_path()
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    schema = (Path(__file__).parent / "schema.sql").read_text()
    conn.executescript(schema)
    # Seed sectors from config
    cfg = load_config()
    for s in cfg.get("sectors", []):
        conn.execute("""
            INSERT OR IGNORE INTO sectors (sector_code, sector_name, monitored)
            VALUES (?, ?, ?)
        """, (s["code"], s["name"], 1 if s.get("monitored") else 0))
    conn.commit()
    return conn

@contextmanager
def get_db(db_path=None):
    conn = init_db(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def row_to_dict(row):
    return dict(row) if row else {}

def rows_to_dicts(rows):
    return [dict(r) for r in rows]
