"""
backend/api/main.py — FastAPI REST API for FundScope

Key endpoints:
  GET  /api/sectors                        All IA sectors + monitored status
  POST /api/sectors/{code}/toggle          Toggle monitoring on/off for a sector
  POST /api/sectors/monitored              Batch update monitored sectors
  GET  /api/sectors/{code}/rankings        Current rankings for a monitored sector
  GET  /api/sectors/{code}/top3            Top 3 funds (6M)
  GET  /api/funds/{fund_id}                Fund detail + decile history
  GET  /api/alerts                         Alert history (paginated)
  GET  /api/alerts/latest                  This week's alerts
  GET  /api/summary                        Dashboard summary stats
  GET  /api/pipeline/status                Pipeline run log
  POST /api/pipeline/run                   Trigger pipeline (mock or live)
"""

import sys, os
from pathlib import Path
from datetime import date
from typing import Optional, List
import yaml

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from backend.db import get_db, rows_to_dicts, init_db

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "sectors.yaml"

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

def save_config(cfg: dict):
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

app = FastAPI(title="FundScope API", description="IA Unit Trust & OEIC Monitor", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve frontend
FRONTEND_DIR = Path(__file__).parent.parent.parent / "frontend"
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

def latest_week_date(conn) -> Optional[str]:
    row = conn.execute("SELECT MAX(week_date) as wd FROM fund_performance").fetchone()
    return dict(row).get("wd") if row else None

# ── Sectors ───────────────────────────────────────────────────────────────────

@app.get("/api/sectors")
def get_sectors():
    """All IA sectors with current monitored status."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT s.sector_code, s.sector_name, s.monitored,
                   COUNT(DISTINCT f.fund_id) as fund_count
            FROM sectors s
            LEFT JOIN funds f ON f.sector_code = s.sector_code AND f.active = 1
            GROUP BY s.sector_code
            ORDER BY s.sector_name
        """).fetchall()
        return rows_to_dicts(rows)

class SectorToggle(BaseModel):
    monitored: bool

@app.post("/api/sectors/{sector_code:path}/toggle")
def toggle_sector(sector_code: str, body: SectorToggle):
    """Toggle monitoring on/off for a specific IA sector."""
    with get_db() as conn:
        conn.execute(
            "UPDATE sectors SET monitored = ? WHERE sector_code = ?",
            (1 if body.monitored else 0, sector_code)
        )
        updated = conn.execute(
            "SELECT * FROM sectors WHERE sector_code = ?", (sector_code,)
        ).fetchone()
        if not updated:
            raise HTTPException(404, f"Sector '{sector_code}' not found")

    # Also update config yaml so it persists
    cfg = load_config()
    for s in cfg.get("sectors", []):
        if s["code"] == sector_code:
            s["monitored"] = body.monitored
    save_config(cfg)

    return {"sector_code": sector_code, "monitored": body.monitored}

class BatchSectors(BaseModel):
    sector_codes: List[str]

@app.post("/api/sectors/monitored")
def set_monitored_sectors(body: BatchSectors):
    """Set exactly which sectors are monitored (replaces all previous settings)."""
    cfg = load_config()
    enabled = set(body.sector_codes)
    with get_db() as conn:
        conn.execute("UPDATE sectors SET monitored = 0")
        for code in enabled:
            conn.execute("UPDATE sectors SET monitored = 1 WHERE sector_code = ?", (code,))
    for s in cfg.get("sectors", []):
        s["monitored"] = s["code"] in enabled
    save_config(cfg)
    return {"monitored_count": len(enabled), "sector_codes": list(enabled)}

@app.get("/api/sectors/{sector_code:path}/rankings")
def get_sector_rankings(sector_code: str,
                        week_date: Optional[str] = None,
                        period: str = Query("6m"),
                        limit: int = Query(200)):
    with get_db() as conn:
        if not week_date:
            week_date = latest_week_date(conn)
        if not week_date:
            return []
        period_col = f"return_{period}"
        rows = conn.execute(f"""
            SELECT f.fund_id, f.fund_name, f.isin, f.fund_group,
                   p.return_1m, p.return_3m, p.return_6m, p.return_1y,
                   r.decile_1m, r.decile_3m, r.decile_6m,
                   r.quartile_1m, r.quartile_3m, r.quartile_6m,
                   r.rank_1m, r.rank_3m, r.rank_6m,
                   r.streak_1m, r.streak_3m, r.streak_6m,
                   r.total_in_sector, r.week_date
            FROM fund_rankings r
            JOIN funds f ON f.fund_id = r.fund_id
            JOIN fund_performance p ON p.fund_id = r.fund_id AND p.week_date = r.week_date
            WHERE r.sector_code = ? AND r.week_date = ?
            ORDER BY p.{period_col} DESC NULLS LAST
            LIMIT ?
        """, (sector_code, week_date, limit)).fetchall()
        return rows_to_dicts(rows)

@app.get("/api/sectors/{sector_code:path}/top3")
def get_sector_top3(sector_code: str, week_date: Optional[str] = None):
    with get_db() as conn:
        if not week_date:
            week_date = latest_week_date(conn)
        if not week_date:
            return []
        rows = conn.execute("""
            SELECT f.fund_id, f.fund_name, f.fund_group,
                   p.return_6m, p.return_3m, p.return_1m, r.decile_6m, r.streak_6m
            FROM fund_performance p
            JOIN funds f ON f.fund_id = p.fund_id
            JOIN fund_rankings r ON r.fund_id = p.fund_id AND r.week_date = p.week_date
            WHERE p.week_date = ? AND f.sector_code = ? AND p.return_6m IS NOT NULL
            ORDER BY p.return_6m DESC LIMIT 3
        """, (week_date, sector_code)).fetchall()
        result = rows_to_dicts(rows)
        for i, r in enumerate(result):
            r["rank"] = i + 1
        return result

# ── Funds ─────────────────────────────────────────────────────────────────────

@app.get("/api/funds/search")
def search_funds(q: str = Query(..., min_length=2), limit: int = 20):
    with get_db() as conn:
        rows = conn.execute("""
            SELECT fund_id, fund_name, isin, sector_code, fund_group, active
            FROM funds WHERE fund_name LIKE ? AND active=1 ORDER BY fund_name LIMIT ?
        """, (f"%{q}%", limit)).fetchall()
        return rows_to_dicts(rows)

@app.get("/api/funds/{fund_id}")
def get_fund(fund_id: str, weeks: int = 52):
    with get_db() as conn:
        fund = conn.execute("SELECT * FROM funds WHERE fund_id=?", (fund_id,)).fetchone()
        if not fund:
            raise HTTPException(404, "Fund not found")
        history = conn.execute("""
            SELECT p.week_date, p.return_1m, p.return_3m, p.return_6m, p.return_1y,
                   r.decile_1m, r.decile_3m, r.decile_6m,
                   r.rank_1m, r.rank_3m, r.rank_6m,
                   r.streak_1m, r.streak_3m, r.streak_6m, r.total_in_sector
            FROM fund_performance p
            LEFT JOIN fund_rankings r ON r.fund_id=p.fund_id AND r.week_date=p.week_date
            WHERE p.fund_id=? ORDER BY p.week_date DESC LIMIT ?
        """, (fund_id, weeks)).fetchall()
        alerts = conn.execute("""
            SELECT * FROM alert_history WHERE fund_id=? ORDER BY week_date DESC LIMIT 20
        """, (fund_id,)).fetchall()
        return {"fund": dict(fund), "history": rows_to_dicts(history), "alerts": rows_to_dicts(alerts)}

# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/api/alerts")
def get_alerts(limit: int = 100, offset: int = 0,
               sector_code: Optional[str] = None, period: Optional[str] = None):
    with get_db() as conn:
        conds, params = [], []
        if sector_code:
            conds.append("a.sector_code = ?"); params.append(sector_code)
        if period:
            conds.append("a.period = ?"); params.append(period)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        rows = conn.execute(f"""
            SELECT a.*, f.fund_name, s.sector_name
            FROM alert_history a
            JOIN funds f ON f.fund_id = a.fund_id
            JOIN sectors s ON s.sector_code = a.sector_code
            {where}
            ORDER BY a.week_date DESC, a.streak_broken DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset]).fetchall()
        return rows_to_dicts(rows)

@app.get("/api/alerts/latest")
def get_latest_alerts():
    with get_db() as conn:
        wd = latest_week_date(conn)
        if not wd:
            return []
        rows = conn.execute("""
            SELECT a.*, f.fund_name, s.sector_name
            FROM alert_history a
            JOIN funds f ON f.fund_id=a.fund_id
            JOIN sectors s ON s.sector_code=a.sector_code
            WHERE a.week_date=? ORDER BY a.streak_broken DESC
        """, (wd,)).fetchall()
        return rows_to_dicts(rows)

# ── Summary ───────────────────────────────────────────────────────────────────

@app.get("/api/summary")
def get_summary():
    with get_db() as conn:
        wd = latest_week_date(conn)
        def n(q, *p):
            r = conn.execute(q, p).fetchone()
            return list(dict(r).values())[0] if r else 0
        return {
            "week_date":          wd,
            "total_funds":        n("SELECT COUNT(DISTINCT fund_id) FROM funds WHERE active=1"),
            "monitored_sectors":  n("SELECT COUNT(*) FROM sectors WHERE monitored=1"),
            "total_sectors":      n("SELECT COUNT(*) FROM sectors"),
            "alerts_this_week":   n("SELECT COUNT(*) FROM alert_history WHERE week_date=?", wd) if wd else 0,
            "total_alerts_ever":  n("SELECT COUNT(*) FROM alert_history"),
        }

@app.get("/api/pipeline/status")
def pipeline_status(limit: int = 30):
    with get_db() as conn:
        rows = conn.execute("""
            SELECT * FROM pipeline_log ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        return rows_to_dicts(rows)

# ── Pipeline trigger ──────────────────────────────────────────────────────────

class PipelineRequest(BaseModel):
    use_mock: bool = True
    dry_run: bool = True
    week_date: Optional[str] = None

pipeline_status_cache = {"running": False, "last_result": None}

def run_pipeline_task(use_mock: bool, dry_run: bool, week_date_str: Optional[str]):
    pipeline_status_cache["running"] = True
    try:
        from backend.pipeline import run_pipeline
        wd = date.fromisoformat(week_date_str) if week_date_str else date.today()
        result = run_pipeline(week_date=wd, use_mock_data=use_mock, dry_run=dry_run)
        pipeline_status_cache["last_result"] = result
    finally:
        pipeline_status_cache["running"] = False

@app.post("/api/pipeline/run")
def trigger_pipeline(req: PipelineRequest, background: BackgroundTasks):
    if pipeline_status_cache["running"]:
        raise HTTPException(409, "Pipeline already running")
    background.add_task(run_pipeline_task, req.use_mock, req.dry_run, req.week_date)
    return {"status": "started", "use_mock": req.use_mock, "dry_run": req.dry_run}

@app.get("/api/pipeline/running")
def pipeline_running():
    return pipeline_status_cache

@app.get("/health")
def health():
    return {"status": "ok", "universe": "IA Unit Trusts & OEICs"}

@app.get("/")
def serve_frontend():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "FundScope API — open /docs for API reference"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
