"""
fundscope/backend/engine/persist.py

Database persistence layer — writes scraped performance data and rankings to DB.
"""

import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)


def upsert_funds(conn, performances: list[dict], week_date: date):
    """Insert or update fund records in the funds table."""
    today = week_date.isoformat()
    for p in performances:
        conn.execute("""
            INSERT INTO funds (fund_id, fund_name, isin, sector_code, active, first_seen, last_seen)
            VALUES (:fund_id, :fund_name, :isin, :sector_code, 1, :today, :today)
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name = excluded.fund_name,
                last_seen = :today,
                active = 1
        """, {**p, "today": today})


def upsert_performances(conn, performances: list[dict]):
    """Insert weekly performance snapshots. Skips if already present for this week."""
    for p in performances:
        conn.execute("""
            INSERT OR IGNORE INTO fund_performance
                (fund_id, week_date, return_1m, return_3m, return_6m)
            VALUES
                (:fund_id, :week_date, :return_1m, :return_3m, :return_6m)
        """, p)


def upsert_rankings(conn, rankings: list[dict]):
    """Insert weekly ranking rows."""
    for r in rankings:
        conn.execute("""
            INSERT OR REPLACE INTO fund_rankings
                (fund_id, sector_code, week_date,
                 decile_1m, decile_3m, decile_6m,
                 quartile_1m, quartile_3m, quartile_6m,
                 rank_1m, rank_3m, rank_6m,
                 total_in_sector,
                 streak_1m, streak_3m, streak_6m)
            VALUES
                (:fund_id, :sector_code, :week_date,
                 :decile_1m, :decile_3m, :decile_6m,
                 :quartile_1m, :quartile_3m, :quartile_6m,
                 :rank_1m, :rank_3m, :rank_6m,
                 :total_in_sector,
                 :streak_1m, :streak_3m, :streak_6m)
        """, r)


def insert_alerts(conn, alerts: list[dict]):
    """Insert alert records (only new ones — idempotent)."""
    for a in alerts:
        conn.execute("""
            INSERT OR IGNORE INTO alert_history
                (fund_id, sector_code, week_date, alert_type, period,
                 prev_decile, curr_decile, streak_broken, return_value)
            VALUES
                (:fund_id, :sector_code, :week_date, :alert_type, :period,
                 :prev_decile, :curr_decile, :streak_broken, :return_value)
        """, a)


def log_pipeline_run(conn, sector_code: str, status: str, funds_scraped: int,
                     error_message: str | None, duration_secs: float, run_date: date):
    conn.execute("""
        INSERT INTO pipeline_log
            (run_date, sector_code, status, funds_scraped, error_message, duration_secs)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (run_date.isoformat(), sector_code, status, funds_scraped, error_message, duration_secs))


def get_prior_rankings(conn, sector_code: str, week_date: date) -> list[dict]:
    """Fetch the most recent rankings for a sector prior to the given week."""
    import sqlite3
    cursor = conn.execute("""
        SELECT r.*, f.fund_name
        FROM fund_rankings r
        JOIN funds f ON f.fund_id = r.fund_id
        WHERE r.sector_code = ?
          AND r.week_date < ?
        ORDER BY r.week_date DESC, r.fund_id
    """, (sector_code, week_date.isoformat()))
    rows = cursor.fetchall()
    if not rows:
        return []
    # Only take the most recent week's data
    if rows:
        most_recent_date = rows[0]["week_date"] if hasattr(rows[0], "__getitem__") else rows[0][3]
        if isinstance(rows[0], sqlite3.Row):
            most_recent_date = dict(rows[0])["week_date"]
            return [dict(r) for r in rows if dict(r)["week_date"] == most_recent_date]
    return [dict(r) for r in rows]
