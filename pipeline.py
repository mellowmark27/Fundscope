"""
backend/pipeline.py — Main weekly pipeline for IA Unit Trusts & OEICs.

Run manually:
    python -m backend.pipeline --mock --dry-run
    python -m backend.pipeline --date 2026-02-16
"""
import logging, os, sys, time
from datetime import date
from pathlib import Path
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.db import get_db, init_db, rows_to_dicts
from backend.scraper.trustnet import TrustnetScraper, generate_mock_data
from backend.engine.ranking import rank_sector

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("pipeline.log")])
logger = logging.getLogger("fundscope.pipeline")

CONFIG_PATH = Path(__file__).parent.parent / "config" / "sectors.yaml"

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def run_pipeline(week_date: date = None, use_mock_data: bool = False, dry_run: bool = False) -> dict:
    if week_date is None:
        week_date = date.today()

    logger.info(f"{'='*60}")
    logger.info(f"FundScope Pipeline — IA Unit Trusts & OEICs — {week_date}")
    logger.info(f"Mode: {'MOCK' if use_mock_data else 'LIVE'} | dry_run={dry_run}")

    cfg = load_config()
    monitored = [s for s in cfg["sectors"] if s.get("monitored")]
    logger.info(f"Monitoring {len(monitored)} IA sectors")

    # Init DB
    init_db()

    # ── Step 1: Scrape / generate data ───────────────────────────────────────
    all_performances, failed_sectors = {}, []

    if use_mock_data:
        for s in monitored:
            data = generate_mock_data(s["code"], week_date)
            all_performances[s["code"]] = data
            logger.info(f"  ✓ Mock: {s['name']} ({len(data)} funds)")
    else:
        scraper = TrustnetScraper()
        for s in monitored:
            t = time.time()
            try:
                data = scraper.fetch_sector(s["code"], week_date)
                all_performances[s["code"]] = data
                logger.info(f"  ✓ Scraped: {s['name']} ({len(data)} funds in {time.time()-t:.1f}s)")
            except Exception as e:
                failed_sectors.append(s["name"])
                logger.error(f"  ✗ Failed: {s['name']} — {e}")

    total_funds = sum(len(v) for v in all_performances.values())
    logger.info(f"Total fund records: {total_funds}")

    # ── Step 2: Persist performance data ─────────────────────────────────────
    with get_db() as conn:
        for sector_code, perfs in all_performances.items():
            for p in perfs:
                # Upsert fund
                conn.execute("""
                    INSERT INTO funds (fund_id, fund_name, isin, sedol, sector_code, fund_group, active, first_seen, last_seen)
                    VALUES (:fund_id,:fund_name,:isin,:sedol,:sector_code,:fund_group,1,:wd,:wd)
                    ON CONFLICT(fund_id) DO UPDATE SET
                        fund_name=excluded.fund_name, last_seen=excluded.last_seen, active=1,
                        fund_group=COALESCE(excluded.fund_group, fund_group)
                """, {**p, "wd": week_date.isoformat()})
                # Upsert performance
                conn.execute("""
                    INSERT OR IGNORE INTO fund_performance
                        (fund_id, week_date, return_1m, return_3m, return_6m, return_1y)
                    VALUES (:fund_id,:week_date,:return_1m,:return_3m,:return_6m,:return_1y)
                """, p)
    logger.info("Performance data persisted")

    # ── Step 3: Get prior rankings for streak/alert comparison ────────────────
    prior_by_sector = {}
    with get_db() as conn:
        for sector_code in all_performances:
            rows = conn.execute("""
                SELECT r.* FROM fund_rankings r
                WHERE r.sector_code=? AND r.week_date=(
                    SELECT MAX(week_date) FROM fund_rankings
                    WHERE sector_code=? AND week_date < ?
                )
            """, (sector_code, sector_code, week_date.isoformat())).fetchall()
            prior_by_sector[sector_code] = rows_to_dicts(rows)

    # ── Step 4: Rank and detect alerts ────────────────────────────────────────
    sector_map = {s["code"]: s["name"] for s in cfg["sectors"]}
    all_rankings, all_alerts, all_top3 = [], [], {}

    for sector_code, perfs in all_performances.items():
        result = rank_sector(
            sector_code, sector_map.get(sector_code, sector_code),
            perfs, week_date, prior_by_sector.get(sector_code, [])
        )
        all_rankings.extend(result["rankings"])
        all_alerts.extend(result["alerts"])
        all_top3[sector_code] = result["top3"]

    logger.info(f"Rankings: {len(all_rankings)} | Alerts: {len(all_alerts)}")

    # ── Step 5: Persist rankings and alerts ───────────────────────────────────
    with get_db() as conn:
        for r in all_rankings:
            conn.execute("""
                INSERT OR REPLACE INTO fund_rankings
                    (fund_id,sector_code,week_date,
                     decile_1m,decile_3m,decile_6m,quartile_1m,quartile_3m,quartile_6m,
                     rank_1m,rank_3m,rank_6m,total_in_sector,streak_1m,streak_3m,streak_6m)
                VALUES
                    (:fund_id,:sector_code,:week_date,
                     :decile_1m,:decile_3m,:decile_6m,:quartile_1m,:quartile_3m,:quartile_6m,
                     :rank_1m,:rank_3m,:rank_6m,:total_in_sector,:streak_1m,:streak_3m,:streak_6m)
            """, r)
        for a in all_alerts:
            conn.execute("""
                INSERT OR IGNORE INTO alert_history
                    (fund_id,sector_code,week_date,alert_type,period,prev_decile,curr_decile,streak_broken,return_value)
                VALUES (:fund_id,:sector_code,:week_date,:alert_type,:period,:prev_decile,:curr_decile,:streak_broken,:return_value)
            """, a)

    # ── Step 6: Build fund name lookup ────────────────────────────────────────
    fund_names = {}
    with get_db() as conn:
        for row in conn.execute("SELECT fund_id, fund_name FROM funds").fetchall():
            fund_names[row["fund_id"]] = row["fund_name"]

    # ── Step 7: Email ─────────────────────────────────────────────────────────
    email_ok = True
    if not dry_run:
        try:
            from backend.email.dispatcher import dispatch_digest
            email_ok = dispatch_digest(
                week_date=week_date, alerts=all_alerts, top3_by_sector=all_top3,
                fund_names=fund_names, sector_names=sector_map,
                failed_sectors=failed_sectors, total_funds=total_funds,
            )
        except Exception as e:
            logger.error(f"Email failed: {e}")
            email_ok = False
    else:
        try:
            from backend.email.dispatcher import render_digest
            html = render_digest(week_date, all_alerts, all_top3, fund_names,
                                 sector_map, failed_sectors, total_funds)
            out = Path(f"digest_preview_{week_date}.html")
            out.write_text(html)
            logger.info(f"Email preview → {out}")
        except Exception as e:
            logger.error(f"Preview render failed: {e}")

    summary = dict(week_date=week_date.isoformat(), sectors_ok=len(all_performances),
                   sectors_failed=len(failed_sectors), total_funds=total_funds,
                   alert_count=len(all_alerts), email_sent=email_ok)
    logger.info(f"Pipeline complete: {summary}")
    return summary


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mock",    action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--date",    default=None)
    args = p.parse_args()
    wd = date.fromisoformat(args.date) if args.date else date.today()
    run_pipeline(week_date=wd, use_mock_data=args.mock, dry_run=args.dry_run)
