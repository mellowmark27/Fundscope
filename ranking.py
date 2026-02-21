"""backend/engine/ranking.py — Decile/quartile ranking and alert detection."""
import logging
from datetime import date
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def assign_deciles(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(pd.NA, index=series.index, dtype="Int64")
    ranks = valid.rank(ascending=False, method="first")
    n = len(valid)
    deciles = np.ceil(ranks / n * 10).clip(1, 10).astype("Int64")
    result = pd.Series(pd.NA, index=series.index, dtype="Int64")
    result[valid.index] = deciles
    return result

def assign_quartiles(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(pd.NA, index=series.index, dtype="Int64")
    ranks = valid.rank(ascending=False, method="first")
    n = len(valid)
    quartiles = np.ceil(ranks / n * 4).clip(1, 4).astype("Int64")
    result = pd.Series(pd.NA, index=series.index, dtype="Int64")
    result[valid.index] = quartiles
    return result

def assign_ranks(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    if len(valid) < 1:
        return pd.Series(pd.NA, index=series.index, dtype="Int64")
    ranks = valid.rank(ascending=False, method="first").astype("Int64")
    result = pd.Series(pd.NA, index=series.index, dtype="Int64")
    result[valid.index] = ranks
    return result

def rank_sector(sector_code: str, sector_name: str, performances: list[dict],
                week_date: date, prior: list[dict],
                quartile_threshold: int = 20, min_funds: int = 5) -> dict:
    if not performances:
        return {"rankings": [], "alerts": [], "top3": [], "use_quartiles": False, "n": 0}
    df = pd.DataFrame(performances)
    n = len(df)
    if n < min_funds:
        logger.warning(f"Sector {sector_name}: only {n} funds — skipping")
        return {"rankings": [], "alerts": [], "top3": [], "use_quartiles": False, "n": n}

    use_q = n < quartile_threshold
    for p in ["1m", "3m", "6m"]:
        col = f"return_{p}"
        if col in df.columns:
            df[f"decile_{p}"]   = assign_deciles(df[col])
            df[f"quartile_{p}"] = assign_quartiles(df[col])
            df[f"rank_{p}"]     = assign_ranks(df[col])
        else:
            df[f"decile_{p}"] = pd.NA
            df[f"quartile_{p}"] = pd.NA
            df[f"rank_{p}"] = pd.NA
    df["total_in_sector"] = n

    prior_map = {r["fund_id"]: r for r in (prior or [])}
    alerts, rankings = [], []

    for _, row in df.iterrows():
        fid = row["fund_id"]
        prev = prior_map.get(fid, {})
        streaks = {}

        for p in ["1m", "3m", "6m"]:
            cd = int(row[f"decile_{p}"]) if pd.notna(row[f"decile_{p}"]) else None
            cq = int(row[f"quartile_{p}"]) if pd.notna(row[f"quartile_{p}"]) else None
            prev_streak = int(prev.get(f"streak_{p}", 0) or 0)
            in_top = (cq == 1) if use_q else (cd == 1)
            streaks[f"streak_{p}"] = prev_streak + 1 if in_top else 0

            # Alert: was in top last week, not now
            if use_q:
                was_top = int(prev.get(f"quartile_{p}", 99) or 99) == 1
                if was_top and cq != 1:
                    alerts.append(dict(fund_id=fid, sector_code=sector_code,
                        week_date=week_date.isoformat(), alert_type="quartile_drop",
                        period=p, prev_decile=1, curr_decile=cq,
                        streak_broken=prev_streak,
                        return_value=float(row.get(f"return_{p}") or 0)))
            else:
                was_top = int(prev.get(f"decile_{p}", 99) or 99) == 1
                if was_top and cd != 1:
                    alerts.append(dict(fund_id=fid, sector_code=sector_code,
                        week_date=week_date.isoformat(), alert_type="decile_drop",
                        period=p, prev_decile=1, curr_decile=cd,
                        streak_broken=prev_streak,
                        return_value=float(row.get(f"return_{p}") or 0)))

        def safe_int(v):
            return int(v) if pd.notna(v) else None
        rankings.append({
            "fund_id": fid, "sector_code": sector_code, "week_date": week_date.isoformat(),
            "decile_1m": safe_int(row["decile_1m"]), "decile_3m": safe_int(row["decile_3m"]),
            "decile_6m": safe_int(row["decile_6m"]),
            "quartile_1m": safe_int(row["quartile_1m"]), "quartile_3m": safe_int(row["quartile_3m"]),
            "quartile_6m": safe_int(row["quartile_6m"]),
            "rank_1m": safe_int(row["rank_1m"]), "rank_3m": safe_int(row["rank_3m"]),
            "rank_6m": safe_int(row["rank_6m"]),
            "total_in_sector": n, **streaks,
        })

    df_6m = df.dropna(subset=["return_6m"]).nlargest(3, "return_6m")
    top3 = [{
        "rank": i+1, "fund_id": r["fund_id"], "fund_name": r["fund_name"],
        "sector_code": sector_code, "sector_name": sector_name,
        "return_6m": float(r["return_6m"]),
        "return_3m": float(r["return_3m"]) if pd.notna(r.get("return_3m")) else None,
        "return_1m": float(r["return_1m"]) if pd.notna(r.get("return_1m")) else None,
    } for i, (_, r) in enumerate(df_6m.iterrows())]

    return {"rankings": rankings, "alerts": alerts, "top3": top3, "use_quartiles": use_q, "n": n}
