"""
backend/scraper/trustnet.py

Trustnet scraper for IA Unit Trusts & OEICs universe.

Universe code: o
URL pattern:
  https://www.trustnet.com/fund/price-performance/o/ia-unit-trusts-and-oeics
  ?norisk=true&sortby=P11GBP_D_6M&sortorder=desc&PageSize=500&sector=IA+UK+All+Companies

Trustnet renders tables client-side via JS, so data comes from their internal API.
The underlying API endpoint (discovered via browser DevTools Network tab) is:
  https://www.trustnet.com/api/v2/fund/price-performance
  ?universe=o&sector={encoded_sector}&sortby=P11GBP_D_6M&sortorder=desc&pageSize=500

Column mapping from Trustnet API response:
  fundName       → fund_name
  isin           → isin
  performance1M  → return_1m
  performance3M  → return_3m
  performance6M  → return_6m   (sortby param key: P11GBP_D_6M)
  performance1Y  → return_1y
  performance3Y  → return_3y
  sectorName     → sector_code (verify)

NOTE: Verify the exact API endpoint and field names using browser DevTools
      (Network tab → XHR/Fetch) on the Trustnet performance page before deploying.
      The mock data generator below provides a faithful substitute for development.
"""

import re, time, logging, random
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus
import yaml

logger = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "sectors.yaml"

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

def make_fund_id(fund_name: str, isin: str = None) -> str:
    if isin and isin.strip() and isin.strip().upper() not in ("N/A", "NONE", ""):
        return isin.strip().upper()
    slug = re.sub(r"[^a-z0-9]+", "-", fund_name.lower()).strip("-")[:60]
    return f"f-{slug}"


class TrustnetScraper:
    """
    Fetches IA Unit Trust & OEIC performance data from Trustnet.

    The site is JavaScript-rendered. Two approaches (in priority order):
      1. Trustnet internal REST API (discovered via DevTools) — JSON, clean
      2. HTML table scraping with BeautifulSoup — fallback

    Trustnet internal API (verify endpoint via DevTools before deployment):
      GET https://www.trustnet.com/api/v2/fund/price-performance
          ?universe=o
          &sector=IA+UK+All+Companies
          &sortby=P11GBP_D_6M
          &sortorder=desc
          &pageSize=500
          &norisk=true
    """

    # Attempt these API patterns in order until one works
    API_CANDIDATES = [
        "https://www.trustnet.com/api/v2/fund/price-performance",
        "https://www.trustnet.com/api/v1/fund/price-performance",
        "https://api.trustnet.com/v2/fund/performance",
    ]

    PERF_URL = "https://www.trustnet.com/fund/price-performance/o/ia-unit-trusts-and-oeics"

    # Possible field names Trustnet might use (map multiple candidates)
    FIELD_MAP = {
        "fund_name":  ["fundName", "FundName", "name", "Name", "fund_name"],
        "isin":       ["isin", "ISIN", "Isin"],
        "sedol":      ["sedol", "SEDOL"],
        "fund_group": ["managementGroup", "fundGroup", "FundGroup", "group"],
        "return_1m":  ["performance1M",  "return1M",  "1M",  "oneMonth",  "p1m"],
        "return_3m":  ["performance3M",  "return3M",  "3M",  "threeMonth","p3m"],
        "return_6m":  ["performance6M",  "return6M",  "6M",  "sixMonth",  "p6m", "P11GBP_D_6M"],
        "return_1y":  ["performance1Y",  "return1Y",  "1Y",  "oneYear",   "p1y"],
        "return_3y":  ["performance3Y",  "return3Y",  "3Y",  "threeYear", "p3y"],
    }

    def __init__(self):
        import requests
        cfg = load_config()
        sc = cfg.get("scraper", {})
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": sc.get("user_agent", "FundScope/1.0"),
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Referer": self.PERF_URL,
        })
        self.delay    = sc.get("request_delay_secs", 4)
        self.retries  = sc.get("max_retries", 3)
        self.retry_d  = sc.get("retry_delay_secs", 30)
        self.timeout  = sc.get("timeout_secs", 30)
        self.page_sz  = sc.get("page_size", 500)

    def _get(self, url, params=None):
        import requests
        for attempt in range(1, self.retries + 1):
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                return r
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt}/{self.retries} failed: {e}")
                if attempt < self.retries:
                    time.sleep(self.retry_d)
                else:
                    raise

    def _extract_field(self, record: dict, field: str):
        """Try multiple candidate field names, return first match."""
        for key in self.FIELD_MAP.get(field, []):
            if key in record:
                return record[key]
        return None

    def _parse_return(self, val) -> float | None:
        if val is None:
            return None
        try:
            s = str(val).replace("%", "").replace(",", "").replace("−", "-").strip()
            return None if s in ("", "-", "n/a", "N/A", "—") else float(s)
        except (ValueError, TypeError):
            return None

    def _parse_api_response(self, data, sector_code: str, week_date: date) -> list[dict]:
        """Parse Trustnet JSON API response into normalised fund dicts."""
        # API might return list directly or nested under a key
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = (data.get("data") or data.get("funds") or
                       data.get("results") or data.get("items") or [])
        else:
            return []

        results = []
        for rec in records:
            name = self._extract_field(rec, "fund_name")
            if not name or str(name).strip() in ("", "nan"):
                continue
            isin = self._extract_field(rec, "isin")
            results.append({
                "fund_id":    make_fund_id(str(name), str(isin) if isin else None),
                "fund_name":  str(name).strip(),
                "isin":       str(isin).strip() if isin else None,
                "sedol":      str(self._extract_field(rec, "sedol") or "").strip() or None,
                "fund_group": str(self._extract_field(rec, "fund_group") or "").strip() or None,
                "sector_code": sector_code,
                "week_date":  week_date.isoformat(),
                "return_1m":  self._parse_return(self._extract_field(rec, "return_1m")),
                "return_3m":  self._parse_return(self._extract_field(rec, "return_3m")),
                "return_6m":  self._parse_return(self._extract_field(rec, "return_6m")),
                "return_1y":  self._parse_return(self._extract_field(rec, "return_1y")),
                "return_3y":  self._parse_return(self._extract_field(rec, "return_3y")),
            })
        return results

    def fetch_sector_api(self, sector_code: str, week_date: date) -> list[dict]:
        """Try Trustnet's internal JSON API."""
        params = {
            "universe":  "o",
            "sector":    sector_code,
            "sortby":    "P11GBP_D_6M",
            "sortorder": "desc",
            "pageSize":  self.page_sz,
            "norisk":    "true",
        }
        for api_url in self.API_CANDIDATES:
            try:
                resp = self._get(api_url, params=params)
                if "application/json" in resp.headers.get("Content-Type", ""):
                    data = resp.json()
                    results = self._parse_api_response(data, sector_code, week_date)
                    if results:
                        logger.info(f"  ✓ API: {sector_code} → {len(results)} funds")
                        time.sleep(self.delay)
                        return results
            except Exception as e:
                logger.debug(f"  API {api_url} failed: {e}")
        raise ValueError(f"No working API endpoint found for {sector_code}")

    def fetch_sector_html(self, sector_code: str, week_date: date) -> list[dict]:
        """Fallback: parse Trustnet HTML performance table."""
        import pandas as pd
        from bs4 import BeautifulSoup

        params = {
            "norisk":    "true",
            "sector":    sector_code,
            "sortby":    "P11GBP_D_6M",
            "sortorder": "desc",
            "PageSize":  self.page_sz,
        }
        resp = self._get(self.PERF_URL, params=params)
        time.sleep(self.delay)

        soup = BeautifulSoup(resp.text, "lxml")
        tables = soup.find_all("table")
        best_df, best_n = None, 0
        for tbl in tables:
            try:
                dfs = pd.read_html(str(tbl))
                if dfs and len(dfs[0]) > best_n:
                    best_df, best_n = dfs[0], len(dfs[0])
            except Exception:
                continue

        if best_df is None or best_n == 0:
            raise ValueError(f"No parseable table found for {sector_code}")

        # Try to map columns
        col_patterns = {
            "fund_name":  ["Fund", "Name"],
            "isin":       ["ISIN"],
            "return_1m":  ["1 m", "1m", "1 Month"],
            "return_3m":  ["3 m", "3m", "3 Month"],
            "return_6m":  ["6 m", "6m", "6 Month"],
            "return_1y":  ["1 y", "1y", "1 Year"],
        }
        def find_col(df, patterns):
            for p in patterns:
                for c in df.columns:
                    if p.lower() in str(c).lower():
                        return c
            return None

        results = []
        name_col = find_col(best_df, col_patterns["fund_name"])
        if not name_col:
            raise ValueError(f"Cannot find fund name column in HTML table. Cols: {list(best_df.columns)}")

        for _, row in best_df.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name or name.lower() in ("nan", "name", "fund"):
                continue
            isin_col = find_col(best_df, col_patterns["isin"])
            isin = str(row.get(isin_col, "")).strip() if isin_col else None
            results.append({
                "fund_id":    make_fund_id(name, isin),
                "fund_name":  name,
                "isin":       isin if isin and isin.lower() != "nan" else None,
                "sedol":      None,
                "fund_group": None,
                "sector_code": sector_code,
                "week_date":  week_date.isoformat(),
                "return_1m":  self._parse_return(row.get(find_col(best_df, col_patterns["return_1m"]))),
                "return_3m":  self._parse_return(row.get(find_col(best_df, col_patterns["return_3m"]))),
                "return_6m":  self._parse_return(row.get(find_col(best_df, col_patterns["return_6m"]))),
                "return_1y":  self._parse_return(row.get(find_col(best_df, col_patterns["return_1y"]))),
                "return_3y":  None,
            })
        logger.info(f"  ✓ HTML: {sector_code} → {len(results)} funds")
        return results

    def fetch_sector(self, sector_code: str, week_date: date) -> list[dict]:
        """Try API first, fall back to HTML scraping."""
        try:
            return self.fetch_sector_api(sector_code, week_date)
        except Exception as e:
            logger.warning(f"API failed ({e}), trying HTML scrape…")
            return self.fetch_sector_html(sector_code, week_date)

    def fetch_monitored_sectors(self, week_date: date = None) -> tuple[dict, dict]:
        """Fetch all monitored sectors. Returns (results, errors)."""
        cfg = load_config()
        if week_date is None:
            week_date = date.today()
        monitored = [s for s in cfg["sectors"] if s.get("monitored")]
        results, errors = {}, {}
        for s in monitored:
            try:
                data = self.fetch_sector(s["code"], week_date)
                results[s["code"]] = data
            except Exception as e:
                logger.error(f"FAILED {s['name']}: {e}")
                errors[s["code"]] = str(e)
        return results, errors


# ── Realistic mock data for IA Unit Trusts & OEICs ───────────────────────────

IA_FUND_NAMES = {
    "IA UK All Companies": [
        "Liontrust Special Situations","Fidelity Special Situations","Artemis UK Select",
        "Schroder Recovery","Jupiter UK Special Situations","Man GLG UK Income",
        "Invesco UK Equity High Income","JOHCM UK Equity Income","Trojan Income",
        "Evenlode Income","TB Evenlode Income","Royal London UK Equity Income",
        "Threadneedle UK","Schroders UK Opportunities","Rathbone UK Opportunities",
        "Ninety One UK Alpha","Aviva Investors UK Listed Equity","abrdn UK Opportunities",
        "BNY Mellon UK Income","Dimensional UK Core Equity","L&G UK Index","Vanguard FTSE UK All Share",
        "iShares UK Equity Index","HSBC FTSE All Share Index","Fidelity Index UK",
    ],
    "IA UK Equity Income": [
        "City of London Equity","Murray Income","Edinburgh Investment","Law Debenture",
        "Trojan Income","Evenlode Income","Royal London UK Equity Income","Man GLG UK Income",
        "Schroder Income","Invesco UK Equity High Income","JOHCM UK Equity Income",
        "Finsbury Growth & Income","Perpetual Income & Growth","Henderson UK Equity Income",
        "Artemis Income","M&G Dividend","Standard Life UK Equity Income Unconstrained",
        "Dimensional UK Targeted Value","Vanguard FTSE UK Equity Income Index","iShares UK Dividend",
    ],
    "IA Global": [
        "Fundsmith Equity","Baillie Gifford Global Discovery","Scottish Mortgage (OEIC)",
        "Rathbone Global Opportunities","Morgan Stanley Global Brands","Artemis Global Income",
        "Ninety One Global Special Situations","Liontrust Global Growth","Fidelity Global Focus",
        "Stewart Investors Global Emerging Markets Leaders","Brown Advisory Global Leaders",
        "Polar Capital Global Technology","Allianz Global Equity","Vanguard Global Stock Index",
        "HSBC Global Strategy Dynamic","Dimensional Global Core Equity","L&G Global 100 Index",
        "iShares Global Equity Index","Fidelity World Index","BlackRock Global Equity",
        "Invesco Global Focus","Jupiter Global Value Equity","T. Rowe Price Global Growth",
        "Comgest Growth World","Guardcap Global Equity","Veritas Global Real Return",
        "Blue Whale Growth","GQG Partners Global Equity","Nomura Global High Conviction",
        "WS Montanaro Global Select","Trojan Global Income","Schroder QEP Global Active Value",
    ],
    "IA Global Equity Income": [
        "Artemis Global Income","Murray International","JPMorgan Global Equity Income",
        "Fidelity Global Dividend","Guinness Global Equity Income","M&G Global Dividend",
        "Newton Global Income","Investec Global Quality Equity Income","Evenlode Global Income",
        "Royal London Global Equity Income","Schroder Global Equity Income",
        "Dimensional Global Targeted Value","Vanguard FTSE All-World High Dividend Yield",
        "WisdomTree Global Quality Dividend Growth","WS Canaccord Genuity Global Eq Income",
    ],
    "IA Global Emerging Markets": [
        "Stewart Investors Global Emerging Markets Leaders","Fidelity Emerging Markets",
        "GQG Partners Emerging Markets Equity","Ninety One Global Special Situations",
        "Genesis Emerging Markets","Aubrey Global Emerging Markets Opportunities",
        "Mobius Emerging Markets","Schroder Global Emerging Markets","JPMorgan Emerging Markets",
        "HSBC Global Emerging Markets","Vanguard Emerging Markets Stock Index","abrdn Emerging Markets",
        "BlackRock Emerging Markets","Dimensional Emerging Markets Core Equity",
        "Comgest Growth Emerging Markets","Coronation Global Emerging Markets",
        "Somerset Emerging Markets Dividend Growth","GS Emerging Markets CORE Equity",
        "RWC Global Emerging Markets","Fundsmith Emerging Equities Trust (OEIC)",
    ],
    "IA UK Smaller Companies": [
        "Liontrust UK Micro Cap","Marlborough Multi Cap Income","Slater Growth",
        "Octopus UK Micro Cap Growth","Miton UK Multi Cap Income","abrdn UK Smaller Companies",
        "Gresham House UK Multi Cap Income","Unicorn UK Income","Threadneedle UK Smaller Companies",
        "FTF Martin Currie UK Smaller Cos","Vanguard FTSE UK All Share Index",
        "Henderson Smaller Companies (OEIC)","Dimension UK Small Companies",
        "River and Mercantile UK Dynamic Equity","Schroder UK Smaller Companies",
        "TB Amati UK Smaller Companies","Canaccord Genuity UK Smaller Companies",
        "Gresham House UK Smaller Cos","WS Canaccord Genuity UK Smaler Cos",
        "Chelverton UK Equity Income","Chelverton UK Equity Growth",
    ],
    "IA North America": [
        "Baillie Gifford American","Vanguard US Equity Index","HSBC American Index",
        "Fidelity Index US","L&G US Index","iShares US Equity Index",
        "Royal London US Growth","Brown Advisory US Equity Growth","Natixis Loomis Sayles US Equity Leaders",
        "T. Rowe Price US Large Cap Growth Equity","Alger American Asset Growth",
        "Legg Mason ClearBridge US Aggressive Growth","JPMorgan US Select","Artemis US Select",
        "Threadneedle American","Schroder US Mid Cap","Dimensional US Core Equity",
        "Neuberger Berman US Multi Cap Opportunities","Polen Capital Focus US Growth",
        "Guinness US Equity Income","Brown Advisory US Sustainable Growth",
        "Gabelli US Fundamental Value","Findlay Park American","Hermes US SMID Equity",
    ],
    "IA Flexible Investment": [
        "Personal Assets Trust (OEIC)","Capital Gearing (OEIC)","Trojan",
        "Ruffer Total Return","VT Tatton Global Adventurous","Premier Miton Diversified Growth",
        "Jupiter Merlin Growth Portfolio","Schroder Multi-Asset Total Return",
        "Invesco Distribution","Man GLG Dynamic Income","Waverton Multi-Asset Income",
        "BNY Mellon Multi-Asset Balanced","Artemis Monthly Distribution","VT AJ Bell Adventurous",
        "WS Canaccord Genuity Balanced","Vanguard LifeStrategy 80% Equity",
        "HSBC Global Strategy Balanced","Dimensional Global Allocation",
    ],
    "IA Mixed Investment 40-85% Shares": [
        "Vanguard LifeStrategy 60% Equity","HSBC Global Strategy Balanced",
        "Fidelity Multi Asset Allocator Balanced","L&G Multi-Index 5","Dimensional 60-40 Global",
        "Royal London Sustainable Diversified","Schroder MM Diversity","BNY Mellon Multi-Asset Growth",
        "Jupiter Merlin Balanced Portfolio","Invesco Global Targeted Returns",
        "Rathbone Strategic Growth Portfolio","Premier Miton Diversified Balance",
        "WS Canaccord Genuity Balanced","Baillie Gifford Managed","Aviva Investors Multi-Asset Core 5",
        "AXA Framlington Managed Balanced","MI Downing Fox Balanced",
        "Margetts Aries Strategy","Hargreaves Lansdown Multi-Manager Balanced Managed",
    ],
    "IA Sterling Strategic Bond": [
        "Artemis Strategic Bond","M&G Strategic Corporate Bond","Royal London Strategic Bond",
        "TwentyFour Dynamic Bond","Rathbone Ethical Bond","Jupiter Strategic Bond",
        "Invesco Tactical Bond","Baillie Gifford Strategic Bond","Schroder Strategic Credit",
        "Henderson Strategic Bond","Liontrust Monthly Income Bond","Vanguard UK Government Bond Index",
        "iShares Corporate Bond Index","HSBC Sterling Bond","Fidelity Strategic Bond",
        "Aviva Investors Strategic Bond","Axa Framlington Strategic Bond",
        "GAM Star Credit Opportunities","Kames Strategic Bond",
    ],
    "IA Infrastructure": [
        "First Sentier Global Listed Infrastructure","Legg Mason IF Rare Infrastructure Income",
        "ATLAS Infrastructure","RARE Infrastructure Income","Vanguard FTSE All-World",
        "iShares Global Infrastructure","L&G Global Infrastructure Index",
        "BlackRock Natural Resources Growth & Income","Schroder Global Energy Transition",
        "Lazard Global Listed Infrastructure Equity","Cohen & Steers Global Listed Infrastructure",
    ],
}

def generate_mock_data(sector_code: str, week_date: date, prev_data: list[dict] = None) -> list[dict]:
    """
    Generate realistic IA Unit Trust mock data.
    If prev_data is given, applies a small weekly drift for realism.
    """
    rng = random.Random(hash(sector_code + week_date.isoformat()) % 999983)

    # Get fund names for this sector, fall back to generic
    names = IA_FUND_NAMES.get(sector_code, [])
    if not names:
        # Generic names for sectors without templates
        n = 18
        names = [f"{sector_code.split('IA ')[-1]} Fund {i+1}" for i in range(n)]

    prev_map = {p["fund_name"]: p for p in (prev_data or [])}

    results = []
    for name in names:
        prev = prev_map.get(name, {})
        # Drift from previous week or generate fresh
        def drift(old, mu, sigma):
            if old is not None:
                return round(old + rng.gauss(0, sigma * 0.25), 2)
            return round(rng.gauss(mu, sigma), 2)

        isin = f"GB{abs(hash(name)) % 10**10:010d}"
        r6m = drift(prev.get("return_6m"), 7.0, 11.0)
        r3m = drift(prev.get("return_3m"), 3.2, 6.0)
        r1m = drift(prev.get("return_1m"), 0.9, 3.5)
        r1y = drift(prev.get("return_1y"), 12.0, 14.0)

        results.append({
            "fund_id":    make_fund_id(name, isin),
            "fund_name":  name,
            "isin":       isin,
            "sedol":      None,
            "fund_group": name.split()[0],
            "sector_code": sector_code,
            "week_date":  week_date.isoformat(),
            "return_1m":  r1m,
            "return_3m":  r3m,
            "return_6m":  r6m,
            "return_1y":  r1y,
            "return_3y":  drift(None, 22.0, 18.0),
        })

    return results
