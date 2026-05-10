import os
from pathlib import Path


def load_local_env(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


load_local_env()

APP_VERSION = "0.6.0"

MARKETS = ("KOSPI", "KOSDAQ")

LOOKBACK_TRADING_DAYS = 60
AVG_TRADING_VALUE_COLUMN = f"avg_trading_value_{LOOKBACK_TRADING_DAYS}d"
AVG_TRADING_VALUE_EOK_COLUMN = f"{AVG_TRADING_VALUE_COLUMN}_eok"
TOP_N = 10
NEWS_MAX_ITEMS_DEFAULT = 30
SWING_TOP_N = 20
SWING_NEWS_MAX_ITEMS_DEFAULT = 30
SWING_NEWS_LOOKBACK_DAYS = 5
SWING_HISTORY_TRADING_DAYS = 60
SWING_BACKTEST_HISTORY_TRADING_DAYS = 80
SWING_MARKET_RISK_CACHE_PATH = Path("data/cache/swing_market_risk_flags.csv")
SPECIAL_SWING_TOP_N = 10
SPECIAL_SWING_CANDIDATE_POOL_N = 100
SPECIAL_SWING_SHORTLIST_N = 30
SPECIAL_SWING_FINAL_N = 10
SPECIAL_SWING_NEWS_MAX_ITEMS_DEFAULT = 50
SPECIAL_SWING_NEWS_LOOKBACK_DAYS = 5
SPECIAL_SWING_NEWS_CUTOFF_HOUR = 8
SPECIAL_SWING_NEWS_CUTOFF_MINUTE = 0
SPECIAL_SWING_HISTORY_TRADING_DAYS = 60

MIN_MARKET_CAP = 30_000_000_000
MIN_AVG_TRADING_VALUE_20D = 500_000_000
MAX_PER = 12.0
MAX_PBR = 1.2
MIN_ESTIMATED_ROE = 8.0

STRICT_MIN_MARKET_CAP = 50_000_000_000
STRICT_MIN_AVG_TRADING_VALUE = 1_000_000_000
STRICT_MAX_PER = 10.0
STRICT_MAX_PBR = 1.0
STRICT_MIN_ESTIMATED_ROE = 10.0

LIQUIDITY_FULL_SCORE_VALUE = 5_000_000_000
MAX_RAW_SCORE = 70.0

REQUEST_SLEEP_SECONDS = 0.2
RETRY_COUNT = 3
RETRY_SLEEP_SECONDS = 1.5

RESULT_DIR = Path("data/results")
CSV_ENCODING = "utf-8-sig"
KST_TIMEZONE = "Asia/Seoul"

SECTOR_COLUMNS = [
    "sector",
    "industry",
]

NEWS_OUTPUT_COLUMNS = [
    "news_count",
    "news_sentiment",
    "news_risk_flags",
    "news_titles",
    "news_summary",
]

NEWS_RAW_COLUMNS = [
    "code",
    "name",
    "news_rank",
    "title",
    "description",
    "link",
    "naver_link",
    "description_truncated",
    "pub_date",
    "keyword_flags",
]

DART_OUTPUT_COLUMNS = [
    "dart_corp_code",
    "dart_bsns_year",
    "revenue",
    "operating_profit",
    "net_income",
    "debt_ratio",
    "operating_margin",
]

OUTPUT_COLUMNS = [
    "date",
    "rank",
    "code",
    "name",
    "market",
    "price",
    "market_cap",
    "market_cap_eok",
    "per",
    "pbr",
    "eps",
    "bps",
    "estimated_roe",
    AVG_TRADING_VALUE_COLUMN,
    AVG_TRADING_VALUE_EOK_COLUMN,
    "score",
]
