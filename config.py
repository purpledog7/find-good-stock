from pathlib import Path

APP_VERSION = "0.3.0"

MARKETS = ("KOSPI", "KOSDAQ")

LOOKBACK_TRADING_DAYS = 60
AVG_TRADING_VALUE_COLUMN = f"avg_trading_value_{LOOKBACK_TRADING_DAYS}d"
AVG_TRADING_VALUE_EOK_COLUMN = f"{AVG_TRADING_VALUE_COLUMN}_eok"
TOP_N = 20

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
