from pathlib import Path

MARKETS = ("KOSPI", "KOSDAQ")

LOOKBACK_TRADING_DAYS = 20
TOP_N = 20

MIN_MARKET_CAP = 30_000_000_000
MIN_AVG_TRADING_VALUE_20D = 500_000_000
MAX_PER = 12.0
MAX_PBR = 1.2
MIN_ESTIMATED_ROE = 8.0

LIQUIDITY_FULL_SCORE_VALUE = 5_000_000_000
MAX_RAW_SCORE = 70.0

REQUEST_SLEEP_SECONDS = 0.2
RETRY_COUNT = 3
RETRY_SLEEP_SECONDS = 1.5

RESULT_DIR = Path("data/results")
CSV_ENCODING = "utf-8-sig"

OUTPUT_COLUMNS = [
    "date",
    "code",
    "name",
    "market",
    "price",
    "market_cap",
    "per",
    "pbr",
    "eps",
    "bps",
    "estimated_roe",
    "avg_trading_value_20d",
    "score",
    "ai_summary",
]
