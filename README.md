# 국내 저평가 주식 탐색 MVP

KOSPI와 KOSDAQ 전체 종목을 대상으로 저PER, 저PBR, 추정 ROE, 최근 60거래일 유동성 조건을 적용하고 CSV로 저장하는 Python 프로그램이야.

## 설치

```powershell
python -m pip install -r requirements.txt
```

최신 pykrx의 KRX 조회는 로그인 환경변수가 필요해.

```powershell
$env:KRX_ID="your-id"
$env:KRX_PW="your-password"
```

## 실행

```powershell
python main.py
```

선택 옵션:

```powershell
python main.py --date 2026-05-04 --top-n 20
python main.py --include-summary
python main.py --strict
python main.py --min-market-cap-eok 500 --min-avg-trading-value-eok 10 --max-per 10 --max-pbr 1.0 --min-estimated-roe 10
```

`--strict`는 더 보수적인 기준이야. 시총 500억 이상, 60거래일 평균 거래대금 10억 이상, PER 10 이하, PBR 1.0 이하, 추정 ROE 10% 이상을 사용해.

## 결과

CSV는 아래 경로에 저장돼.

```text
data/results/YYYY-MM-DD_all.csv
data/results/YYYY-MM-DD_top20.csv
```

`market_cap`과 `avg_trading_value_60d`는 원 단위야. CSV 보기 편하게 `market_cap_eok`, `avg_trading_value_60d_eok`도 같이 저장해.
`rank`는 점수 기준 순위야.

## OpenDART 보강

상위 결과에 연간 재무제표 항목을 추가하려면 OpenDART API 키가 필요해.

```powershell
$env:DART_API_KEY="your-dart-api-key"
python main.py --include-dart
```

추가되는 컬럼:

```text
dart_corp_code, dart_bsns_year, revenue, operating_profit,
net_income, debt_ratio, operating_margin
```

기본값은 전년도 사업보고서(`11011`)와 연결 재무제표(`CFS`)야.

```powershell
python main.py --include-dart --dart-year 2025 --dart-report-code 11011 --dart-fs-div CFS
```

## v1 기준

- ROE는 `EPS / BPS * 100`으로 계산한 추정값을 사용해.
- 영업이익, 순이익, 부채비율은 `--include-dart` 옵션을 사용할 때 상위 결과에만 보강해.
- 기본 결과는 계산 데이터만 저장해. `--include-summary`를 쓰면 상위 결과에 규칙 기반 `summary` 컬럼을 추가해.
