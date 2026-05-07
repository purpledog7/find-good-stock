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

## 여러 프로필 추천

단일 기준 Top20 대신 여러 저평가 관점으로 후보를 모으려면 `advisor.py`를 실행해.

```powershell
python advisor.py
```

사용하는 기본 프로필:

```text
balanced, conservative, deep_value, quality_value,
liquid_value, small_cap_value, low_pbr_focus
```

결과 파일:

```text
data/results/YYYY-MM-DD_profile_candidates.csv
data/results/YYYY-MM-DD_recommend20.csv
data/results/YYYY-MM-DD_codex_review_prompt.md
```

`recommendation_score`는 여러 프로필 매칭 수, 기존 score, 유동성, 추정 ROE, 시총 안정성을 합친 추천용 점수야.
`codex_review_prompt.md`는 Codex App에서 열어 최종 후보를 설명형으로 검토할 때 쓰는 프롬프트야.
업종 정보는 FinanceDataReader의 KRX-DESC 데이터를 사용해서 기본으로 보강해.

특정 프로필만 실행할 수도 있어.

```powershell
python advisor.py --profile deep_value --profile quality_value
```

업종 보강을 건너뛰려면:

```powershell
python advisor.py --skip-sector
```

## 최근 뉴스 보강

전날 00:00부터 분석 직전까지의 네이버 뉴스 검색 결과를 최종 추천 종목에 붙일 수 있어.

필요한 환경변수:

```powershell
$env:NAVER_CLIENT_ID="your-naver-client-id"
$env:NAVER_CLIENT_SECRET="your-naver-client-secret"
```

실행:

```powershell
python advisor.py --include-news
```

기본값은 종목당 뉴스 50개 검색이야. 추천 20개 기준 최대 1,000개 검색이라 네이버 검색 API 일 한도 25,000건 대비 여유가 있어.

추가되는 컬럼:

```text
news_count, news_sentiment, news_risk_flags, news_titles, news_summary
```

뉴스를 켜면 원본 뉴스 목록도 따로 저장해.

```text
data/results/YYYY-MM-DD_news_raw.csv
```

`recommend20.csv`에는 종목별 뉴스 요약이 들어가고, `news_raw.csv`에는 가져온 뉴스 제목, 요약, 링크, 발행 시간이 전부 저장돼. Codex 리뷰 프롬프트에도 이 원본 뉴스 CSV 경로를 넣어둬.

기간을 직접 지정할 수도 있어.

```powershell
python advisor.py --include-news --news-from 2026-05-05T00:00:00+09:00 --news-to 2026-05-06T07:30:00+09:00
```

뉴스 분석은 투자 판단이 아니라 리스크 키워드 점검용이야. 유상증자, 전환사채, 적자, 소송, 거래정지 같은 키워드는 `news_risk_flags`로 표시해.

## v1 기준

- ROE는 `EPS / BPS * 100`으로 계산한 추정값을 사용해.
- 영업이익, 순이익, 부채비율은 `--include-dart` 옵션을 사용할 때 상위 결과에만 보강해.
- 기본 결과는 계산 데이터만 저장해. `--include-summary`를 쓰면 상위 결과에 규칙 기반 `summary` 컬럼을 추가해.
