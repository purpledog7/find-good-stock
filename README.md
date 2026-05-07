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
python main.py --date 2026-05-04 --top-n 10
python main.py --include-summary
python main.py --strict
python main.py --min-market-cap-eok 500 --min-avg-trading-value-eok 10 --max-per 10 --max-pbr 1.0 --min-estimated-roe 10
```

`--strict`는 더 보수적인 기준이야. 시총 500억 이상, 60거래일 평균 거래대금 10억 이상, PER 10 이하, PBR 1.0 이하, 추정 ROE 10% 이상을 사용해.

## 결과

CSV는 아래 경로에 저장돼.

```text
data/results/YYYY-MM-DD_all.csv
data/results/YYYY-MM-DD_top10.csv
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

단일 기준 Top10 대신 여러 저평가 관점으로 후보를 모으려면 `advisor.py`를 실행해.

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
data/results/YYYY-MM-DD_recommend10.csv
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

기준일 전날 16:00부터 기준일 당일 07:00까지의 네이버 뉴스 검색 결과를 최종 추천 종목별 MD 파일로 저장할 수 있어.

필요한 환경변수:

```powershell
$env:NAVER_CLIENT_ID="your-naver-client-id"
$env:NAVER_CLIENT_SECRET="your-naver-client-secret"
```

실행:

```powershell
python advisor.py --include-news
```

기본값은 종목당 최신 뉴스 30개야. `--news-max-items`는 1~100개 사이로 지정할 수 있어. 추천 10개 기준 최대 300개라 네이버 검색 API 일 한도 25,000건 대비 여유가 있어.

뉴스는 CSV에 요약 컬럼으로 붙이지 않고, 원본 목록만 회사별로 묶어 MD 파일에 저장해.

```text
data/results/YYYY-MM-DD_news_raw.md
```

`recommend10.csv`는 정량 데이터만 담고, `news_raw.md`에는 가져온 뉴스 제목, 네이버 설명, 링크, 발행 시간이 요약 없이 저장돼. Codex 리뷰 프롬프트에도 이 원본 뉴스 MD 경로를 넣어둬.

기간을 직접 지정할 수도 있어.

```powershell
python advisor.py --include-news --news-from 2026-05-05T16:00:00+09:00 --news-to 2026-05-06T07:00:00+09:00
```

뉴스 분석은 `news_raw.md`를 Codex App에서 읽고 별도로 진행해. 프로그램은 분석하지 않고 데이터 준비까지만 해.

## 스윙 후보 데이터 준비

3~4일 안에 움직일 후보를 별도로 찾으려면 `swing.py`를 실행해.

```powershell
python swing.py --include-news
```

스윙 후보는 가치주 필터가 아니라 아래 4개 엔진으로 검사해.

```text
event_pivot, vcp_squeeze, darvas_breakout, pullback_ladder
```

최소 1개 엔진에 실제로 매칭된 종목만 후보로 남겨.

기본값:

```text
후보 Top30
뉴스 기간: 진입 예정일 2일 전 00:00 ~ 진입 예정일 07:30
뉴스 개수: 종목당 최신 50개, 옵션 범위 1~100개
진입 기준가: 시세 기준 거래일 종가
```

결과 파일:

```text
data/results/YYYY-MM-DD_swing_candidates.csv
data/results/YYYY-MM-DD_swing_news_raw.md
data/results/YYYY-MM-DD_swing_review_prompt.md
```

`swing_candidates.csv`에는 -4%, -8%, -10% 물타기 가격과 +4%, +7% 익절 가격을 같이 저장해. 이 가격들은 KRX 호가단위에 맞춰서 매수 가격은 아래 호가, 익절 가격은 위 호가로 정리돼. `swing_news_raw.md`는 뉴스 요약 없이 회사별 원문 목록만 저장하고, AI 분석은 `swing_review_prompt.md`를 Codex App에서 읽어서 진행하면 돼.

스윙 후보 CSV에는 엔진별 점수도 같이 들어가.

```text
event_pivot_score, volume_breakout_score, contraction_score,
darvas_breakout_score, pullback_ladder_score,
relative_strength_score, risk_penalty
```

뉴스 검색은 종목명 단독뿐 아니라 `종목명 주식`, `종목명 공시`, `종목명 계약`, `종목명 실적` 쿼리도 같이 사용해서 촉매성 뉴스를 더 넓게 모아.

시장경보/관리종목성 리스크를 수동으로 반영하려면 아래 파일을 만들면 돼.

```text
data/cache/swing_market_risk_flags.csv
```

형식:

```text
code,risk_flags,exclude_swing
000000,investment_warning,true
```

간이 백테스트까지 같이 만들려면:

```powershell
python swing.py --include-news --include-backtest
```

추가 결과:

```text
data/results/YYYY-MM-DD_swing_backtest.csv
```

## v1 기준

- ROE는 `EPS / BPS * 100`으로 계산한 추정값을 사용해.
- 영업이익, 순이익, 부채비율은 `--include-dart` 옵션을 사용할 때 상위 결과에만 보강해.
- 기본 결과는 계산 데이터만 저장해. `--include-summary`를 쓰면 상위 결과에 규칙 기반 `summary` 컬럼을 추가해.
