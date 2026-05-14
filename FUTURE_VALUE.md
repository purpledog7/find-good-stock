# Future value scanner

`future_value.py`는 KRX KOSDAQ 전체 종목 중 현재가 5,000원 이하 종목을 먼저 모으고,
IT, AI, 로봇, 우주, 양자컴퓨터와 인접 미래 테마 근거가 있는 종목을 조사용 후보로 정리해.

이 기능은 투자 추천이나 자동 매매용이 아니야. ROI, PER, PBR, ROE 같은 정량 가치평가보다
업종 설명과 최근 뉴스에 나온 미래 테마 연결 근거를 넓게 모으는 데 초점을 둬.

## Run

KRX snapshot collection needs pykrx login environment variables.

```powershell
$env:KRX_ID="your-krx-id"
$env:KRX_PW="your-krx-password"
```

```powershell
python future_value.py
```

뉴스 수집에는 네이버 뉴스 API 환경변수가 필요해.

```powershell
$env:NAVER_CLIENT_ID="your-naver-client-id"
$env:NAVER_CLIENT_SECRET="your-naver-client-secret"
```

뉴스 없이 KRX/업종 정보만으로 빠르게 후보를 만들려면:

```powershell
python future_value.py --skip-news
```

## Defaults

```text
Market: KOSDAQ
Max price: 5,000 KRW
News window: recent 90 calendar days
News items: latest 30 per stock
News time budget: 600 seconds
Candidate limit: 0, no final limit
News queries: company name plus stock, AI, robot, space, quantum, semiconductor, software terms
Themes: IT/software, AI/data center, semiconductor/materials, robot/automation,
        space/aerospace, quantum/security, autonomous/mobility
```

## Result files

```text
data/results/YYYY-MM-DD_future_value_all_evaluated.csv
data/results/YYYY-MM-DD_future_value_candidates.csv
data/results/YYYY-MM-DD_future_value_candidates_by_theme.md
data/results/YYYY-MM-DD_future_value_news_raw.md
data/results/YYYY-MM-DD_future_value_news_dataset.json
data/results/YYYY-MM-DD_future_value_research_prompt.md
data/results/YYYY-MM-DD_future_value_phase2_research.csv
data/results/YYYY-MM-DD_future_value_phase2_summary.md
data/results/YYYY-MM-DD_future_value_phase2_web_raw.md
data/results/YYYY-MM-DD_future_value_phase2_ai_review_prompt.md
```

`future_value_research_prompt.md`를 Codex에서 읽으면 뉴스 링크와 데이터셋을 바탕으로
최종 조사 문서인 `future_value_summary.md`를 만들 수 있어.
`future_value_phase2_ai_review_prompt.md`는 2차 웹검색 결과를 다시 AI가 검증해서
`future_value_phase2_ai_summary.md`를 만들 때 쓰는 프롬프트야.

## Useful options

```powershell
python future_value.py --max-price 5000
python future_value.py --candidate-limit 200
python future_value.py --news-lookback-days 180 --news-max-items 50
python future_value.py --news-time-budget-seconds 0
python future_value.py --skip-sector
python future_value.py --include-phase2-research --phase2-top-n 30
python future_value.py --include-phase2-research --phase2-top-n 0 --phase2-web-max-items 10
python future_value.py --include-phase2-research --phase2-include-dart
```

## Phase 2 research

`--include-phase2-research`를 켜면 1차 후보를 다시 네이버 웹문서로 검색해서 사원수, 직원수, 매출액, 연매출, 회사소개, IR, 사업보고서, 중요 뉴스 단서를 모아.

기본값은 1차 상위 30개만 조사해. 전체 후보를 조사하려면 `--phase2-top-n 0`을 사용해.

Phase 2 매출은 기본적으로 네이버 웹문서 검색 스니펫에서 추출한 후보값이야. `DART_API_KEY`가 있고 `--phase2-include-dart`를 켜면 OpenDART 연매출을 우선 사용해.

`DART_API_KEY`는 OpenDART 사이트(`https://opendart.fss.or.kr/`)에서 회원가입/로그인 후 인증키를 신청해서 발급받아.
발급 후 `.env`에 `DART_API_KEY=발급받은키` 형태로 넣거나 PowerShell에서 `$env:DART_API_KEY="발급받은키"`로 설정하면 돼.

사원수와 웹 매출은 검색 결과 미리보기에서 뽑은 값이라 틀릴 수 있어. 정확한 확인이 안 되면 `unknown`으로 남기고, 원본 링크를 함께 저장해.
