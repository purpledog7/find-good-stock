# 국내 저평가 주식 탐색 MVP

KOSPI와 KOSDAQ 전체 종목을 대상으로 저PER, 저PBR, 추정 ROE, 유동성 조건을 적용하고 CSV로 저장하는 Python 프로그램이야.

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
python main.py --skip-summary
```

## 결과

CSV는 아래 경로에 저장돼.

```text
data/results/YYYY-MM-DD_all.csv
data/results/YYYY-MM-DD_top20.csv
```

## v1 기준

- ROE는 `EPS / BPS * 100`으로 계산한 추정값을 사용해.
- 영업이익, 순이익, 부채비율은 OpenDART 같은 별도 재무제표 출처가 필요해서 v1에서는 제외했어.
- `ai_summary`는 외부 AI API가 아니라 규칙 기반 문장으로 생성해.
