# 오늘의 주식

Vercel에 배포하는 스페셜스윙 결과 뷰어야.

## 환경변수

```text
BLOB_READ_WRITE_TOKEN=vercel_blob_rw_token
SITE_PASSWORD=test!1234
AUTH_SECRET=change-this-auth-secret
INGEST_SECRET=change-this-ingest-secret
```

Vercel Blob store는 Private으로 만들고 프로젝트에 연결해.

## 로컬 실행

```powershell
npm install
npm run dev
```

## 배포 설정

- Vercel project root: `web`
- Project name: `today-stock`
- Site display name: `오늘의 주식`

## 결과 업로드

루트 workspace `.env`에는 아래 값을 넣어.

```text
TODAY_STOCK_SITE_URL=https://your-vercel-domain.vercel.app
TODAY_STOCK_INGEST_SECRET=INGEST_SECRET와_같은_값
```

스페셜스윙 최종 결과 생성 후:

```powershell
python "C:\Users\purpl\.codex\skills\special-swing-runner\scripts\publish_site_result.py" --workspace "C:\myprojects\find-good-stock"
```
