import { requireSession } from "@/app/lib/auth";

export default async function PlaceholderPage() {
  await requireSession();
  return (
    <main className="page">
      <h1 className="page-title">준비중</h1>
      <div className="placeholder-grid">
        <section className="placeholder">
          <h2>1일단타스윙</h2>
          <p className="page-copy">오전 매수, 오후 매도 기준 기능을 나중에 붙일 자리야.</p>
        </section>
        <section className="placeholder">
          <h2>오늘의 주식시황</h2>
          <p className="page-copy">시장 요약과 수급 브리핑을 나중에 붙일 자리야.</p>
        </section>
      </div>
    </main>
  );
}
