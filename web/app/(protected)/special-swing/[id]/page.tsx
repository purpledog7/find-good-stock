import Link from "next/link";
import { notFound } from "next/navigation";

import { requireSession } from "@/app/lib/auth";
import { readSpecialSwingResult } from "@/app/lib/blob-store";

export const runtime = "nodejs";

export default async function SpecialSwingDetailPage({ params }: { params: Promise<{ id: string }> }) {
  await requireSession();
  const { id } = await params;
  const result = await readSpecialSwingResult(id);
  if (!result) {
    notFound();
  }

  return (
    <main className="page">
      <section className="page-header">
        <div>
          <h1 className="page-title">{result.title}</h1>
          <p className="page-copy">
            기준일 {result.signal_date} · 저장 {formatDateTime(result.generated_at)}
          </p>
        </div>
        <Link className="button secondary" href="/special-swing">
          목록으로
        </Link>
      </section>

      <section className="detail-grid">
        <pre className="brief-box">{result.telegram_brief}</pre>
        <aside className="side-panel">
          <h2>Top10</h2>
          <div className="rank-list">
            {result.top10.map((pick) => (
              <div className="rank-item" key={pick.code}>
                <div className="rank-name">
                  {pick.final_rank}. {pick.name} ({pick.code})
                </div>
                <div className="rank-note">{pick.grade || pick.key_catalyst || pick.reason || "요약 없음"}</div>
              </div>
            ))}
          </div>
        </aside>
      </section>
    </main>
  );
}

function formatDateTime(value: string) {
  return new Intl.DateTimeFormat("ko-KR", {
    dateStyle: "full",
    timeStyle: "medium",
    timeZone: "Asia/Seoul"
  }).format(new Date(value));
}
