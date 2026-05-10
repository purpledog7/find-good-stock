import Link from "next/link";

import { requireSession } from "@/app/lib/auth";
import { readSpecialSwingIndex } from "@/app/lib/blob-store";

export const runtime = "nodejs";

export default async function SpecialSwingPage() {
  await requireSession();
  const index = await readSpecialSwingIndex();
  const latest = index.items[0];

  return (
    <main className="page">
      <section className="page-header">
        <div>
          <h1 className="page-title">스페셜스윙 결과</h1>
          <p className="page-copy">날짜와 실행 시간별로 저장된 스페셜스윙 Top10 브리핑을 최신순으로 보여줘.</p>
        </div>
        {latest ? (
          <Link className="button" href={`/special-swing/${latest.id}`}>
            최신 결과 보기
          </Link>
        ) : null}
      </section>

      <section className="stat-row">
        <div className="stat">
          <div className="stat-label">저장된 결과</div>
          <div className="stat-value">{index.items.length}</div>
        </div>
        <div className="stat">
          <div className="stat-label">최신 기준일</div>
          <div className="stat-value">{latest?.signal_date || "-"}</div>
        </div>
        <div className="stat">
          <div className="stat-label">마지막 저장</div>
          <div className="stat-value">{latest ? formatDateTime(latest.generated_at) : "-"}</div>
        </div>
      </section>

      <section className="result-list">
        {index.items.length === 0 ? (
          <div className="empty">아직 저장된 스페셜스윙 결과가 없어.</div>
        ) : (
          index.items.map((item) => (
            <article className="result-card" key={item.id}>
              <div className="card-head">
                <div>
                  <h2 className="card-title">{item.title}</h2>
                  <div className="card-meta">
                    기준일 {item.signal_date} · 저장 {formatDateTime(item.generated_at)}
                  </div>
                </div>
                <span className="pill">Top {item.top5.length}</span>
              </div>
              <div className="top-picks">
                {item.top5.map((pick) => (
                  <span className="pick" key={`${item.id}-${pick.code}`}>
                    {pick.final_rank}. {pick.name} {pick.leader_score ? `${pick.leader_score}점` : ""}
                  </span>
                ))}
              </div>
              <Link className="button secondary" href={`/special-swing/${item.id}`}>
                상세 보기
              </Link>
            </article>
          ))
        )}
      </section>
    </main>
  );
}

function formatDateTime(value: string) {
  return new Intl.DateTimeFormat("ko-KR", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "Asia/Seoul"
  }).format(new Date(value));
}
