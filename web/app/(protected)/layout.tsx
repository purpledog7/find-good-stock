import Link from "next/link";
import type { ReactNode } from "react";

export default function ProtectedLayout({ children }: { children: ReactNode }) {
  return (
    <>
      <header className="topbar">
        <div className="topbar-inner">
          <Link className="brand" href="/special-swing">
            <span className="brand-title">오늘의 주식</span>
            <span className="brand-subtitle">스윙 리서치 아카이브</span>
          </Link>
          <nav className="nav" aria-label="주요 메뉴">
            <Link className="active" href="/special-swing">
              스페셜스윙
            </Link>
            <Link href="/placeholder">1일단타스윙 준비중</Link>
            <Link href="/placeholder">오늘의 주식시황 준비중</Link>
          </nav>
        </div>
      </header>
      {children}
    </>
  );
}
