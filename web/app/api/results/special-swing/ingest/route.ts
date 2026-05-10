import { NextResponse } from "next/server";

import { upsertSpecialSwingResult } from "@/app/lib/blob-store";
import type { SpecialSwingPick, SpecialSwingResult } from "@/app/lib/types";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const authHeader = request.headers.get("authorization") || "";
  const expected = process.env.INGEST_SECRET;
  if (!expected || authHeader !== `Bearer ${expected}`) {
    return NextResponse.json({ ok: false, error: "unauthorized" }, { status: 401 });
  }

  const body = (await request.json().catch(() => null)) as Partial<SpecialSwingResult> | null;
  const validationError = validatePayload(body);
  if (validationError) {
    return NextResponse.json({ ok: false, error: validationError }, { status: 400 });
  }

  const id = sanitizeId(body!.id || `${body!.signal_date}-${body!.generated_at}`);
  const baseUrl = new URL(request.url).origin;
  const result: SpecialSwingResult = {
    detail_markdown: body!.detail_markdown || "",
    files: body!.files || {},
    generated_at: body!.generated_at!,
    id,
    signal_date: body!.signal_date!,
    telegram_brief: body!.telegram_brief || "",
    title: body!.title || `${body!.signal_date} 스페셜스윙 Top10`,
    top10: normalizeTop10(body!.top10 || []),
    top5: normalizeTop10(body!.top10 || []).slice(0, 5),
    type: "special_swing",
    url: `${baseUrl}/special-swing/${id}`
  };

  await upsertSpecialSwingResult(result);
  return NextResponse.json({ ok: true, id, url: result.url });
}

function validatePayload(body: Partial<SpecialSwingResult> | null) {
  if (!body) {
    return "missing body";
  }
  if (body.type !== "special_swing") {
    return "type must be special_swing";
  }
  if (!body.signal_date) {
    return "signal_date is required";
  }
  if (!body.generated_at) {
    return "generated_at is required";
  }
  if (!Array.isArray(body.top10) || body.top10.length === 0) {
    return "top10 is required";
  }
  return "";
}

function normalizeTop10(rows: SpecialSwingPick[]) {
  return rows.slice(0, 10).map((row, index) => ({
    ...row,
    code: String(row.code || "").padStart(6, "0"),
    final_rank: Number(row.final_rank || index + 1),
    name: row.name || "종목명 없음"
  }));
}

function sanitizeId(value: string) {
  return value
    .replace(/[:.]/g, "")
    .replace(/[^a-zA-Z0-9_-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}
