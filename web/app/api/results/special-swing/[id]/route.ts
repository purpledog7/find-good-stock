import { NextResponse } from "next/server";

import { hasSession } from "@/app/lib/auth";
import { readSpecialSwingResult } from "@/app/lib/blob-store";

export const runtime = "nodejs";

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  if (!(await hasSession())) {
    return NextResponse.json({ ok: false, error: "unauthorized" }, { status: 401 });
  }

  const { id } = await params;
  const result = await readSpecialSwingResult(id);
  if (!result) {
    return NextResponse.json({ ok: false, error: "not found" }, { status: 404 });
  }
  return NextResponse.json(result);
}
