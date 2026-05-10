import { NextResponse } from "next/server";

import { hasSession } from "@/app/lib/auth";
import { readSpecialSwingIndex } from "@/app/lib/blob-store";

export const runtime = "nodejs";

export async function GET() {
  if (!(await hasSession())) {
    return NextResponse.json({ ok: false, error: "unauthorized" }, { status: 401 });
  }
  return NextResponse.json(await readSpecialSwingIndex());
}
