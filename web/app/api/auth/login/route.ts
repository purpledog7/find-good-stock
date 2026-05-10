import { NextResponse } from "next/server";

import { createSessionToken, getSitePassword, SESSION_COOKIE, sessionCookieOptions } from "@/app/lib/auth";

export async function POST(request: Request) {
  const body = (await request.json().catch(() => null)) as { password?: string } | null;
  if (!body?.password || body.password !== getSitePassword()) {
    return NextResponse.json({ ok: false }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set(SESSION_COOKIE, createSessionToken(), sessionCookieOptions());
  return response;
}
