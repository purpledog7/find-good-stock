import { get, put } from "@vercel/blob";
import { unstable_noStore as noStore } from "next/cache";

import type { SpecialSwingIndex, SpecialSwingResult } from "./types";

const ACCESS = "private" as const;
const INDEX_PATH = "results/special-swing/index.json";

export async function readSpecialSwingIndex(): Promise<SpecialSwingIndex> {
  noStore();
  return readJsonBlob<SpecialSwingIndex>(INDEX_PATH, { items: [], updated_at: "" });
}

export async function readSpecialSwingResult(id: string): Promise<SpecialSwingResult | null> {
  noStore();
  return readJsonBlob<SpecialSwingResult | null>(detailPath(id), null);
}

export async function upsertSpecialSwingResult(result: SpecialSwingResult) {
  const now = new Date().toISOString();
  const summary = {
    generated_at: result.generated_at,
    id: result.id,
    signal_date: result.signal_date,
    title: result.title,
    top5: result.top10.slice(0, 5),
    type: "special_swing" as const,
    url: result.url
  };

  await writeJsonBlob(detailPath(result.id), result);

  const current = await readSpecialSwingIndex();
  const items = [summary, ...current.items.filter((item) => item.id !== result.id)].sort((a, b) =>
    b.generated_at.localeCompare(a.generated_at)
  );

  await writeJsonBlob(INDEX_PATH, { items, updated_at: now });
}

async function readJsonBlob<T>(pathname: string, fallback: T): Promise<T> {
  try {
    const blob = await get(pathname, { access: ACCESS, useCache: false });
    if (!blob || blob.statusCode !== 200 || !blob.stream) {
      return fallback;
    }

    return (await new Response(blob.stream).json()) as T;
  } catch {
    return fallback;
  }
}

async function writeJsonBlob(pathname: string, payload: unknown) {
  await put(pathname, JSON.stringify(payload, null, 2), {
    access: ACCESS,
    allowOverwrite: true,
    cacheControlMaxAge: 60,
    contentType: "application/json"
  });
}

function detailPath(id: string) {
  return `results/special-swing/${id}.json`;
}
