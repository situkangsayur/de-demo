// Proxy ke API Go (dash-api). URL dibaca saat runtime dari env API_URL, bukan saat build.
import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

async function teruskan(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  const { path } = await ctx.params;
  const base = process.env.API_URL || "http://dash-api:8080";
  const url = `${base}/api/${path.join("/")}${req.nextUrl.search}`;
  const init: RequestInit = { method: req.method, headers: { "Content-Type": "application/json" }, cache: "no-store" };
  if (req.method !== "GET") init.body = await req.text();
  try {
    const r = await fetch(url, init);
    return new Response(await r.text(), { status: r.status, headers: { "Content-Type": "application/json" } });
  } catch (e) {
    return Response.json({ error: `API tidak terjangkau: ${String(e)}` }, { status: 502 });
  }
}

export const GET = teruskan;
export const POST = teruskan;
