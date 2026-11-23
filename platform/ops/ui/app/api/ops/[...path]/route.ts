// Proxy server-side dari UI ke ops-api (alamat internal dibaca saat runtime, bukan saat build).
// Mendukung Server-Sent Events: body upstream diteruskan sebagai stream.
import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

const API = process.env.OPS_API_URL ?? "http://ops-api:8080";

async function proxy(req: NextRequest, path: string[]) {
  const url = new URL(req.url);
  const target = `${API}/api/${path.map(encodeURIComponent).join("/")}${url.search}`;
  const live = path.join("/") === "flow/live";
  try {
    const res = await fetch(target, {
      method: req.method,
      headers: { "content-type": "application/json" },
      body: req.method === "GET" ? undefined : await req.text(),
      cache: "no-store",
      signal: live ? req.signal : AbortSignal.timeout(15000),
    });
    const headers = new Headers({ "content-type": res.headers.get("content-type") ?? "application/json" });
    if (live) {
      headers.set("cache-control", "no-cache");
      headers.set("x-accel-buffering", "no");
    }
    return new Response(res.body, { status: res.status, headers });
  } catch (e) {
    return Response.json({ error: `ops-api tidak terjangkau: ${String(e)}` }, { status: 502 });
  }
}

export async function GET(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  return proxy(req, (await ctx.params).path);
}

export async function POST(req: NextRequest, ctx: { params: Promise<{ path: string[] }> }) {
  return proxy(req, (await ctx.params).path);
}
