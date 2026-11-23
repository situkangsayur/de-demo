// Proxy dokumentasi dbt statis (index.html, manifest.json, catalog.json) dari API Go.
import { NextRequest } from "next/server";

export const dynamic = "force-dynamic";

export async function GET(_req: NextRequest, ctx: { params: Promise<{ path?: string[] }> }) {
  const { path } = await ctx.params;
  const base = process.env.API_URL || "http://api:8080";
  const p = (path && path.length ? path : ["index.html"]).map(encodeURIComponent).join("/");
  try {
    const r = await fetch(`${base}/dbt-docs/${p}`, { cache: "no-store" });
    const tipe = r.headers.get("Content-Type") || "application/octet-stream";
    return new Response(await r.arrayBuffer(), { status: r.status, headers: { "Content-Type": tipe } });
  } catch (e) {
    return new Response(`API tidak terjangkau: ${String(e)}`, { status: 502 });
  }
}
