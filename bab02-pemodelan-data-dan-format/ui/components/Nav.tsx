"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "OLTP vs star schema"],
  ["/scd", "SCD tipe 1 vs 2"],
  ["/format", "Format file"],
  ["/parquet", "Di dalam Parquet"],
  ["/evolusi", "Schema evolution"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab DE Bab 2 · Pemodelan data dan format</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
