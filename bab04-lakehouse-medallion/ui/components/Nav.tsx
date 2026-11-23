"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Medallion"],
  ["/lacak", "Satu transaksi, tiga lapisan"],
  ["/iceberg", "Pohon metadata Iceberg"],
  ["/time-travel", "Time travel & pruning"],
  ["/sql", "DuckDB atas lakehouse"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab P04 · Lakehouse medallion &amp; Iceberg</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
