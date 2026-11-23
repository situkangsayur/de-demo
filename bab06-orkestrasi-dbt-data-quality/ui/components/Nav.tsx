"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Orkestrasi Airflow"],
  ["/idempotensi", "Idempotency & backfill"],
  ["/lineage", "dbt lineage"],
  ["/mutu", "Data quality"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab P06 · Orkestrasi, dbt, data quality</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
        <a href="/dbt-docs/index.html" target="_blank" rel="noreferrer">dbt docs ↗</a>
      </nav>
    </header>
  );
}
