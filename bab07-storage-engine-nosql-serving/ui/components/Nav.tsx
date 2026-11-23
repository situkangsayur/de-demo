"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Benchmark pola akses"],
  ["/engine", "B-tree vs LSM"],
  ["/kolom", "Row vs columnar"],
  ["/fitur", "Serving fitur_nasabah"],
  ["/search", "Search (OpenSearch)"],
  ["/ledger", "Hash chain"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab P07 · Storage engine, NoSQL, serving</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
