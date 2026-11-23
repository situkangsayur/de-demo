"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Benchmark engine"],
  ["/rencana", "Lazy vs eager & query plan"],
  ["/zero-copy", "Zero-copy Arrow"],
  ["/memori", "Batas memori & streaming"],
  ["/pipeline", "Pipeline bronze → gold"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab P05 · ETL satu mesin</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
