"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Empat pertanyaan"],
  ["/lineage", "Lineage ETL vs ELT"],
  ["/airflow", "Airflow"],
  ["/dq", "Data quality"],
  ["/time-travel", "Time travel"],
  ["/tiering", "Tiering & cost"],
  ["/fitur", "Fitur untuk tim AI"],
  ["/uji", "Tiga uji demo"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Bank Syariah Digital · Dashboard Direksi</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
