"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Peta siklus hidup"],
  ["/dashboard", "Dashboard, hot path vs cold path"],
  ["/peran", "Peran dan tier"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab DE Bab 1 · Data Engineering 101</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
