"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const MENU = [
  ["/", "Object storage"],
  ["/partisi", "Partition"],
  ["/file-kecil", "Small files"],
  ["/kompresi", "Kompresi"],
  ["/durabilitas", "Replication vs EC"],
  ["/tiering", "Tiering dan cost"],
];

export default function Nav() {
  const p = usePathname();
  return (
    <header className="atas">
      <b>Lab DE Bab 3 · Storage skala besar dan tiering</b>
      <nav>
        {MENU.map(([href, label]) => (
          <Link key={href} href={href} className={p === href ? "aktif" : ""}>{label}</Link>
        ))}
      </nav>
    </header>
  );
}
