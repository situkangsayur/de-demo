import "./globals.css";
import type { Metadata } from "next";
import Nav from "@/components/Nav";

export const metadata: Metadata = {
  title: "Lab ETL Satu Mesin P05",
  description: "Demo kelas Data Engineering P05: Arrow, Polars, DuckDB, DataFusion, dan pandas pada satu mesin",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="id">
      <body>
        <Nav />
        <main>{children}</main>
      </body>
    </html>
  );
}
