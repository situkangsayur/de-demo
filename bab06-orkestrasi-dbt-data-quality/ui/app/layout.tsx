import "./globals.css";
import type { Metadata } from "next";
import Nav from "@/components/Nav";

export const metadata: Metadata = {
  title: "Lab P06 Orkestrasi",
  description: "Demo kelas Data Engineering P06: Airflow, dbt, dan data quality pada data bank syariah",
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
