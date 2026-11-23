import "./globals.css";
import type { Metadata } from "next";
import Nav from "@/components/Nav";

export const metadata: Metadata = {
  title: "Lab DE Bab 2",
  description: "Pemodelan data (OLTP, star schema, SCD) dan format file (CSV, JSON, Avro, Parquet)",
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
