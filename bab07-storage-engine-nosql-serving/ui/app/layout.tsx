import "./globals.css";
import type { Metadata } from "next";
import Nav from "@/components/Nav";

export const metadata: Metadata = {
  title: "Lab P07 Storage Engine",
  description: "Demo kelas Data Engineering P07: storage engine, NoSQL, dan serving store pada data bank syariah",
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
