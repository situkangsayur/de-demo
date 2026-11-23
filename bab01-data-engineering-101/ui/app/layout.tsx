import "./globals.css";
import type { Metadata } from "next";
import Nav from "@/components/Nav";

export const metadata: Metadata = {
  title: "Lab DE Bab 1",
  description: "Siklus hidup data: satu transaksi ditelusuri dari aplikasi sampai dashboard",
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
