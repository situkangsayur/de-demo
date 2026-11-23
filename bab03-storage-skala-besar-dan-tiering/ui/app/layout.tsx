import "./globals.css";
import type { Metadata } from "next";
import Nav from "@/components/Nav";

export const metadata: Metadata = {
  title: "Lab DE Bab 3",
  description: "Object storage, partition, small files, kompresi, durabilitas, tiering dan cost di RustFS",
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
