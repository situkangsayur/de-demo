import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Aliran Data · de-platform",
  description: "Aliran event aplikasi dan CDC lewat NATS JetStream ke bronze Parquet di RustFS",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="id">
      <body>
        <header className="top">
          <div className="brand">
            <b>de-platform</b> <span className="muted">· ops: ingestion</span>
          </div>
          <nav>
            <a href="/">Aliran data</a>
            <a href="/generator">Generator lalu lintas</a>
          </nav>
        </header>
        <main>{children}</main>
      </body>
    </html>
  );
}
