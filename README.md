# de-demo

Demo kelas mata kuliah **Data Engineering** (Program Studi Teknik Informatika STMIK Tazkia,
pengampu Hendri Karisma). Setiap demo punya UI/visualisasi. Potongan kode buku ajar ada di repo
terpisah, [`sample-code-buku`](https://github.com/situkangsayur/sample-code-buku).

Inti repo ini adalah **platform end-to-end** yang juga menjadi contoh proyek UTS: aplikasi
operasional → event ke NATS (dari aplikasi dan dari CDC Debezium) → Airflow menjalankan pipeline ELT
dan ETL → data lake Iceberg di RustFS dan data warehouse ClickHouse → dashboard yang membaca keduanya.
Lab per pertemuan (P01–P07) memakai data dari platform yang sama.

Konvensi: [`docs/KONVENSI.md`](docs/KONVENSI.md). Tahap 1: sampai UTS.
