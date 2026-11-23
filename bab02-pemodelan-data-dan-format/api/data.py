"""Generator data bank kecil dengan skema platform (platform/KONTRAK-DATA.md) dan tiga bentuk model:

- OLTP ternormalisasi (3NF): cabang, nasabah, rekening, transaksi. Seperti di database aplikasi, tabel
  nasabah hanya menyimpan kota *sekarang* (perubahan menimpa nilai lama).
- Star schema (Kimball): fakta_transaksi + dim_tanggal, dim_cabang, dim_kanal, dim_produk, dim_nasabah
  (SCD tipe 2, memakai surrogate key) dan dim_nasabah_scd1 untuk pembanding.
- One big table (OBT): fakta yang sudah digabung dengan semua atribut dimensi.

Semua dibangkitkan di DuckDB dengan hash() sehingga deterministik untuk N yang sama.
"""
from __future__ import annotations

import datetime as dt

import duckdb

CABANG = [(1, "JKT01", "KC Jakarta Thamrin", "jakarta pusat", "Jabodetabek"),
          (2, "JKT02", "KC Jakarta Selatan", "jakarta selatan", "Jabodetabek"),
          (3, "BGR01", "KC Bogor", "bogor", "Jabodetabek"),
          (4, "BDG01", "KC Bandung", "bandung", "Jawa Barat"),
          (5, "SBY01", "KC Surabaya", "surabaya", "Jawa Timur"),
          (6, "MDN01", "KC Medan", "medan", "Sumatra"),
          (7, "MKS01", "KC Makassar", "makassar", "Sulawesi"),
          (8, "YGY01", "KC Yogyakarta", "yogyakarta", "Jawa Tengah-DIY")]
N_NASABAH = 8000          # nasabah 7712 (contoh slide) selalu ada
NASABAH_CONTOH = 7712
PINDAH_CONTOH = dt.date(2026, 7, 1)   # Bogor -> Bandung, seperti slide "SCD tipe 2 dalam tabel"


def hari_ini() -> dt.date:
    return dt.date.today()


def bangkitkan(con: duckdb.DuckDBPyConnection, n_transaksi: int) -> dict:
    akhir = hari_ini()
    awal = akhir - dt.timedelta(days=365)
    # hash() DuckDB bertipe UBIGINT; dipersempit ke BIGINT agar aman dipakai sebagai indeks list dan interval
    con.execute("CREATE OR REPLACE MACRO h64(a, b) AS CAST(hash(a, b) % 1000000007 AS BIGINT)")
    con.execute("DROP TABLE IF EXISTS cabang; DROP TABLE IF EXISTS nasabah; DROP TABLE IF EXISTS rekening;"
                "DROP TABLE IF EXISTS transaksi; DROP TABLE IF EXISTS riwayat_kota")
    con.execute("CREATE TABLE cabang (id INTEGER PRIMARY KEY, kode VARCHAR, nama VARCHAR, kota VARCHAR, wilayah VARCHAR)")
    con.executemany("INSERT INTO cabang VALUES (?, ?, ?, ?, ?)", CABANG)
    # riwayat kota: 12% nasabah pernah pindah sekali dalam setahun terakhir
    con.execute(f"""
        CREATE TABLE riwayat_kota AS
        WITH n AS (
          SELECT i AS id,
                 1 + h64(i, 'cab') % 8 AS cabang_awal,
                 h64(i, 'pindah') % 100 < 12 AS pindah,
                 1 + h64(i, 'tujuan') % 8 AS cabang_tujuan,
                 DATE '{awal}' + CAST(30 + h64(i, 'tgl') % 300 AS INTEGER) AS tgl_pindah
          FROM range(1, {N_NASABAH} + 1) r(i))
        SELECT id,
               CASE WHEN id = {NASABAH_CONTOH} THEN 3 ELSE cabang_awal END AS cabang_awal,
               CASE WHEN id = {NASABAH_CONTOH} THEN TRUE ELSE pindah AND cabang_tujuan <> cabang_awal END AS pindah,
               CASE WHEN id = {NASABAH_CONTOH} THEN 4 ELSE cabang_tujuan END AS cabang_tujuan,
               CASE WHEN id = {NASABAH_CONTOH} THEN DATE '{PINDAH_CONTOH}' ELSE tgl_pindah END AS tgl_pindah
        FROM n""")
    con.execute(f"""
        CREATE TABLE nasabah AS
        SELECT r.id, 'CIF' || lpad(r.id::VARCHAR, 7, '0') AS cif,
               ['Ahmad','Siti','Muhammad','Nur','Rizki','Dewi','Fajar','Aisyah','Budi','Fatimah'][1 + h64(r.id,'d') % 10]
                 || ' ' || ['Pratama','Hidayat','Saputra','Lestari','Nugroho','Siregar','Wibowo','Maulana'][1 + h64(r.id,'b') % 8] AS nama,
               c.kota AS kota,      -- OLTP: hanya kota sekarang
               ['ritel','ritel','prioritas','umkm'][1 + h64(r.id,'s') % 4] AS segmen,
               r.cabang_awal AS cabang_id,
               CASE WHEN r.id = {NASABAH_CONTOH} THEN TIMESTAMP '2024-01-15 09:00:00'
                    ELSE TIMESTAMP '{awal}' - INTERVAL (h64(r.id,'c') % 900) DAY END AS created_at
        FROM riwayat_kota r JOIN cabang c ON c.id = CASE WHEN r.pindah THEN r.cabang_tujuan ELSE r.cabang_awal END""")
    con.execute("""
        CREATE TABLE rekening AS
        SELECT id, '71' || lpad(id::VARCHAR, 8, '0') AS no_rekening, id AS nasabah_id, cabang_id,
               ['tabungan_wadiah','tabungan_wadiah','giro','deposito_mudharabah'][1 + h64(id,'j') % 4] AS jenis,
               'aktif' AS status, created_at AS dibuka_at
        FROM nasabah""")
    # transaksi: kanal berbobot seperti platform (mobile 45, atm 18, teller 9, web 13, ecommerce 15)
    con.execute(f"""
        CREATE TABLE transaksi AS
        WITH t AS (
          SELECT i,
                 CASE WHEN i % 997 = 0 THEN {NASABAH_CONTOH} ELSE 1 + h64(i, 'rek') % {N_NASABAH} END AS rekening_id,
                 h64(i, 'kanal') % 100 AS k,
                 TIMESTAMP '{awal}' + INTERVAL (h64(i, 'w') % (365 * 86400)) SECOND AS occurred_at,
                 [5,10,20,25,50,75,100,150,200,250,500,1000,2500][1 + h64(i, 'nom') % 13] * 10000 AS nominal
          FROM range({n_transaksi}) r(i))
        SELECT 'TRX' || strftime(t.occurred_at, '%Y%m%d') || lpad(t.i::VARCHAR, 8, '0') AS id,
               t.rekening_id,
               CASE WHEN h64(t.i, 'lain') % 100 < 80 THEN rk.cabang_id ELSE 1 + h64(t.i, 'cb') % 8 END AS cabang_id,
               CASE WHEN t.k < 45 THEN 'mobile' WHEN t.k < 63 THEN 'atm' WHEN t.k < 72 THEN 'teller'
                    WHEN t.k < 85 THEN 'web' ELSE 'ecommerce' END AS kanal,
               CASE WHEN t.k >= 85 THEN 'bayar_belanja'
                    ELSE ['setor','tarik','transfer_masuk','transfer_keluar','angsuran'][1 + h64(t.i, 'jn') % 5] END AS jenis,
               CAST(t.nominal AS DECIMAL(18,2)) AS nominal,
               t.occurred_at,
               t.occurred_at + INTERVAL 1 SECOND AS created_at
        FROM t JOIN rekening rk ON rk.id = t.rekening_id
        ORDER BY t.occurred_at""")
    bangun_star(con)
    return ringkasan(con)


def bangun_star(con: duckdb.DuckDBPyConnection) -> None:
    for t in ["fakta_transaksi", "dim_tanggal", "dim_cabang", "dim_kanal", "dim_produk", "dim_nasabah",
              "dim_nasabah_scd1", "obt_transaksi"]:
        con.execute(f"DROP TABLE IF EXISTS {t}")
    con.execute("""
        CREATE TABLE dim_tanggal AS
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS sk_tanggal, CAST(d AS DATE) AS tanggal,
               year(d) AS tahun, quarter(d) AS kuartal, month(d) AS bulan, strftime(d, '%Y-%m') AS tahun_bulan,
               dayname(d) AS hari, isodow(d) >= 6 AS akhir_pekan
        FROM range(DATE '2023-01-01', DATE '2027-12-31', INTERVAL 1 DAY) r(d)""")
    con.execute("CREATE TABLE dim_cabang AS SELECT id AS sk_cabang, kode, nama, kota, wilayah FROM cabang")
    con.execute("""CREATE TABLE dim_kanal AS SELECT * FROM (VALUES (1,'mobile','digital'),(2,'atm','fisik'),
                   (3,'teller','fisik'),(4,'web','digital'),(5,'ecommerce','digital')) t(sk_kanal, kanal, kelompok)""")
    con.execute("""CREATE TABLE dim_produk AS SELECT * FROM (VALUES (1,'tabungan_wadiah','wadiah'),(2,'giro','wadiah'),
                   (3,'deposito_mudharabah','mudharabah')) t(sk_produk, jenis_rekening, akad)""")
    # SCD tipe 2: satu baris per versi kota, surrogate key berurutan
    con.execute("""
        CREATE TABLE dim_nasabah AS
        WITH versi AS (
          SELECT n.id AS id_nasabah, n.segmen, c0.kota, CAST(n.created_at AS DATE) AS berlaku_dari,
                 CASE WHEN r.pindah THEN r.tgl_pindah - 1 ELSE DATE '9999-12-31' END AS berlaku_sampai
          FROM nasabah n JOIN riwayat_kota r ON r.id = n.id JOIN cabang c0 ON c0.id = r.cabang_awal
          UNION ALL
          SELECT n.id, n.segmen, c1.kota, r.tgl_pindah, DATE '9999-12-31'
          FROM nasabah n JOIN riwayat_kota r ON r.id = n.id JOIN cabang c1 ON c1.id = r.cabang_tujuan
          WHERE r.pindah)
        SELECT CAST(row_number() OVER (ORDER BY berlaku_dari, id_nasabah) AS INTEGER) AS sk_nasabah, id_nasabah, segmen, kota,
               berlaku_dari, berlaku_sampai, berlaku_sampai = DATE '9999-12-31' AS aktif
        FROM versi""")
    # SCD tipe 1: satu baris per nasabah, nilai ditimpa (sama dengan isi OLTP)
    con.execute("CREATE TABLE dim_nasabah_scd1 AS SELECT id AS sk_nasabah, id AS id_nasabah, segmen, kota FROM nasabah")
    con.execute("""
        CREATE TABLE fakta_transaksi AS
        SELECT t.id AS id_transaksi,
               CAST(strftime(t.occurred_at, '%Y%m%d') AS INTEGER) AS sk_tanggal,
               dn.sk_nasabah, dp.sk_produk, t.cabang_id AS sk_cabang, dk.sk_kanal,
               t.rekening_id AS id_nasabah,     -- kunci alami, dipakai pembanding SCD tipe 1
               t.nominal
        FROM transaksi t
        JOIN rekening r ON r.id = t.rekening_id
        JOIN dim_nasabah dn ON dn.id_nasabah = r.nasabah_id
             AND CAST(t.occurred_at AS DATE) BETWEEN dn.berlaku_dari AND dn.berlaku_sampai
        JOIN dim_produk dp ON dp.jenis_rekening = r.jenis
        JOIN dim_kanal dk ON dk.kanal = t.kanal
        ORDER BY sk_tanggal""")
    con.execute("""
        CREATE TABLE obt_transaksi AS
        SELECT f.id_transaksi, d.tanggal, d.tahun_bulan, c.nama AS nama_cabang, c.wilayah, k.kanal, k.kelompok,
               p.jenis_rekening, p.akad, n.id_nasabah, n.kota AS kota_nasabah, n.segmen, f.nominal
        FROM fakta_transaksi f JOIN dim_tanggal d USING (sk_tanggal) JOIN dim_cabang c USING (sk_cabang)
        JOIN dim_kanal k USING (sk_kanal) JOIN dim_produk p USING (sk_produk) JOIN dim_nasabah n USING (sk_nasabah)""")


def ringkasan(con: duckdb.DuckDBPyConnection) -> dict:
    tabel = ["cabang", "nasabah", "rekening", "transaksi", "dim_tanggal", "dim_cabang", "dim_kanal", "dim_produk",
             "dim_nasabah", "dim_nasabah_scd1", "fakta_transaksi", "obt_transaksi"]
    hasil = {}
    for t in tabel:
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        kol = [r[0] for r in con.execute(f"DESCRIBE {t}").fetchall()]
        hasil[t] = {"baris": n, "kolom": kol}
    return hasil


# Pertanyaan analitik yang sama ditulis untuk tiga model. q1: tidak terpengaruh SCD; q2: kota nasabah.
PERTANYAAN = {
    "q1": {
        "judul": "Total nominal per bulan, wilayah cabang, dan kanal (tahun berjalan)",
        "oltp": """SELECT strftime(t.occurred_at, '%Y-%m') AS bulan, c.wilayah, t.kanal,
       SUM(t.nominal) AS total, COUNT(*) AS jumlah
FROM transaksi t
JOIN cabang c ON c.id = t.cabang_id
WHERE year(t.occurred_at) = year(current_date)
GROUP BY ALL ORDER BY ALL""",
        "star": """SELECT d.tahun_bulan AS bulan, c.wilayah, k.kanal,
       SUM(f.nominal) AS total, COUNT(*) AS jumlah
FROM fakta_transaksi f
JOIN dim_tanggal d ON d.sk_tanggal = f.sk_tanggal
JOIN dim_cabang  c ON c.sk_cabang  = f.sk_cabang
JOIN dim_kanal   k ON k.sk_kanal   = f.sk_kanal
WHERE d.tahun = year(current_date)
GROUP BY ALL ORDER BY ALL""",
        "obt": """SELECT tahun_bulan AS bulan, wilayah, kanal,
       SUM(nominal) AS total, COUNT(*) AS jumlah
FROM obt_transaksi
WHERE year(tanggal) = year(current_date)
GROUP BY ALL ORDER BY ALL""",
    },
    "q2": {
        "judul": "Total nominal per kota nasabah, segmen, dan jenis akad (setahun terakhir)",
        "oltp": """SELECT n.kota AS kota_nasabah, n.segmen,
       CASE WHEN r.jenis LIKE '%mudharabah' THEN 'mudharabah' ELSE 'wadiah' END AS akad,
       SUM(t.nominal) AS total, COUNT(*) AS jumlah
FROM transaksi t
JOIN rekening r ON r.id = t.rekening_id
JOIN nasabah  n ON n.id = r.nasabah_id
GROUP BY ALL ORDER BY ALL""",
        "star": """SELECT n.kota AS kota_nasabah, n.segmen, p.akad,
       SUM(f.nominal) AS total, COUNT(*) AS jumlah
FROM fakta_transaksi f
JOIN dim_nasabah n ON n.sk_nasabah = f.sk_nasabah   -- SCD 2: kota saat transaksi
JOIN dim_produk  p ON p.sk_produk  = f.sk_produk
GROUP BY ALL ORDER BY ALL""",
        "obt": """SELECT kota_nasabah, segmen, akad,
       SUM(nominal) AS total, COUNT(*) AS jumlah
FROM obt_transaksi
GROUP BY ALL ORDER BY ALL""",
    },
}

JOIN_INFO = {  # jumlah join dan kedalaman rantai join terpanjang
    ("q1", "oltp"): (1, 1), ("q1", "star"): (3, 1), ("q1", "obt"): (0, 0),
    ("q2", "oltp"): (2, 2), ("q2", "star"): (2, 1), ("q2", "obt"): (0, 0),
}
