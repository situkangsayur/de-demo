"""Membuat bucket `lake` di RustFS bila belum ada (idempoten). Dijalankan service rustfs-init."""

import sys
import time

from pyarrow import fs as pafs

from .common import bucket, get_logger, s3fs

log = get_logger("rustfs-init")


def main() -> int:
    name = bucket()
    for attempt in range(1, 61):
        try:
            s3 = s3fs(allow_bucket_creation=True)
            info = s3.get_file_info(name)
            if info.type == pafs.FileType.NotFound:
                s3.create_dir(name)
                log.info("bucket dibuat", extra={"f": {"bucket": name}})
            else:
                log.info("bucket sudah ada", extra={"f": {"bucket": name}})
            # Penanda prefix bronze agar terlihat di console RustFS sebelum data pertama masuk.
            with s3.open_output_stream(f"{name}/bronze/_README.txt") as f:
                f.write(
                    b"Bronze: s3://lake/bronze/<sumber>/<entitas>/dt=YYYY-MM-DD/part-<batch_id>.parquet\n"
                    b"Lihat de-demo/platform/KONTRAK-DATA.md\n"
                )
            return 0
        except Exception as e:  # RustFS mungkin belum siap menerima request
            log.warning("menunggu rustfs", extra={"f": {"attempt": attempt, "err": str(e)}})
            time.sleep(2)
    log.error("bucket gagal dibuat", extra={"f": {"bucket": name}})
    return 1


if __name__ == "__main__":
    sys.exit(main())
