select dt, jumlah_trx, total_nominal, sumber
from {{ source('raw', 'kontrol') }}
where dt = toDate('{{ var("tgl") }}')
