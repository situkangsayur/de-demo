{# Normalisasi kolom mentah (Nullable(String)) di raw. Semua aturan sama persis dengan jalur ETL
   (pipeline/kolom.py dan pipeline/silver.py) agar kedua jalur menghasilkan angka yang identik. #}

{% macro ke_waktu(c) -%}
multiIf(
  {{ c }} IS NULL OR {{ c }} = '', NULL,
  match({{ c }}, '^-?[0-9]+$'), multiIf(
      abs(toInt64OrZero({{ c }})) > 100000000000000000, toDateTime64(fromUnixTimestamp64Nano(toInt64OrZero({{ c }})), 6, 'UTC'),
      abs(toInt64OrZero({{ c }})) > 100000000000000, fromUnixTimestamp64Micro(toInt64OrZero({{ c }}), 'UTC'),
      abs(toInt64OrZero({{ c }})) > 100000000000, toDateTime64(fromUnixTimestamp64Milli(toInt64OrZero({{ c }}), 'UTC'), 6, 'UTC'),
      abs(toInt64OrZero({{ c }})) > 1000000, toDateTime64(toInt64OrZero({{ c }}), 6, 'UTC'),
      toDateTime64(toInt64OrZero({{ c }}) * 86400, 6, 'UTC')),
  parseDateTime64BestEffortOrNull({{ c }}, 6, 'UTC'))
{%- endmacro %}

{% macro ke_angka(c) -%}
toDecimal64(round(toFloat64OrNull(trimBoth({{ c }})), 2), 2)
{%- endmacro %}

{% macro token(expr) -%}
if(({{ expr }}) IS NULL OR ({{ expr }}) = '', NULL,
   concat('tok_', substring(lower(hex(SHA256(concat('{{ env_var("TOKEN_PEPPER") }}', '|', {{ expr }})))), 1, 20)))
{%- endmacro %}

{% macro norm_hp(c) -%}
if(startsWith(replaceRegexpAll({{ c }}, '[^0-9]', ''), '0'),
   concat('62', substring(replaceRegexpAll({{ c }}, '[^0-9]', ''), 2)),
   replaceRegexpAll({{ c }}, '[^0-9]', ''))
{%- endmacro %}

{% macro samaran(c) -%}
if({{ c }} IS NULL, NULL,
   arrayStringConcat(arrayMap(w -> concat(substringUTF8(w, 1, 1), '***'), splitByChar(' ', trimBoth(assumeNotNull({{ c }})))), ' '))
{%- endmacro %}

{# versi terakhir per id: updated_at, _ingested_at, _ts_ms terbaru, lalu cdc mengalahkan app, lalu _event_id #}
{% macro urut_terakhir() -%}
{{ ke_waktu('updated_at') }} DESC NULLS LAST, {{ ke_waktu('_ingested_at') }} DESC NULLS LAST,
toInt64OrNull(_ts_ms) DESC NULLS LAST, _source DESC, _event_id DESC
{%- endmacro %}

{% macro kanal_std(c) -%}
transform(lowerUTF8(trimBoth({{ c }})),
  ['mobile banking', 'm-banking', 'mbanking', 'app', 'counter', 'cabang', 'internet banking', 'ib', 'netbanking',
   'e-commerce', 'toko', 'shop'],
  ['mobile', 'mobile', 'mobile', 'mobile', 'teller', 'teller', 'web', 'web', 'web',
   'ecommerce', 'ecommerce', 'ecommerce'],
  lowerUTF8(trimBoth({{ c }})))
{%- endmacro %}

{% macro akad_std(c) -%}
transform(trimBoth({{ c }}),
  ['MRB', 'MURABAHAH', 'MSY', 'MUSYARAKAH', 'MDH', 'MUDHARABAH', 'MUDARABAH', 'IJR', 'IJARAH'],
  ['MRB', 'MRB', 'MSY', 'MSY', 'MDH', 'MDH', 'MDH', 'IJR', 'IJR'],
  '')
{%- endmacro %}
