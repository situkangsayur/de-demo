{#- Uji generik buatan sendiri: baris yang melanggar = nilai di luar [min_nilai, max_nilai]. -#}
{% test rentang_nilai(model, column_name, min_nilai, max_nilai) %}
select {{ column_name }} as nilai
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_nilai }} or {{ column_name }} > {{ max_nilai }})
{% endtest %}
