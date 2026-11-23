{# Generic test: nilai harus di (min_lebih_dari, maks]. #}
{% test rentang_nilai(model, column_name, lebih_dari, paling_besar) %}
select * from {{ model }}
where {{ column_name }} <= {{ lebih_dari }} or {{ column_name }} > {{ paling_besar }}
{% endtest %}
