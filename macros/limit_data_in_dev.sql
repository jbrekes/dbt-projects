{%- macro limit_data_in_dev(column_name, dev_day_of_data=3) -%}

{%- if target.name == 'default' -%}
where {{ column_name }} >= date_add(current_timestamp, INTERVAL {{ dev_day_of_data }} DAY)
{%- endif -%}

{%- endmacro -%}