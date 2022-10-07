
{% snapshot orders_snapshot %}

  {{
    config(
      target_database='dev_db',
      target_schema='dbt_achen',
      strategy='check',
      unique_key='ORDER_ID',
      check_cols=['status'],
    )
  }}

  SELECT * 
  FROM {{ source('postgres', 'orders') }}

{% endsnapshot %}