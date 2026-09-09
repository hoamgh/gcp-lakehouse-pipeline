{% snapshot customers_snapshot %}

{{
    config(
        target_schema=target.schema ~ '_snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_name', 'email', 'phone', 'zip_code', 'city', 'state'],
        invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}
