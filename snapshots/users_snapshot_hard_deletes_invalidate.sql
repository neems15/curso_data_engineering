{% snapshot users_snapshot_hard_deletes_invalidate %}

{{
    config(
      target_schema='snapshots',
      unique_key='DNI',
      strategy='timestamp',
      updated_at='fecha_alta_sistema',
      hard_deletes='invalidate',
    )
}}

select * from {{ source('google_sheets', 'users') }}

{% endsnapshot %}