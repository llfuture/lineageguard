with ranked as (
    select ca_id, c_id, ca_name, ca_tax_st, action_ts,
           row_number() over (partition by ca_id order by action_seq desc) as rn
    from {{ source('raw', 'raw_account_actions') }}
    where ca_id is not null
)
select ca_id as account_id, c_id as customer_id, ca_name as account_name,
       ca_tax_st as tax_status
from ranked where rn = 1
