-- FINWIRE CMP records: latest record per CIK (fixed-width slice per TPC-DI spec)
with parsed as (
    select
        trim(substr(line, 79, 10))                as cik,
        trim(substr(line, 19, 60))                as company_name,
        trim(substr(line, 89, 4))                 as status,
        trim(substr(line, 93, 2))                 as industry_id,
        trim(substr(line, 95, 4))                 as sp_rating,
        substr(line, 1, 15)                       as pts
    from {{ source('raw', 'raw_finwire') }}
    where rec_type = 'CMP'
),
ranked as (
    select *, row_number() over (partition by cik order by pts desc) as rn
    from parsed
)
select cik, company_name, status, industry_id, sp_rating, pts
from ranked
where rn = 1
