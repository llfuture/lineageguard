with parsed as (
    select
        trim(substr(line, 19, 15))                          as symbol,
        trim(substr(line, 34, 6))                           as issue_type,
        trim(substr(line, 40, 4))                           as status,
        trim(substr(line, 44, 70))                          as security_name,
        trim(substr(line, 114, 6))                          as ex_id,
        try_cast(trim(substr(line, 120, 13)) as bigint)     as shares_outstanding,
        try_cast(trim(substr(line, 149, 12)) as double)     as dividend,
        trim(substr(line, 161, 60))                         as co_name_or_cik,
        substr(line, 1, 15)                                 as pts
    from {{ source('raw', 'raw_finwire') }}
    where rec_type = 'SEC'
),
ranked as (
    select *, row_number() over (partition by symbol order by pts desc) as rn
    from parsed
)
select symbol, issue_type, status, security_name, ex_id,
       shares_outstanding, dividend, co_name_or_cik
from ranked
where rn = 1
