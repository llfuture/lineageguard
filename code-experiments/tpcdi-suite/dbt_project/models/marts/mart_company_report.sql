-- SINK: per company, security count and latest reported fundamentals
with latest as (
    select cik, max(fin_year * 10 + fin_qtr) as latest_yq
    from {{ ref('fact_financials') }} group by cik
),
fin as (
    select f.cik, f.revenue, f.eps, f.assets
    from {{ ref('fact_financials') }} f
    join latest l on f.cik = l.cik and f.fin_year * 10 + f.fin_qtr = l.latest_yq
),
sec as (
    select cik, count(*) as n_securities from {{ ref('dim_security') }}
    where cik is not null group by cik
)
select c.cik, c.company_name, c.industry_name,
       coalesce(s.n_securities, 0)      as n_securities,
       round(avg(f.revenue), 2)         as latest_revenue,
       round(avg(f.eps), 4)             as latest_eps
from {{ ref('dim_company') }} c
left join sec s on c.cik = s.cik
left join fin f on c.cik = f.cik
group by 1, 2, 3, 4
