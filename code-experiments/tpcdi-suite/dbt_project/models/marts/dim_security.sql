select s.symbol, s.issue_type, s.status, s.security_name, s.ex_id,
       s.shares_outstanding, s.dividend, s.co_name_or_cik,
       d.cik, d.company_name, d.industry_name
from {{ ref('stg_security') }} s
left join {{ ref('dim_company') }} d on s.co_name_or_cik = d.cik
