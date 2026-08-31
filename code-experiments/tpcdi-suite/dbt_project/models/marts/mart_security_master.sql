-- SINK (row-preserving dimension mart): one row per security.
select symbol, security_name, issue_type, status,
       shares_outstanding, dividend, cik, company_name, industry_name
from {{ ref('dim_security') }}
