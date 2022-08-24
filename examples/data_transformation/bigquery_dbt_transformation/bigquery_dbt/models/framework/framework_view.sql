
select *
from {{ ref('framework_table') }}
where framework is not null
