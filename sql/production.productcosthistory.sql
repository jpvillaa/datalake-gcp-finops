select
	cast(productid as text) as productid,
	cast(startdate as text) as startdate,
	cast(enddate as text) as enddate,
	cast(standardcost as text) as standardcost,
	cast(modifieddate as text) as modifieddate,
    cast(current_date as text) AS ingestion_date,
    'production.productcosthistory' AS data_source,
    cast(session_user as text) as current_user
from production.productcosthistory;
