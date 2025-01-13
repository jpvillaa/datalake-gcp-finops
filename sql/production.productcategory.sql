SELECT 
    cast(productcategoryid as text) as productcategoryid,
    cast(name as text) as name,
    cast(rowguid as text) as rowguid,
    cast(modifieddate as text) as modifieddate,
    cast(current_date as text) AS ingestion_date,
    'production.productcategory' AS data_source,
    cast(session_user as text) AS current_user
FROM production.productcategory;
