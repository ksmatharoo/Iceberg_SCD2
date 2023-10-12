MERGE INTO s3lakehouse.blog.customer_base as b
USING
( SELECT null as custkey_match, custkey, name, state, zip, cust_since, last_update_dt,'Y' as active_ind,current_timestamp as end_dt
FROM s3lakehouse.blog.customer_land
UNION ALL
SELECT
custkey as custkey_match,custkey, name, state, zip, cust_since, last_update_dt,active_ind,end_dt
FROM s3lakehouse.blog.customer_base
WHERE custkey IN
(SELECT custkey FROM s3lakehouse.blog.customer_land where active_ind = 'Y')
) as scdChangeRows
ON (b.custkey = scdChangeRows.custkey and b.custkey = scdChangeRows.custkey_match)
WHEN MATCHED and b.active_ind = 'Y' THEN
UPDATE SET end_dt = current_timestamp,active_ind = 'N'
WHEN NOT MATCHED THEN
        INSERT (custkey, name, state, zip, cust_since,last_update_dt,active_ind,end_dt)
            VALUES(scdChangeRows.custkey, scdChangeRows.name, scdChangeRows.state, scdChangeRows.zip,
                    scdChangeRows.cust_since,scdChangeRows.last_update_dt,'Y',null);