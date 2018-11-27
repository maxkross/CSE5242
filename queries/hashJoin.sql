SELECT /*+USE_HASH("order")*/ *
FROM   "lineitem" INNER JOIN"order" 
ON l_orderkey = o_orderkey
WHERE o_zip=43202;

