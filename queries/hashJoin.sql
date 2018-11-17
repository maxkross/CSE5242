SELECT /*+USE_HASH("order")*/ l_orderkey
FROM   "lineitem" INNER JOIN "order"
ON l_orderkey = o_orderkey;

