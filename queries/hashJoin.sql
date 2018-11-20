SELECT /*+USE_HASH("order")*/ l_orderkey
FROM   "lineitem" INNER JOIN "orders"
ON l_orderkey = o_orderkey;

