SELECT *
FROM lineitem, iteminfo, orders
WHERE l_partkey = i_partkey and l_orderkey = o_orderkey;
