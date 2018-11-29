SELECT l_partkey, l_orderkey
FROM lineitem, iteminfo, "order"
WHERE l_partkey = i_partkey and l_orderkey = o_orderkey;
