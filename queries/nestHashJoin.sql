SELECT l_partkey, o_orderkey
FROM lineitem, iteminfo, "order"
WHERE l_partkey = i_partkey and l_orderkey = o_orderkey;
