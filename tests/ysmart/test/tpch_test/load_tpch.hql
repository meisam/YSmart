create table nation(n_nationkey int, n_name string, n_regionkey int, n_comment string)                                                                                                                                                                                                                                            row format delimited fields terminated by '|' lines terminated by '\n';                    
create table region(r_regionkey int, r_name string, r_comment string)                                                                                                                                                                                                                                                             row format delimited fields terminated by '|' lines terminated by '\n';                    
create table part(p_partkey int, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice float, p_comment string)                                                                                                                                                              row format delimited fields terminated by '|' lines terminated by '\n';                    
create table supplier(s_suppkey int, s_name string, s_address string, s_nationkey int, s_phone string, s_acctbal float, s_comment string)                                                                                                                                                                                         row format delimited fields terminated by '|' lines terminated by '\n';                    
create table partsupp(ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost float, ps_comment string)                                                                                                                                                                                                                    row format delimited fields terminated by '|' lines terminated by '\n';                    
create table customer(c_custkey int, c_name string, c_address string, c_nationkey int, c_phone string, c_acctbal float, c_mktsegment string, c_comment string)                                                                                                                                                                    row format delimited fields terminated by '|' lines terminated by '\n';                    
create table orders(o_orderkey int, o_custkey int, o_orderstatus string, o_totalprice float, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string)                                                                                                                                    row format delimited fields terminated by '|' lines terminated by '\n';                    
create table lineitem(l_orderkey int, l_partkey int, l_suppkey int, l_linenumber int, l_quantity float, l_extendedprice float, l_discount float, l_tax float, l_returnflag string, l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, l_shipmode string, l_comment string) row format delimited fields terminated by '|' lines terminated by '\n';


load data local inpath 'nation.tbl' into table nation;
load data local inpath 'region.tbl' into table region;
load data local inpath 'part.tbl' into table part;
load data local inpath 'supplier.tbl' into table supplier;
load data local inpath 'partsupp.tbl' into table partsupp;
load data local inpath 'customer.tbl' into table customer;
load data local inpath 'orders.tbl' into table orders;
load data local inpath 'lineitem.tbl' into table lineitem;
