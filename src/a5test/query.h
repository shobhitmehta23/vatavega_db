char *test1 = "SELECT SUM (ps.ps_supplycost), s.s_suppkey \ 
FROM part AS p, supplier AS s, partsupp AS ps \
WHERE (p.p_partkey = ps.ps_partkey) AND \
(s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500) \
GROUP BY s.s_suppkey";

char *test2 =
		"SELECT SUM (c.c_acctbal),c.c_name \
		FROM customer AS c, orders AS o \
		WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000) \
		GROUP BY c.c_name";

char *test3 =
		"SELECT l.l_orderkey, l.l_partkey, l.l_suppkey \
		FROM lineitem AS l \
		WHERE (l.l_returnflag = 'R') AND \ 
		      (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')";


char *test4 =
		"SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal \ 
		FROM customer AS c1, customer AS c2  \
		WHERE (c1.c_nationkey = c2.c_nationkey) AND \
			  (c1.c_name ='Customer#000070919')";


char *test5 = "SELECT SUM(l.l_discount) \
FROM customer AS c, orders AS o, lineitem AS l \
WHERE (c.c_custkey = o.o_custkey) AND \
      (o.o_orderkey = l.l_orderkey) AND \
      (c.c_name = 'Customer#000070919') AND \ 
	  (l.l_quantity > 30) AND (l.l_discount < 0.03)";

char *test6 =
		"SELECT l.l_orderkey \
		FROM lineitem AS l \
		WHERE (l.l_quantity > 30)";

char *test7 =
		"SELECT DISTINCT c.c_name \
		FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r \
		WHERE (l.l_orderkey = o.o_orderkey) AND \
		      (o.o_custkey = c.c_custkey) AND \
			  (c.c_nationkey = n.n_nationkey) AND \
			  (n.n_regionkey = r.r_regionkey)";

char *test8 =
		"SELECT l.l_discount \
		FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r \ 
		WHERE (l.l_orderkey = o.o_orderkey) AND \
			  (o.o_custkey = c.c_custkey) AND \
			  (c.c_nationkey = n.n_nationkey) AND \
			  (n.n_regionkey = r.r_regionkey) AND \
			  (r.r_regionkey = 1) AND (o.o_orderkey < 10000)";



char *test9 =
		"SELECT SUM (l.l_discount) \
		FROM customer AS c, orders AS o, lineitem AS l \
		WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND \
			  (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30) AND \
			  (l.l_discount < 0.03)";

char *test10 =
		"SELECT SUM (l.l_extendedprice * l.l_discount) \
	FROM lineitem AS l \
	WHERE (l.l_discount<0.07) AND (l.l_quantity < 24)";

char *test11 =
		"SELECT SUM (ps.ps_supplycost) \
		FROM part AS p, supplier AS s, partsupp AS ps \
		WHERE (p.p_partkey = ps.ps_partkey) AND \
			  (s.s_suppkey = ps.ps_suppkey) AND \
			  (s.s_acctbal > 2500.0)";

char *test12 =
"SELECT SUM (c.c_acctbal) \
FROM customer AS c, orders AS o \
WHERE (c.c_custkey = o.o_custkey) AND \
	  (o.o_totalprice < 10000.0)";

char *test13 =
		"SELECT l.l_orderkey, l.l_partkey, l.l_suppkey \
		FROM lineitem AS l \
		WHERE (l.l_returnflag = 'R') AND \
			  (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL') AND \
			  (l.l_orderkey > 5000) AND (l.l_orderkey < 6000)";

char *test14 =
		"SELECT ps.ps_partkey, ps.ps_suppkey, ps.ps_availqty \
FROM partsupp AS ps \
WHERE (ps.ps_partkey < 100) AND (ps.ps_suppkey < 50)";

char *test15 =
		"SELECT SUM (l.l_discount) \
FROM customer AS c, orders AS o, lineitem AS l \
WHERE (c.c_custkey = o.o_custkey) AND \
	  (o.o_orderkey = l.l_orderkey) AND \
	  (c.c_name = 'Customer#000070919') AND \
	  (l.l_quantity > 30.0) AND (l.l_discount < 0.03)";

char *test16 =
		"SELECT DISTINCT s.s_name \
FROM supplier AS s, part AS p, partsupp AS ps \
WHERE (s.s_suppkey = ps.ps_suppkey) AND \
	  (p.p_partkey = ps.ps_partkey) AND \
	  (p.p_mfgr = 'Manufacturer#4') AND \
	  (ps.ps_supplycost < 350.0)";

char *test17 =
		"SELECT SUM (l.l_extendedprice * (1 - l.l_discount)), l.l_orderkey, o.o_orderdate, o.o_shippriority \
FROM customer AS c, orders AS o, lineitem AS l \
WHERE (c.c_mktsegment = 'BUILDING') AND \
      (c.c_custkey = o.o_custkey) AND (l.l_orderkey = o.o_orderkey) AND \
	  (l.l_orderkey < 100 OR o.o_orderkey < 100) \
GROUP BY l_orderkey, o_orderdate, o_shippriority";

char * tests[17] = {test1, test2, test3, test4, test5, test6, test7, test8, test9, test10, test11, test12, test13, test14, test15, test16, test17};
