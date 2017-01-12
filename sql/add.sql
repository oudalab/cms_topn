--
--Testing cms_add function of the extension 
--

--check null cms
SELECT cms_add(NULL, 5);

--check composite types
create type composite_type as ( a int, b text);
select cms_add(cms(2), (3,'cms')::composite_type);

--check null new item for different type of elements
CREATE TABLE add_test (
	cms_column cms
);

--fixed size type like integer
INSERT INTO add_test VALUES(cms(3));
UPDATE add_test SET cms_column = cms_add(cms_column, NULL::integer);
SELECT cms_info(cms_column) FROM add_test;

--variable length type like text
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms(3));
UPDATE add_test SET cms_column = cms_add(cms_column, NULL::text);
SELECT cms_info(cms_column) FROM add_test;

--check type consistency
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms(2));
UPDATE add_test SET cms_column = cms_add(cms_column, 'hello'::text);
UPDATE add_test SET cms_column = cms_add(cms_column, 3);

--check normal insertion
--cidr
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms(2));
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.100.128/25'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, NULL::cidr);
SELECT cms_topn(cms_column, NULL::cidr) from add_test;
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/24'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/24'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/24'::cidr);
SELECT cms_topn(cms_column, NULL::cidr) from add_test;
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/25'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/25'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/25'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168/25'::cidr);
SELECT cms_topn(cms_column, NULL::cidr) from add_test;
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.1'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.1'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.1'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.1'::cidr);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.1'::cidr);
SELECT cms_topn(cms_column, NULL::cidr) from add_test;
SELECT cms_info(cms_column) from add_test;

--inet
DELETE FROM add_test;
INSERT INTO add_test VALUES(cms(2));
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.100.128/25'::inet);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.100.128/25'::inet);
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.100.128/25'::inet);
SELECT cms_topn(cms_column, NULL::inet) from add_test;
UPDATE add_test SET cms_column = cms_add(cms_column, '192.168.100.128/23'::inet);
SELECT cms_topn(cms_column, NULL::inet) from add_test;
UPDATE add_test SET cms_column = cms_add(cms_column, NULL::inet);
UPDATE add_test SET cms_column = cms_add(cms_column, NULL::inet);
SELECT cms_topn(cms_column, NULL::inet) from add_test;
UPDATE add_test SET cms_column = cms_add(cms_column, '10.1.2.3/32'::inet);
UPDATE add_test SET cms_column = cms_add(cms_column, '10.1.2.3/32'::inet);
UPDATE add_test SET cms_column = cms_add(cms_column, '10.1.2.3/32'::inet);
UPDATE add_test SET cms_column = cms_add(cms_column, '10.1.2.3/32'::inet);
SELECT cms_topn(cms_column, NULL::inet) from add_test;

