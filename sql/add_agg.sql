--
--Testing cms_add_agg function of the extension 
--

--prepare tables for aggregates
create table numbers (
	int_column int
);

INSERT INTO numbers SELECT 0 FROM generate_series(1,200);
INSERT INTO numbers SELECT 1 FROM generate_series(1,10);
INSERT INTO numbers SELECT 2 FROM generate_series(1,150);
INSERT INTO numbers SELECT 3 FROM generate_series(1,3);
INSERT INTO numbers SELECT 4 FROM generate_series(1,40000);
INSERT INTO numbers SELECT 5 FROM generate_series(1,6000);
INSERT INTO numbers SELECT NULL FROM generate_series(1,5);

create table strings (
	text_column text
);

INSERT INTO strings SELECT '0' FROM generate_series(1,2);
INSERT INTO strings SELECT '1' FROM generate_series(1,1);
INSERT INTO strings SELECT '2' FROM generate_series(1,15000);
INSERT INTO strings SELECT '3' FROM generate_series(1,20);
INSERT INTO strings SELECT '4' FROM generate_series(1,6);
INSERT INTO strings SELECT '5' FROM generate_series(1,70000);
INSERT INTO strings SELECT NULL FROM generate_series(1,30);

--check errors for unproper parameters
SELECT cms_add_agg(int_column, 0) FROM numbers;
SELECT cms_add_agg(int_column, -1, 0.2, 0.9) FROM numbers;
SELECT cms_add_agg(int_column, 1, 2, 0.9) FROM numbers;
SELECT cms_add_agg(int_column, 3, 0.2, 1.5) FROM numbers;

--check aggregates for fixed size types like integer with default parameters
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 0;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 1;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 2;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 3;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 4;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 5;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers WHERE int_column < 6;
SELECT cms_topn(cms_add_agg(int_column, 3), NULL::integer) FROM numbers;
SELECT cms_info(cms_add_agg(int_column, 3)) FROM numbers;

--check aggregates for variable length types like text
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '0';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '1';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '2';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '3';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '4';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '5';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings WHERE text_column < '6';
SELECT cms_topn(cms_add_agg(text_column, 4, 0.01, 0.09), NULL::text) FROM strings;
SELECT cms_info(cms_add_agg(text_column, 4)) FROM strings;
