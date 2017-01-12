--
--Testing cms_union_agg function of the extension
--

CREATE TABLE union_agg (
	cms_column cms
);

--check with null values
INSERT INTO union_agg VALUES(NULL);
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

INSERT INTO union_agg VALUES(NULL);
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 2) FROM numbers WHERE int_column = 0;
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 2) FROM numbers WHERE int_column = 1;
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

INSERT INTO union_agg VALUES(NULL);
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;


--check cmss with different parameters
DELETE FROM union_agg;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 1) FROM numbers WHERE int_column = 0;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 1, 0.1, 0.9) FROM numbers WHERE int_column = 1;
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

DELETE FROM union_agg;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 2) FROM numbers WHERE int_column = 0;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 1) FROM numbers WHERE int_column = 0;
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

DELETE FROM union_agg;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 4) FROM numbers WHERE int_column = 0;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(text_column, 2) FROM strings WHERE text_column = '0';
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;

--check normal cases
DELETE FROM union_agg;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 0;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 1;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 2;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 3;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 4;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 5;
INSERT INTO union_agg(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers WHERE int_column = 6;
SELECT cms_topn(cms_union_agg(cms_column), NULL::integer) from union_agg;
