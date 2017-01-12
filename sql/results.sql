--
--Check functions of the extension which returns results like frequencies and the top n:
--cms_topn, cms_info, cms_get_frequency.
--

--cms_topn
--check with null and empty values
SELECT cms_topn(NULL, NULL::integer);
SELECT cms_topn(cms(2), NULL::text);

--check with wrong parameters
SELECT cms_topn(cms_add(cms(2),2),NULL::text);

--cms_info
--check with null and empty values
SELECT cms_info(NULL);
SELECT cms_info(cms(1));
SELECT cms_info(cms(2, 0.01, 0.99));

--check normal cases
SELECT cms_info(cms_add(cms(1), 5));
SELECT cms_info(cms_add(cms(2), 5));
SELECT cms_info(cms_add(cms(2, 0.1, 0.9), '5'::text));

--cms_get_frequency
--check with null and empty values
SELECT cms_get_frequency(NULL, NULL::text);
SELECT cms_get_frequency(NULL, 3::integer);
SELECT cms_get_frequency(cms_add(cms(1),5), NULL::text);
SELECT cms_get_frequency(cms(3), NULL::bigint);

--check normal cases
CREATE TABLE results (
	cms_column cms
);

INSERT INTO results(cms_column) SELECT cms_add_agg(int_column, 3) FROM numbers;
SELECT cms_get_frequency(cms_column, 0) FROM results;
SELECT cms_get_frequency(cms_column, 1) FROM results;
SELECT cms_get_frequency(cms_column, 2) FROM results;
SELECT cms_get_frequency(cms_column, 3) FROM results;
SELECT cms_get_frequency(cms_column, 4) FROM results;
SELECT cms_get_frequency(cms_column, 5) FROM results;
SELECT cms_get_frequency(cms_column, -1) FROM results;
SELECT cms_get_frequency(cms_column, NULL::integer) FROM results;

DELETE FROM results;
INSERT INTO results(cms_column) SELECT cms_add_agg(text_column, 2) FROM strings;
SELECT cms_get_frequency(cms_column, '0'::text) FROM results;
SELECT cms_get_frequency(cms_column, '1'::text) FROM results;
SELECT cms_get_frequency(cms_column, '2'::text) FROM results;
SELECT cms_get_frequency(cms_column, '3'::text) FROM results;
SELECT cms_get_frequency(cms_column, '4'::text) FROM results;
SELECT cms_get_frequency(cms_column, '5'::text) FROM results;
SELECT cms_get_frequency(cms_column, '6'::text) FROM results;
SELECT cms_get_frequency(cms_column, NULL::text) FROM results;




