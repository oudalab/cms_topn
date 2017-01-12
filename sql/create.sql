--
--Testing cms function of the extension which creates new cms structure
--

CREATE EXTENSION cms_mms;

--check errors for unproper parameters
SELECT cms(0);
SELECT cms(1, -0.1, 0.9);
SELECT cms(-1, 0.1, 0.9);
SELECT cms(3, 0.1, -0.5);
SELECT cms(4, 0.02, 1.1);

--check proper parameters
CREATE TABLE create_test (
	cms_column cms
);

INSERT INTO create_test values(cms(10));
INSERT INTO create_test values(cms(5, 0.01, 0.95));
SELECT cms_info(cms_column) from create_test;
