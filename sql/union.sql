--
--Testing cms_union function of the extension
--

--check cases with null
SELECT cms_union(NULL, NULL);
SELECT * FROM cms_topn(cms_union(cms_add(cms(1),4), NULL), NULL::integer);
SELECT * FROM cms_topn(cms_union(NULL, cms_add(cms(2),'cms'::text)), NULL::text);

--check cases with empty cms
SELECT * FROM cms_topn(cms_union(cms(1), cms(1)), NULL::integer);
SELECT * FROM cms_topn(cms_union(cms(3), cms_add(cms(3),'cms'::text)), NULL::text);
SELECT * FROM cms_topn(cms_union(cms_add(cms(2),4), cms(2)), NULL::integer);

--check if parameters of the cmss are not the same
SELECT cms_union(cms(2), cms(1));
SELECT cms_union(cms(1, 0.1, 0.9), cms(1, 0.1, 0.8));
SELECT cms_union(cms(1, 0.1, 0.99), cms(1, 0.01, 0.99));
SELECT cms_union(cms_add(cms(2), 2), cms_add(cms(2), '2'::text));

--check normal cases
SELECT * FROM cms_topn(cms_union(cms_add(cms(1),2), cms_add(cms(1),3)), NULL::integer);
SELECT * FROM cms_topn(cms_union(cms_add(cms(1),2), cms_add(cms(1),2)), NULL::integer);
SELECT * FROM cms_topn(cms_union(cms_add(cms(2),'two'::text), cms_add(cms(2),'three'::text)), NULL::text);
SELECT * FROM cms_topn(cms_union(cms_add(cms(2),'two'::text), cms_add(cms(2),'two'::text)), NULL::text);
SELECT * FROM cms_topn(cms_union(cms_add(cms(3),'2'::text), cms_add(cms(3),'3'::text)), NULL::text);
SELECT * FROM cms_topn(cms_union(cms_add(cms(3),'2'::text), cms_add(cms(3),'2'::text)), NULL::text);
