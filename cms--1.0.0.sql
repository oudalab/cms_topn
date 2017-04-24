/* cms/cms--1.0.0.sql */

/* ----- Count-min sketch functions / types ----- */

CREATE TYPE cms;

CREATE FUNCTION cms_in(cstring)
	RETURNS cms
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_out(cms)
	RETURNS cstring
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_recv(internal)
	RETURNS cms
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION cms_send(cms)
	RETURNS bytea
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE cms (
	input = cms_in,
	output = cms_out,
	receive = cms_recv,
	send = cms_send,
	storage = extended
);

CREATE FUNCTION cms( double precision default 0.001, double precision default 0.99)
	RETURNS cms
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION cms_add(cms, anyelement)
	RETURNS cms
	AS 'MODULE_PATHNAME', 'cms_add'
	LANGUAGE C IMMUTABLE;	
	
CREATE FUNCTION cms_get_frequency(cms, anyelement)
	RETURNS bigint
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_info(cms)
	RETURNS text
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

