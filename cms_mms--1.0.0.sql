/* cms_mms/cms_mms--1.0.0.sql */

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

/* ----- Min-mask sketch functions / types ----- */

CREATE TYPE mms;

CREATE FUNCTION mms_in(cstring)
	RETURNS mms
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION mms_out(mms)
	RETURNS cstring
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION mms_recv(internal)
	RETURNS mms
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION mms_send(mms)
	RETURNS bytea
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE mms (
	input = mms_in,
	output = mms_out,
	receive = mms_recv,
	send = mms_send,
	storage = extended
);

CREATE FUNCTION mms(double precision default 0.001, double precision default 0.99)
	RETURNS mms
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;

CREATE FUNCTION mms_add(mms, anyelement, integer)
	RETURNS mms
	AS 'MODULE_PATHNAME', 'mms_add'
	LANGUAGE C IMMUTABLE;	

CREATE FUNCTION mms_get_mask(mms, anyelement)
	RETURNS bigint
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;


