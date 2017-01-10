/* cms_topn/cms_topn--1.0.0.sql */

CREATE TYPE cms_topn;

CREATE FUNCTION cms_topn_in(cstring)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_topn_out(cms_topn)
	RETURNS cstring
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_topn_recv(internal)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION cms_topn_send(cms_topn)
	RETURNS bytea
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE cms_topn (
	input = cms_topn_in,
	output = cms_topn_out,
	receive = cms_topn_recv,
	send = cms_topn_send,
	storage = extended
);

CREATE FUNCTION cms_topn(integer, double precision default 0.001, double precision default 0.99)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION cms_topn_add(cms_topn, anyelement)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME', 'cms_topn_add'
	LANGUAGE C IMMUTABLE;	

CREATE FUNCTION cms_topn_add_agg(cms_topn, anyelement, integer)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;

CREATE FUNCTION cms_topn_add_agg_with_parameters(cms_topn, anyelement, integer, double precision default 0.001, double precision default 0.99)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;
	
CREATE AGGREGATE cms_topn_add_agg(anyelement, integer)(
	SFUNC = cms_topn_add_agg,
    STYPE = cms_topn
);

CREATE AGGREGATE cms_topn_add_agg(anyelement, integer, double precision, double precision )(
	SFUNC = cms_topn_add_agg_with_parameters,
    STYPE = cms_topn
);
	
CREATE FUNCTION cms_topn_frequency(cms_topn, anyelement)
	RETURNS bigint
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_topn_union(cms_topn, cms_topn)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;

CREATE FUNCTION cms_topn_union_agg(cms_topn, cms_topn)
	RETURNS cms_topn
	AS 'MODULE_PATHNAME'
	LANGUAGE C IMMUTABLE;

CREATE AGGREGATE cms_topn_union_agg(cms_topn )(
	SFUNC = cms_topn_union_agg,
    STYPE = cms_topn
);

CREATE FUNCTION cms_topn_info(cms_topn)
	RETURNS text
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION topn(cms_topn, anyelement) 
    RETURNS TABLE(item anyelement, frequency bigint)
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;

/* TODO */

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


