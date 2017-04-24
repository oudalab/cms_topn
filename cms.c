/*-------------------------------------------------------------------------
 *
 * cms.c
 *
 * This file contains the function definitions to perform top-n, point and
 * union queries by using the count-min sketch structure.
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include <math.h>
#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "MurmurHash3.h"
#include "utils/array.h"
#include "utils/bytea.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/typcache.h"


#define DEFAULT_ERROR_BOUND 0.001
#define DEFAULT_CONFIDENCE_INTERVAL 0.99
#define MURMUR_SEED 304837963

/*
 * CountMinSketch is the main struct for the count-min sketch top-n implementation and
 * it has two variable length fields. First one is an array for keeping the sketch
 * which is pointed by "sketch" of the struct and second one is an ArrayType for
 * keeping the most frequent n items. It is not possible to lay out two variable
 * width fields consecutively in memory. So we are using pointer arithmetic to
 * reach and handle ArrayType for the most frequent n items.
 */
typedef struct CountMinSketch
{
	char length[4];
	uint32 sketchDepth;
	uint32 sketchWidth;
	uint64 sketch[1];
} CountMinSketch;

/* Local functions forward declarations */
static CountMinSketch* _createCms(float8 errorBound, float8 confidenceInterval);
static CountMinSketch* _updateCms(CountMinSketch* currentCms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry);
static uint64 _updateCmsInPlace(CountMinSketch* cms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry);
static void _convertDatumToBytes(Datum datum, TypeCacheEntry* datumTypeCacheEntry, StringInfo datumString);
static uint64 _cmsEstimateHashedItemFrequency(CountMinSketch* cms, uint64* hashValueArray);
static uint64 _cmsEstimateItemFrequency(CountMinSketch* cms, Datum item, TypeCacheEntry* itemTypeCacheEntry);

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

/* Count-min Sketch functions */
PG_FUNCTION_INFO_V1(cms_in);
PG_FUNCTION_INFO_V1(cms_out);
PG_FUNCTION_INFO_V1(cms_recv);
PG_FUNCTION_INFO_V1(cms_send);
PG_FUNCTION_INFO_V1(cms);
PG_FUNCTION_INFO_V1(cms_add);
PG_FUNCTION_INFO_V1(cms_get_frequency);
PG_FUNCTION_INFO_V1(cms_info);

/* ----- Count-min sketch functionality ----- */

/*
 * cms_in creates cms from printable representation.
 */
Datum cms_in(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));

	return datum;
}


/*
 *  cms_out converts cms to printable representation.
 */
Datum cms_out(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));

	PG_RETURN_CSTRING(datum);
}


/*
 * cms_recv creates cms from external binary format.
 */
Datum cms_recv(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));

	return datum;
}


/*
 * cms_send converts cms to external binary format.
 */
Datum cms_send(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteasend, PG_GETARG_DATUM(0));

	return datum;
}


/*
 * cms is a user-facing UDF which creates new cms with given parameters.
 * The first parameter is for the number of top items which will be kept, the others
 * are for error bound(e) and confidence interval(p) respectively. Given e and p,
 * estimated frequency can be at most (e*||a||) more than real frequency with the
 * probability p while ||a|| is the sum of frequencies of all items according to
 * this paper: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf.
 */
Datum cms(PG_FUNCTION_ARGS)
{
	float8 errorBound = PG_GETARG_FLOAT8(0);
	float8 confidenceInterval =  PG_GETARG_FLOAT8(1);

	CountMinSketch* cms = _createCms(errorBound, confidenceInterval);

	PG_RETURN_POINTER(cms);
}


/*
 * cms_add is a user-facing UDF which inserts new item to the given CountMinSketch.
 * The first parameter is for the CountMinSketch to add the new item and second is for
 * the new item. This function returns updated CountMinSketch.
 */
Datum cms_add(PG_FUNCTION_ARGS)
{
	CountMinSketch* currentCms = NULL;
	CountMinSketch* updatedCms = NULL;
	Datum newItem = 0;
	TypeCacheEntry* newItemTypeCacheEntry = NULL;
	Oid newItemType = InvalidOid;

	/* Check whether cms is null */
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}
	else
	{
		currentCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	}

	/* If new item is null, then return current CountMinSketch */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentCms);
	}

	/* Get item type and check if it is valid */
	newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (newItemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("could not determine input data type")));
	}

	newItem = PG_GETARG_DATUM(1);
	newItemTypeCacheEntry = lookup_type_cache(newItemType, 0);
	updatedCms = _updateCms(currentCms, newItem, newItemTypeCacheEntry);

	PG_RETURN_POINTER(updatedCms);
}


/*
 * cms_get_frequency is a user-facing UDF which returns the estimated frequency
 * of an item. The first parameter is for CountMinSketch and second is for the item to
 * return the frequency.
 */
Datum cms_get_frequency(PG_FUNCTION_ARGS)
{
	CountMinSketch* cms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	Datum item = PG_GETARG_DATUM(1);
	Oid itemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *itemTypeCacheEntry = NULL;
	uint64 frequency = 0;

	if (itemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("could not determine input data types")));
	}

	itemTypeCacheEntry = lookup_type_cache(itemType, 0);
	frequency = _cmsEstimateItemFrequency(cms, item, itemTypeCacheEntry);

	PG_RETURN_INT32(frequency);
}


/*
 * cms_info returns summary about the given CountMinSketch structure.
 */
Datum cms_info(PG_FUNCTION_ARGS)
{
	CountMinSketch* cms = NULL;
	StringInfo cmsInfoString = makeStringInfo();

	cms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	appendStringInfo(cmsInfoString, "Sketch depth = %d, Sketch width = %d, "
	                 "Size = %ukB", cms->sketchDepth, cms->sketchWidth,
	                 VARSIZE(cms) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(cmsInfoString->data));
}


/*
 * _createCms creates CountMinSketch structure with given parameters. The first parameter
 * is for the number of frequent items, other two specifies error bound and confidence
 * interval for this error bound. Size of the sketch is determined with the given
 * error bound and confidence interval according to formula in this paper:
 * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf.
 *
 * Note that here we also allocate initial memory for the ArrayType which keeps
 * the frequent items. This allocation includes ArrayType overhead for one dimensional
 * array and additional memory according to number of top-n items and default size
 * for an individual item.
 */
static CountMinSketch* _createCms(float8 errorBound, float8 confidenceInterval)
{
	CountMinSketch* cms = NULL;
	uint32 sketchWidth = 0;
	uint32 sketchDepth = 0;
	Size staticStructSize = 0;
	Size sketchSize = 0;
	Size totalCmsSize = 0;
	
	if (errorBound <= 0 || errorBound >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for cms"),
		                errhint("Error bound has to be between 0 and 1")));
	}
	else if (confidenceInterval <= 0 || confidenceInterval >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for cms"),
		                errhint("Confidence interval has to be between 0 and 1")));
	}

	sketchWidth = (uint32) ceil(exp(1) / errorBound);
	sketchDepth = (uint32) ceil(log(1 / (1 - confidenceInterval)));
	sketchSize =  sizeof(uint64) * sketchDepth * sketchWidth;
	staticStructSize = sizeof(CountMinSketch);
	totalCmsSize = staticStructSize + sketchSize;

	cms = palloc0(totalCmsSize);
	cms->sketchDepth = sketchDepth;
	cms->sketchWidth = sketchWidth;

	SET_VARSIZE(cms, totalCmsSize);

	return cms;
}


/*
 * _updateCms is a helper function to add new item to CountMinSketch structure. It
 * adds the item to the sketch, calculates its frequency, and updates the top-n
 * array. Finally it forms new CountMinSketch from updated sketch and updated top-n array.
 */
static CountMinSketch* _updateCms(CountMinSketch* currentCms, Datum newItem,
              TypeCacheEntry* newItemTypeCacheEntry)
{
	Datum detoastedItem = 0;

	/* If datum is toasted, detoast it */
	if (newItemTypeCacheEntry->typlen == -1)
	{
		detoastedItem = PointerGetDatum(PG_DETOAST_DATUM(newItem));
	}
	else
	{
		detoastedItem = newItem;
	}

	_updateCmsInPlace(currentCms, detoastedItem, newItemTypeCacheEntry);

	return currentCms;
}


/*
 * _updateCmsInPlace updates sketch inside CountMinSketch in-place with given item
 * and returns new estimated frequency for the given item.
 */
static uint64 _updateCmsInPlace(CountMinSketch* cms, Datum newItem,
                    TypeCacheEntry* newItemTypeCacheEntry)
{
	uint32 hashIndex = 0;
	uint64 hashValueArray[2] = {0, 0};
	StringInfo newItemString = makeStringInfo();
	uint64 newFrequency = 0;
	uint64 minFrequency = UINT64_MAX;

	/* Get hashed values for the given item */
	_convertDatumToBytes(newItem, newItemTypeCacheEntry, newItemString);
	MurmurHash3_x64_128(newItemString->data, newItemString->len, MURMUR_SEED,
	                    &hashValueArray);

	/*
	 * Estimate frequency of the given item from hashed values and calculate new
	 * frequency for this item.
	 */
	minFrequency = _cmsEstimateHashedItemFrequency(cms, hashValueArray);
	newFrequency = minFrequency + 1;

	/*
	 * We can create an independent hash function for each index by using two hash
	 * values from the Murmur Hash function. This is a standard technique from the
	 * hashing literature for the additional hash functions of the form
	 * g(x) = h1(x) + i * h2(x) and does not hurt the independence between hash
	 * function. For more information you can check this paper:
	 * http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
	 */
	for (hashIndex = 0; hashIndex < cms->sketchDepth; hashIndex++)
	{
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 widthIndex = hashValue % cms->sketchWidth;
		uint32 depthOffset = hashIndex * cms->sketchWidth;
		uint32 counterIndex = depthOffset + widthIndex;

		/*
		 * Selective update to decrease effect of collisions. We only update
		 * counters less than new frequency because other counters are bigger
		 * due to collisions.
		 */
		uint64 counterFrequency = cms->sketch[counterIndex];
		if (newFrequency > counterFrequency)
		{
			cms->sketch[counterIndex] = newFrequency;
		}
	}

	return newFrequency;
}


/*
 * _convertDatumToBytes converts datum to byte array and saves it in the given
 * datum string.
 */
static void _convertDatumToBytes(Datum datum, TypeCacheEntry* datumTypeCacheEntry,
                    StringInfo datumString)
{
	int16 datumTypeLength = datumTypeCacheEntry->typlen;
	bool datumTypeByValue = datumTypeCacheEntry->typbyval;
	Size datumSize = 0;

	if (datumTypeLength == -1)
	{
		datumSize = VARSIZE_ANY_EXHDR(DatumGetPointer(datum));
	}
	else
	{
		datumSize = datumGetSize(datum, datumTypeByValue, datumTypeLength);
	}

	if (datumTypeByValue)
	{
		appendBinaryStringInfo(datumString, (char *) &datum, datumSize);
	}
	else
	{
		appendBinaryStringInfo(datumString, VARDATA_ANY(datum), datumSize);
	}
}


/*
 * _cmsEstimateHashedItemFrequency is a helper function to get frequency
 * estimate of an item from it's hashed values.
 */
static uint64 _cmsEstimateHashedItemFrequency(CountMinSketch* cms, uint64* hashValueArray)
{
	uint32 hashIndex = 0;
	uint64 minFrequency = UINT64_MAX;

	for (hashIndex = 0; hashIndex < cms->sketchDepth; hashIndex++)
	{
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 widthIndex = hashValue % cms->sketchWidth;
		uint32 depthOffset = hashIndex * cms->sketchWidth;
		uint32 counterIndex = depthOffset + widthIndex;

		uint64 counterFrequency = cms->sketch[counterIndex];
		if (counterFrequency < minFrequency)
		{
			minFrequency = counterFrequency;
		}
	}

	return minFrequency;
}

/*
 * _cmsEstimateItemFrequency calculates estimated frequency for the given
 * item and returns it.
 */
static uint64 _cmsEstimateItemFrequency(CountMinSketch* cms, Datum item,
                             TypeCacheEntry* itemTypeCacheEntry)
{
	uint64 hashValueArray[2] = {0, 0};
	StringInfo itemString = makeStringInfo();
	uint64 frequency = 0;

	/* If datum is toasted, detoast it */
	if (itemTypeCacheEntry->typlen == -1)
	{
		Datum detoastedItem =  PointerGetDatum(PG_DETOAST_DATUM(item));
		_convertDatumToBytes(detoastedItem, itemTypeCacheEntry, itemString);
	}
	else
	{
		_convertDatumToBytes(item, itemTypeCacheEntry, itemString);
	}

	/*
	 * Calculate hash values for the given item and then get frequency estimate
	 * with these hashed values.
	 */
	MurmurHash3_x64_128(itemString->data, itemString->len, MURMUR_SEED, &hashValueArray);
	frequency = _cmsEstimateHashedItemFrequency(cms, hashValueArray);

	return frequency;
}

