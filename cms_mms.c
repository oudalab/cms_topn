/*-------------------------------------------------------------------------
 *
 * cms_mms.c
 *
 * This file contains the function definitions to perform top-n, point and
 * union queries by using the count-min sketch structure.
 *
 * It additionally contains implementation of an extension upon the
 * coun-min sketch structure called a min-mask sketch that uses bit masks
 * to identify tags associated with items instead of estimating frequency.
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
#define DEFAULT_TOPN_ITEM_SIZE 16
#define TOPN_ARRAY_OVERHEAD ARR_OVERHEAD_NONULLS(1)

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
	uint32 topnItemCount;
	uint32 topnItemSize;
	uint64 minFrequencyOfTopnItems;
	uint64 sketch[1];
} CountMinSketch;


/* 
 * MinMaskSketch is a similar data structure as the CountMinSketch but doesn't bother
 * implementing top-n related behavior because it is not concerned with frequency 
*/
typedef struct MinMaskSketch
{
	char length[4];
	uint32 sketchDepth;
	uint32 sketchWidth;
	uint64 sketch[1];
} MinMaskSketch;


/*
 * FrequentTopnItem is the struct to keep frequent items and their frequencies
 * together.
 * It is useful to sort the top-n items before returning in topn() function.
 */
typedef struct FrequentTopnItem
{
	Datum topnItem;
	uint64 topnItemFrequency;
} FrequentTopnItem;


/* Local functions forward declarations */
static CountMinSketch* _createCms(int32 topnItemCount, float8 errorBound, float8 confidenceInterval);
static CountMinSketch* _updateCms(CountMinSketch* currentCms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry);
static ArrayType* _topnArray(CountMinSketch* cms);
static uint64 _updateCmsInPlace(CountMinSketch* cms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry);
static void _convertDatumToBytes(Datum datum, TypeCacheEntry* datumTypeCacheEntry, StringInfo datumString);
static uint64 _cmsEstimateHashedItemFrequency(CountMinSketch* cms, uint64* hashValueArray);
static ArrayType* _updateTopnArray(CountMinSketch* cms, Datum candidateItem, TypeCacheEntry* itemTypeCacheEntry, uint64 itemFrequency, bool* topnArrayUpdated);
static uint64 _cmsEstimateItemFrequency(CountMinSketch* cms, Datum item, TypeCacheEntry* itemTypeCacheEntry);
static CountMinSketch* _formCms(CountMinSketch* cms, ArrayType* newTopnArray);
static CountMinSketch* _cmsUnion(CountMinSketch* firstCms, CountMinSketch* secondCms, TypeCacheEntry* itemTypeCacheEntry);
void _sortTopnItems(FrequentTopnItem* topnItemArray, int topnItemCount);
static MinMaskSketch* _createMms(float8 errorBound, float8 confidenceInterval);
static MinMaskSketch* _updateMms(MinMaskSketch* currentMms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry, uint64 newItemMask);
static uint64 _updateMmsInPlace(MinMaskSketch* mms, Datum newItem, TypeCacheEntry* newItemTypeCacheEntry, uint64 newItemMask);
static uint64 _mmsEstimateHashedItemMask(MinMaskSketch* mms, uint64* hashValueArray);
static uint64 _mmsEstimateItemMask(MinMaskSketch* mms, Datum item, TypeCacheEntry* itemTypeCacheEntry);
static uint64 _countSetBits(uint64 mask);

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

/* Count-min Sketch functions */
PG_FUNCTION_INFO_V1(cms_in);
PG_FUNCTION_INFO_V1(cms_out);
PG_FUNCTION_INFO_V1(cms_recv);
PG_FUNCTION_INFO_V1(cms_send);
PG_FUNCTION_INFO_V1(cms);
PG_FUNCTION_INFO_V1(cms_add);
PG_FUNCTION_INFO_V1(cms_add_agg);
PG_FUNCTION_INFO_V1(cms_add_agg_with_parameters);
PG_FUNCTION_INFO_V1(cms_union);
PG_FUNCTION_INFO_V1(cms_union_agg);
PG_FUNCTION_INFO_V1(cms_get_frequency);
PG_FUNCTION_INFO_V1(cms_info);
PG_FUNCTION_INFO_V1(cms_topn);

/* Min-mask sketch functions */
PG_FUNCTION_INFO_V1(mms_in);
PG_FUNCTION_INFO_V1(mms_out);
PG_FUNCTION_INFO_V1(mms_recv);
PG_FUNCTION_INFO_V1(mms_send);
PG_FUNCTION_INFO_V1(mms);
PG_FUNCTION_INFO_V1(mms_add);
PG_FUNCTION_INFO_V1(mms_get_mask);


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
	int32 topnItemCount = PG_GETARG_UINT32(0);
	float8 errorBound = PG_GETARG_FLOAT8(1);
	float8 confidenceInterval =  PG_GETARG_FLOAT8(2);

	CountMinSketch* cms = _createCms(topnItemCount, errorBound, confidenceInterval);

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
 * cms_add_agg is aggregate function to add items in a cms. It uses
 * default values for error bound, confidence interval and given top-n item
 * count to create initial cms. The first parameter for the CountMinSketch which
 * is updated during the aggregation, the second one is for the items to add and
 * third one specifies number of items to be kept in top-n array.
 */
Datum cms_add_agg(PG_FUNCTION_ARGS)
{
	CountMinSketch* currentCms = NULL;
	CountMinSketch* updatedCms = NULL;
	uint32 topnItemCount = PG_GETARG_UINT32(2);
	float8 errorBound = DEFAULT_ERROR_BOUND;
	float8 confidenceInterval = DEFAULT_CONFIDENCE_INTERVAL;
	Datum newItem = 0;
	TypeCacheEntry *newItemTypeCacheEntry = NULL;
	Oid newItemType = InvalidOid;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
		                errmsg("cms_add_agg called in non-aggregate context")));
	}

	/* Check whether cms is null and create if it is */
	if (PG_ARGISNULL(0))
	{
		currentCms = _createCms(topnItemCount, errorBound, confidenceInterval);
	}
	else
	{
		currentCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	}

	/* If new item is null, return current CountMinSketch */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentCms);
	}

	/*
	 * Keep type cache entry between subsequent calls in order to get rid of
	 * cache lookup overhead.
	 */
	newItem = PG_GETARG_DATUM(1);
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		newItemTypeCacheEntry = lookup_type_cache(newItemType, 0);
		fcinfo->flinfo->fn_extra = newItemTypeCacheEntry;
	}
	else
	{
		newItemTypeCacheEntry = fcinfo->flinfo->fn_extra;
	}

	updatedCms = _updateCms(currentCms, newItem, newItemTypeCacheEntry);

	PG_RETURN_POINTER(updatedCms);
}


/*
 * cms_add_agg_with_parameters is a aggregate function to add items. It
 * allows to specify parameters of created CountMinSketch structure. In addition to
 * cms_add_agg function, it takes error bound and confidence interval
 * parameters as the forth and fifth parameters.
 */
Datum cms_add_agg_with_parameters(PG_FUNCTION_ARGS)
{
	CountMinSketch* currentCms = NULL;
	CountMinSketch* updatedCms = NULL;
	uint32 topnItemCount = PG_GETARG_UINT32(2);
	float8 errorBound = PG_GETARG_FLOAT8(3);
	float8 confidenceInterval = PG_GETARG_FLOAT8(4);
	Datum newItem = 0;
	TypeCacheEntry *newItemTypeCacheEntry = NULL;
	Oid newItemType = InvalidOid;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
		                errmsg("cms_add_agg_with_parameters called in "
		                       "non-aggregate context")));
	}

	/* Check whether cms is null and create if it is */
	if (PG_ARGISNULL(0))
	{
		currentCms = _createCms(topnItemCount, errorBound, confidenceInterval);
	}
	else
	{
		currentCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	}

	/* If new item is null, return current CountMinSketch */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentCms);
	}

	/*
	 * Keep type cache entry between subsequent calls in order to get rid of
	 * cache lookup overhead.
	 */
	newItem = PG_GETARG_DATUM(1);
	if (fcinfo->flinfo->fn_extra == NULL)
	{
		newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
		newItemTypeCacheEntry = lookup_type_cache(newItemType, 0);
		fcinfo->flinfo->fn_extra = newItemTypeCacheEntry;
	}
	else
	{
		newItemTypeCacheEntry = fcinfo->flinfo->fn_extra;
	}

	updatedCms = _updateCms(currentCms, newItem, newItemTypeCacheEntry);

	PG_RETURN_POINTER(updatedCms);
}


/*
 * cms_union is a user-facing UDF which takes two cms and returns
 * their union.
 */
Datum cms_union(PG_FUNCTION_ARGS)
{
	CountMinSketch* firstCms = NULL;
	CountMinSketch* secondCms = NULL;
	CountMinSketch* newCms = NULL;
	ArrayType* firstTopnArray = NULL;
	Size firstTopnArrayLength = 0;
	TypeCacheEntry *itemTypeCacheEntry = NULL;

	/*
	 * If both cms is null, it returns null. If one of the cms's is
	 * null, it returns other cms.
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}
	else if (PG_ARGISNULL(0))
	{
		secondCms = (CountMinSketch*) PG_GETARG_VARLENA_P(1);
		PG_RETURN_POINTER(secondCms);
	}
	else if (PG_ARGISNULL(1))
	{
		firstCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
		PG_RETURN_POINTER(firstCms);
	}

	firstCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	secondCms = (CountMinSketch*) PG_GETARG_VARLENA_P(1);
	firstTopnArray = _topnArray(firstCms);
	firstTopnArrayLength = ARR_DIMS(firstTopnArray)[0];

	if (firstTopnArrayLength != 0)
	{
		Oid itemType = firstTopnArray->elemtype;
		itemTypeCacheEntry = lookup_type_cache(itemType, 0);
	}

	newCms = _cmsUnion(firstCms, secondCms, itemTypeCacheEntry);

	PG_RETURN_POINTER(newCms);
}


/*
 * _cmsUnion is a helper function for union operations. It first sums up two
 * sketchs and iterates through the top-n array of the second cms to update
 * the top-n of union.
 */
static CountMinSketch* _cmsUnion(CountMinSketch* firstCms, CountMinSketch* secondCms,
             TypeCacheEntry* itemTypeCacheEntry)
{
	ArrayType* firstTopnArray = _topnArray(firstCms);
	ArrayType* secondTopnArray = _topnArray(secondCms);
	ArrayType* newTopnArray = NULL;
	Size firstTopnArrayLength = ARR_DIMS(firstTopnArray)[0];
	Size secondTopnArrayLength = ARR_DIMS(secondTopnArray)[0];
	Size sketchSize = 0;
	CountMinSketch* newCms = NULL;
	uint32 topnItemIndex = 0;
	Datum topnItem = 0;
	ArrayIterator secondTopnArrayIterator = NULL;
	bool isNull = false;
	bool hasMoreItem = false;

	if (firstCms->sketchDepth != secondCms->sketchDepth ||
	    firstCms->sketchWidth != secondCms->sketchWidth ||
	    firstCms->topnItemCount != secondCms->topnItemCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("cannot merge cms with different parameters")));
	}

	if (firstTopnArrayLength == 0)
	{
		newCms = secondCms;
	}
	else if (secondTopnArrayLength == 0)
	{
		newCms = firstCms;
	}
	else if (ARR_ELEMTYPE(firstTopnArray) != ARR_ELEMTYPE(secondTopnArray))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("cannot merge cms of different types")));
	}
	else
	{
		/* For performance reasons we use first cms to merge two cms's */
		newCms = firstCms;
		newTopnArray = firstTopnArray;

		sketchSize = newCms->sketchDepth * newCms->sketchWidth;
		for (topnItemIndex = 0; topnItemIndex < sketchSize; topnItemIndex++)
		{
			newCms->sketch[topnItemIndex] += secondCms->sketch[topnItemIndex];
		}

		secondTopnArrayIterator = array_create_iterator(secondTopnArray, 0, NULL);

		/*
		 * One by one we add top-n items in second top-n array to top-n array of
		 * first(new) top-n array. As a result only top-n elements of two item
		 * list are kept.
		 */
		hasMoreItem = array_iterate(secondTopnArrayIterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			uint64 newItemFrequency = 0;
			bool topnArrayUpdated = false;

			newItemFrequency = _cmsEstimateItemFrequency(newCms, topnItem,
			                                                itemTypeCacheEntry);
			newTopnArray = _updateTopnArray(newCms, topnItem, itemTypeCacheEntry,
			                               newItemFrequency, &topnArrayUpdated);
			if (topnArrayUpdated)
			{
				newCms = _formCms(newCms, newTopnArray);
			}

			hasMoreItem = array_iterate(secondTopnArrayIterator, &topnItem, &isNull);
		}
	}

	return newCms;
}


/*
 * cms_union_agg is aggregate function to create union of cms.
 */
Datum cms_union_agg(PG_FUNCTION_ARGS)
{
	CountMinSketch* firstCms = NULL;
	CountMinSketch* secondCms = NULL;
	CountMinSketch* newCms = NULL;
	ArrayType* firstTopnArray = NULL;
	Size firstTopnArrayLength = 0;
	TypeCacheEntry* itemTypeCacheEntry = NULL;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
		                errmsg("cms_union_agg called in non-aggregate "
		                       "context")));
	}

	/*
	 * If both cms is null, it returns null. If one of the cms's is
	 * null, it returns other cms.
	 */
	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}
	else if (PG_ARGISNULL(0))
	{
		secondCms = (CountMinSketch*) PG_GETARG_VARLENA_P(1);
		PG_RETURN_POINTER(secondCms);
	}
	else if (PG_ARGISNULL(1))
	{
		firstCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
		PG_RETURN_POINTER(firstCms);
	}

	firstCms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	secondCms = (CountMinSketch*) PG_GETARG_VARLENA_P(1);
	firstTopnArray = _topnArray(firstCms);
	firstTopnArrayLength = ARR_DIMS(firstTopnArray)[0];

	/*
	 * Keep type cache entry between subsequent calls in order to get rid of
	 * cache lookup overhead.
	 */
	if (fcinfo->flinfo->fn_extra == NULL && firstTopnArrayLength != 0)
	{
		Oid itemType = firstTopnArray->elemtype;

		itemTypeCacheEntry = lookup_type_cache(itemType, 0);
		fcinfo->flinfo->fn_extra = itemTypeCacheEntry;
	}
	else
	{
		itemTypeCacheEntry = fcinfo->flinfo->fn_extra;
	}

	newCms = _cmsUnion(firstCms, secondCms, itemTypeCacheEntry);

	PG_RETURN_POINTER(newCms);
}


/*
 * cms_get_frequency is a user-facing UDF which returns the estimated frequency
 * of an item. The first parameter is for CountMinSketch and second is for the item to
 * return the frequency.
 */
Datum cms_get_frequency(PG_FUNCTION_ARGS)
{
	CountMinSketch* cms = (CountMinSketch*) PG_GETARG_VARLENA_P(0);
	ArrayType *topnArray = _topnArray(cms);
	Datum item = PG_GETARG_DATUM(1);
	Oid itemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *itemTypeCacheEntry = NULL;
	uint64 frequency = 0;

	if (itemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("could not determine input data types")));
	}

	if (topnArray != NULL && itemType != ARR_ELEMTYPE(topnArray))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("Not proper type for this cms")));
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
 * topn is a user-facing UDF which returns the top items and their frequencies.
 * It first gets the top-n structure and converts it into the ordered array of
 * FrequentTopnItem which keeps Datums and the frequencies in the first call.
 * Then, it returns an item and its frequency according to call counter. This
 * function requires a parameter for the type because PostgreSQL has strongly
 * typed system and the type of frequent items in returning rows has to be given.
 */
Datum cms_topn(PG_FUNCTION_ARGS)
{
	FuncCallContext* functionCallContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	Oid returningItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	CountMinSketch* cms = NULL;
	ArrayType* topnArray = NULL;
	int topnArrayLength = 0;
	Datum topnItem = 0;
	bool isNull = false;
	ArrayIterator topnIterator = NULL;
	bool hasMoreItem = false;
	int callCounter = 0;
	int maxCallCounter = 0;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext = NULL;
		Size topnArraySize = 0;
		TypeCacheEntry* itemTypeCacheEntry = NULL;
		int topnIndex = 0;
		Oid itemType = InvalidOid;
		FrequentTopnItem* sortedTopnArray = NULL;
		TupleDesc completeTupleDescriptor = NULL;

		functionCallContext = SRF_FIRSTCALL_INIT();
		if (PG_ARGISNULL(0))
		{
			SRF_RETURN_DONE(functionCallContext);
		}

		cms = (CountMinSketch*)  PG_GETARG_VARLENA_P(0);
		topnArray = _topnArray(cms);
		topnArrayLength = ARR_DIMS(topnArray)[0];

		/* If there is not any element in the array just return */
		if (topnArrayLength == 0)
		{
			SRF_RETURN_DONE(functionCallContext);
		}

		itemType = ARR_ELEMTYPE(topnArray);
		if (itemType != returningItemType)
		{
			elog(ERROR, "not a proper cms for the result type");
		}

		/* Switch to consistent context for multiple calls */
		oldcontext = MemoryContextSwitchTo(functionCallContext->multi_call_memory_ctx);
		itemTypeCacheEntry = lookup_type_cache(itemType, 0);
		functionCallContext->max_calls = topnArrayLength;

		/* Create an array to copy top-n items and sort them later */
		topnArraySize = sizeof(FrequentTopnItem) * topnArrayLength;
		sortedTopnArray = palloc0(topnArraySize);

		topnIterator = array_create_iterator(topnArray, 0, NULL);
		hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			FrequentTopnItem frequentTopnItem;
			frequentTopnItem.topnItem = topnItem;
			frequentTopnItem.topnItemFrequency =
			        _cmsEstimateItemFrequency(cms, topnItem, itemTypeCacheEntry);
			sortedTopnArray[topnIndex] = frequentTopnItem;

			hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
			topnIndex++;
		}

		_sortTopnItems(sortedTopnArray, topnArrayLength);
		functionCallContext->user_fctx = sortedTopnArray;

		get_call_result_type(fcinfo, &returningItemType, &tupleDescriptor);
		completeTupleDescriptor = BlessTupleDesc(tupleDescriptor);
		functionCallContext->tuple_desc = completeTupleDescriptor;
		MemoryContextSwitchTo(oldcontext);
	}

	functionCallContext = SRF_PERCALL_SETUP();
	maxCallCounter = functionCallContext->max_calls;
	callCounter = functionCallContext->call_cntr;

	if (callCounter < maxCallCounter)
	{
		Datum* tupleValues = (Datum*) palloc(2 * sizeof(Datum));
		HeapTuple topnItemTuple;
		Datum topnItemDatum = 0;
		char* tupleNulls = (char*) palloc0(2 * sizeof(char));
		FrequentTopnItem *sortedTopnArray = NULL;
		TupleDesc completeTupleDescriptor = NULL;

		sortedTopnArray = (FrequentTopnItem*) functionCallContext->user_fctx;
		tupleValues[0] = sortedTopnArray[callCounter].topnItem;
		tupleValues[1] = sortedTopnArray[callCounter].topnItemFrequency;

		/* Non-null attributes are indicated by a ' ' (space) */
		tupleNulls[0] = ' ';
		tupleNulls[1] = ' ';

		completeTupleDescriptor = functionCallContext->tuple_desc;
		topnItemTuple = heap_form_tuple(completeTupleDescriptor, tupleValues, tupleNulls);

		topnItemDatum = HeapTupleGetDatum(topnItemTuple);
		SRF_RETURN_NEXT(functionCallContext, topnItemDatum);
	}
	else
	{
		SRF_RETURN_DONE(functionCallContext);
	}
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
static CountMinSketch* _createCms(int32 topnItemCount, float8 errorBound, float8 confidenceInterval)
{
	CountMinSketch* cms = NULL;
	uint32 sketchWidth = 0;
	uint32 sketchDepth = 0;
	Size staticStructSize = 0;
	Size sketchSize = 0;
	Size topnItemsReservedSize = 0;
	Size topnArrayReservedSize = 0;
	Size totalCmsSize = 0;

	if (topnItemCount <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for cms"),
		                errhint("Number of top items has to be positive")));
	}
	else if (errorBound <= 0 || errorBound >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for cms"),
		                errhint("Error bound has to be between 0 and 1")));
	}
	else if (confidenceInterval <= 0 || confidenceInterval >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for cms_topn"),
		                errhint("Confidence interval has to be between 0 and 1")));
	}

	sketchWidth = (uint32) ceil(exp(1) / errorBound);
	sketchDepth = (uint32) ceil(log(1 / (1 - confidenceInterval)));
	sketchSize =  sizeof(uint64) * sketchDepth * sketchWidth;
	staticStructSize = sizeof(CountMinSketch);
	topnItemsReservedSize = topnItemCount * DEFAULT_TOPN_ITEM_SIZE;
	topnArrayReservedSize = TOPN_ARRAY_OVERHEAD + topnItemsReservedSize;
	totalCmsSize = staticStructSize + sketchSize + topnArrayReservedSize;

	cms = palloc0(totalCmsSize);
	cms->sketchDepth = sketchDepth;
	cms->sketchWidth = sketchWidth;
	cms->topnItemCount = topnItemCount;
	cms->topnItemSize = DEFAULT_TOPN_ITEM_SIZE;
	cms->minFrequencyOfTopnItems = 0;

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
	CountMinSketch* updatedCms = NULL;
	ArrayType* currentTopnArray = NULL;
	ArrayType* updatedTopnArray = NULL;
	Datum detoastedItem = 0;
	Oid newItemType = newItemTypeCacheEntry->type_id;
	Oid currentItemType = InvalidOid;
	uint64 newItemFrequency = 0;
	bool topnArrayUpdated = false;
	Size currentTopnArrayLength = 0;

	currentTopnArray = _topnArray(currentCms);
	currentTopnArrayLength = ARR_DIMS(currentTopnArray)[0];
	currentItemType = ARR_ELEMTYPE(currentTopnArray);

	if (currentTopnArrayLength != 0 && currentItemType != newItemType)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
		                errmsg("not proper type for this cms")));
	}

	/* If datum is toasted, detoast it */
	if (newItemTypeCacheEntry->typlen == -1)
	{
		detoastedItem = PointerGetDatum(PG_DETOAST_DATUM(newItem));
	}
	else
	{
		detoastedItem = newItem;
	}

	newItemFrequency = _updateCmsInPlace(currentCms, detoastedItem,
	                                       newItemTypeCacheEntry);
	updatedTopnArray = _updateTopnArray(currentCms, detoastedItem,
	                                   newItemTypeCacheEntry, newItemFrequency,
	                                   &topnArrayUpdated);

	/* We only form a new CountMinSketch only if top-n array is updated */
	if (topnArrayUpdated)
	{
		updatedCms = _formCms(currentCms, updatedTopnArray);
	}
	else
	{
		updatedCms = currentCms;
	}

	return updatedCms;
}


/*
 * TopnArray returns pointer for the ArrayType which is kept in CountMinSketch structure
 * by calculating its place with pointer arithmetic.
 */
static ArrayType* _topnArray(CountMinSketch* cms)
{
	Size staticSize = sizeof(CountMinSketch);
	Size sketchSize = sizeof(uint64) * cms->sketchDepth * cms->sketchWidth;
	Size sizeWithoutTopnArray = staticSize + sketchSize;
	ArrayType* topnArray = (ArrayType*) (((char*) cms) + sizeWithoutTopnArray);

	return topnArray;
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
 * _updateTopnArray is a helper function for the unions and inserts. It takes
 * given item and its frequency. If the item is not in the top-n array, it tries
 * to insert new item. If there is place in the top-n array, it insert directly.
 * Otherwise, it compares its frequency with the minimum of current items in the
 * array and updates top-n array if new frequency is bigger.
 */
static ArrayType* _updateTopnArray(CountMinSketch* cms, Datum candidateItem, TypeCacheEntry* itemTypeCacheEntry,
                uint64 itemFrequency, bool* topnArrayUpdated)
{
	ArrayType* currentTopnArray = _topnArray(cms);
	ArrayType* updatedTopnArray = NULL;
	int16 itemTypeLength = itemTypeCacheEntry->typlen;
	bool itemTypeByValue = itemTypeCacheEntry->typbyval;
	char itemTypeAlignment = itemTypeCacheEntry->typalign;
	uint64 minOfNewTopnItems = UINT64_MAX;
	bool candidateAlreadyInArray = false;
	int candidateIndex = -1;

	int currentArrayLength = ARR_DIMS(currentTopnArray)[0];
	if (currentArrayLength == 0)
	{
		Oid itemType = itemTypeCacheEntry->type_id;
		if (itemTypeCacheEntry->typtype == TYPTYPE_COMPOSITE)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			                errmsg("composite types are not supported")));
		}

		currentTopnArray = construct_empty_array(itemType);
	}

	/*
	 * If item frequency is smaller than min frequency of old top-n items,
	 * it cannot be in the top-n items and we insert it only if there is place.
	 * Otherwise, we need to check new minimum and whether it is in the top-n.
	 */
	if (itemFrequency <= cms->minFrequencyOfTopnItems)
	{
		if (currentArrayLength < cms->topnItemCount)
		{
			candidateIndex =  currentArrayLength + 1;
			minOfNewTopnItems = itemFrequency;
		}
	}
	else
	{
		ArrayIterator iterator = array_create_iterator(currentTopnArray, 0, NULL);
		Datum topnItem = 0;
		int topnItemIndex = 1;
		int minItemIndex = 1;
		bool hasMoreItem = false;
		bool isNull = false;

		/*
		 * Find the top-n item with minimum frequency to replace it with candidate
		 * item.
		 */
		hasMoreItem = array_iterate(iterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			uint64 topnItemFrequency = 0;

			/* Check if we already have this item in top-n array */
			candidateAlreadyInArray = datumIsEqual(topnItem, candidateItem,
			                                       itemTypeByValue, itemTypeLength);
			if (candidateAlreadyInArray)
			{
				minItemIndex = -1;
				break;
			}

			/* Keep track of minumum frequency item in the top-n array */
			topnItemFrequency = _cmsEstimateItemFrequency(cms, topnItem,
			                                                 itemTypeCacheEntry);
			if (topnItemFrequency < minOfNewTopnItems)
			{
				minOfNewTopnItems = topnItemFrequency;
				minItemIndex = topnItemIndex;
			}

			hasMoreItem = array_iterate(iterator, &topnItem, &isNull);
			topnItemIndex++;
		}

		/* If new item is not in the top-n and there is place, insert the item */
		if (!candidateAlreadyInArray && currentArrayLength < cms->topnItemCount)
		{
			minItemIndex = currentArrayLength + 1;
			minOfNewTopnItems = Min(minOfNewTopnItems, itemFrequency);
		}

		candidateIndex = minItemIndex;
	}

	/*
	 * If it is not in the top-n structure and its frequency bigger than minimum
	 * put into top-n instead of the item which has minimum frequency. If it is
	 * in top-n or not frequent items, do not change anything.
	 */
	if (!candidateAlreadyInArray && minOfNewTopnItems <= itemFrequency)
	{
		updatedTopnArray = array_set(currentTopnArray, 1, &candidateIndex,
		                             candidateItem, false, -1, itemTypeLength,
		                             itemTypeByValue, itemTypeAlignment);
		cms->minFrequencyOfTopnItems = minOfNewTopnItems;
		*topnArrayUpdated = true;
	}
	else
	{
		updatedTopnArray = currentTopnArray;
		*topnArrayUpdated = false;
	}

	return updatedTopnArray;
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


/*
 * _formCms copies current count-min sketch and new top-n array into a new
 * CountMinSketch. This function is called only when there is an update in top-n and it
 * only copies ArrayType part if allocated memory is enough for new ArrayType.
 * Otherwise, it allocates new memory and copies all CountMinSketch.
 */
static CountMinSketch* _formCms(CountMinSketch* cms, ArrayType* newTopnArray)
{
	Size staticSize = sizeof(CountMinSketch);
	Size sketchSize = cms->sketchDepth * cms->sketchWidth * sizeof(uint64);
	Size sizeWithoutTopnArray = staticSize + sketchSize;
	Size topnArrayReservedSize = VARSIZE(cms) - sizeWithoutTopnArray;
	Size newTopnArraySize = ARR_SIZE(newTopnArray);
	Size newCmsSize = 0;
	char *newCms = NULL;
	char *topnArrayOffset = NULL;

	/*
	 * Check whether we have enough memory for new top-n array. If not, we need
	 * to allocate more memory and copy CountMinSketch and new top-n array.
	 */
	if (newTopnArraySize > topnArrayReservedSize)
	{
		Size newItemsReservedSize = 0;
		Size newTopnArrayReservedSize = 0;
		uint32 topnItemCount = cms->topnItemCount;

		/*
		 * Calculate average top-n item size in the new top-n array and use
		 * two times of it for each top-n item while allocating new memory.
		 */
		Size averageTopnItemSize = (Size) (newTopnArraySize / topnItemCount);
		Size topnItemSize = averageTopnItemSize * 2;
		cms->topnItemSize = topnItemSize;

		newItemsReservedSize = topnItemCount * topnItemSize;
		newTopnArrayReservedSize = TOPN_ARRAY_OVERHEAD + newItemsReservedSize;
		newCmsSize = sizeWithoutTopnArray + newTopnArrayReservedSize;
		newCms = palloc0(newCmsSize);

		/* First copy until to top-n array */
		memcpy(newCms, (char*)cms, sizeWithoutTopnArray);

		/* Set size of new CmsTopn */
		SET_VARSIZE(newCms, newCmsSize);
	}
	else
	{
		newCms = (char*)cms;
	}

	/* Finally copy new top-n array */
	topnArrayOffset = ((char*) newCms) + sizeWithoutTopnArray;
	memcpy(topnArrayOffset, newTopnArray, ARR_SIZE(newTopnArray));

	return (CountMinSketch*) newCms;
}


/*
 * _sortTopnItems sorts the top-n items in place according to their frequencies
 * by using selection sort.
 */
void _sortTopnItems(FrequentTopnItem* topnItemArray, int topnItemCount)
{
	int currentItemIndex = 0;
	for (currentItemIndex = 0; currentItemIndex < topnItemCount; currentItemIndex++)
	{
		FrequentTopnItem swapFrequentTopnItem;
		int candidateItemIndex = 0;

		/* Use current top-n item as default max */
		uint64 maxItemFrequency = topnItemArray[currentItemIndex].topnItemFrequency;
		int maxItemIndex = currentItemIndex;

		for (candidateItemIndex = currentItemIndex + 1;
		     candidateItemIndex < topnItemCount; candidateItemIndex++)
		{
			uint64 candidateItemFrequency =
			        topnItemArray[candidateItemIndex].topnItemFrequency;
			if(candidateItemFrequency > maxItemFrequency)
			{
				maxItemFrequency = candidateItemFrequency;
				maxItemIndex = candidateItemIndex;
			}
		}

		swapFrequentTopnItem = topnItemArray[maxItemIndex];
		topnItemArray[maxItemIndex] = topnItemArray[currentItemIndex];
		topnItemArray[currentItemIndex] = swapFrequentTopnItem;
	}
}

/* ----- Min-mask sketch functionality ----- */


/* mms_in creates mms from printable representation */
Datum mms_in(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));

	return datum;
}


/* mms_out converts mms to printable representation */
Datum mms_out(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));

	PG_RETURN_CSTRING(datum);
}


/* mms_recv creates mms from external binary format */
Datum mms_recv(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));

	return datum;
}


/* mms_send converts mms to external binary format */
Datum mms_send(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteasend, PG_GETARG_DATUM(0));

	return datum;
}


/* 
 * mms is a user-facing UDF that creates a min-mask sketch structure
 * with given paramters. Note there is no top-n parameter for this function.
 * errorBound and confidenceInterval have default values so they are
 * optional parameters.
 */
Datum mms(PG_FUNCTION_ARGS)
{
	float8 errorBound = PG_GETARG_FLOAT8(0);
	float8 confidenceInterval =  PG_GETARG_FLOAT8(1);

	MinMaskSketch* mms = _createMms(errorBound, confidenceInterval);

	PG_RETURN_POINTER(mms);
}


/*
 * mms_add is a user-facing UDF that adds an item to an existing min-mask sketch.
 * This function differs from the cms_add counter-part because it also needs the
 * new item's associated mask to add to the sketch.
 */
Datum mms_add(PG_FUNCTION_ARGS)
{
	MinMaskSketch* currentMms = NULL;
	MinMaskSketch* updatedMms = NULL;
	Datum newItem = 0;
	TypeCacheEntry* newItemTypeCacheEntry = NULL;
	Oid newItemType = InvalidOid;
	uint32 newItemMask = 0;

	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}
	else
	{
		currentMms = (MinMaskSketch*) PG_GETARG_VARLENA_P(0);
	}

	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentMms);
	}

	newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (newItemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("could not determine input data type")));
	}

	newItem = PG_GETARG_DATUM(1);
	newItemTypeCacheEntry = lookup_type_cache(newItemType, 0);

	newItemMask = PG_GETARG_INT64(2);

	updatedMms = _updateMms(currentMms, newItem, newItemTypeCacheEntry, newItemMask);

	PG_RETURN_POINTER(updatedMms);
}


/* mms_get_mask is a user-facing UDF that retrieves the estimated mask of a given item. */
Datum mms_get_mask(PG_FUNCTION_ARGS)
{
	MinMaskSketch* mms = (MinMaskSketch*) PG_GETARG_VARLENA_P(0);
	Datum item = PG_GETARG_DATUM(1);
	Oid itemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry* itemTypeCacheEntry = NULL;
	uint32 mask = 0;

	if (itemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("could not determine input data types")));
	}

	itemTypeCacheEntry = lookup_type_cache(itemType, 0);
	mask = _mmsEstimateItemMask(mms, item, itemTypeCacheEntry);

	PG_RETURN_INT64(mask);
}


/* 
 * _createMms creates a MinMaskSketch with given parameters. Its behavior is very similar
 * to the _createCms.
 */
static MinMaskSketch* _createMms(float8 errorBound, float8 confidenceInterval)
{
	MinMaskSketch* mms = NULL;
	uint32 sketchWidth = 0;
	uint32 sketchDepth = 0;
	Size staticStructSize = 0;
	Size sketchSize = 0;
	Size totalMmsSize = 0;
	
	if (errorBound <= 0 || errorBound >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for mms"),
		                errhint("Error bound has to be between 0 and 1")));
	}
	else if (confidenceInterval <= 0 || confidenceInterval >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		                errmsg("invalid parameters for mms"),
		                errhint("Confidence interval has to be between 0 and 1")));
	}

	sketchWidth = (uint32) ceil(exp(1) / errorBound);
	sketchDepth = (uint32) ceil(log(1 / (1 - confidenceInterval)));
	sketchSize =  sizeof(uint64) * sketchDepth * sketchWidth;
	staticStructSize = sizeof(MinMaskSketch);
	totalMmsSize = staticStructSize + sketchSize;

	mms = palloc0(totalMmsSize);
	mms->sketchDepth = sketchDepth;
	mms->sketchWidth = sketchWidth;

	SET_VARSIZE(mms, totalMmsSize);

	return mms;
}


/*
 * _updateMms is a helper function to add a new item to the min-mask sketch structure.
 * It differs from _updateCms in that it needs to accept the new item's bitmask to update
 * the sketch correctly.
 */
static MinMaskSketch* _updateMms(MinMaskSketch* currentMms, Datum newItem,
              TypeCacheEntry* newItemTypeCacheEntry, uint64 newItemMask)
{
	MinMaskSketch* updatedMms = NULL;
	Datum detoastedItem = 0;
	// Oid newItemType = newItemTypeCacheEntry->type_id;
	// Oid currentItemType = InvalidOid;

	/* TODO -- Check item type correctness */

	if (newItemTypeCacheEntry->typlen == -1)
	{
		detoastedItem = PointerGetDatum(PG_DETOAST_DATUM(newItem));
	}
	else
	{
		detoastedItem = newItem;
	}

	newItemMask = _updateMmsInPlace(currentMms, detoastedItem,
	                                       newItemTypeCacheEntry, newItemMask);
	
	updatedMms = currentMms;

	return updatedMms;
}


/* 
 * _updateMmsInPlace updates the sketch inside the MinMaskSketch in-place by
 * adding the new item and returns the new mask for that item.
 */
static uint64 _updateMmsInPlace(MinMaskSketch* mms, Datum newItem,
                    TypeCacheEntry* newItemTypeCacheEntry, uint64 newItemMask)
{
	uint32 hashIndex = 0;
	uint64 hashValueArray[2] = {0, 0};
	StringInfo newItemString = makeStringInfo();
	uint64 newMask = 0;
	uint64 minMask = UINT64_MAX;

	_convertDatumToBytes(newItem, newItemTypeCacheEntry, newItemString);
	MurmurHash3_x64_128(newItemString->data, newItemString->len, MURMUR_SEED,
	                    &hashValueArray);
	
	minMask = _mmsEstimateHashedItemMask(mms, hashValueArray);
	newMask = minMask | newItemMask;
	
	for (hashIndex = 0; hashIndex < mms->sketchDepth; hashIndex++)
	{
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 widthIndex = hashValue % mms->sketchWidth;
		uint32 depthOffset = hashIndex * mms->sketchWidth;
		uint32 counterIndex = depthOffset + widthIndex;
	
		uint64 counterMask = mms->sketch[counterIndex];
		if (_countSetBits(newMask) > _countSetBits(counterMask))
		{
			mms->sketch[counterIndex] = newMask;
		}
	}

	return newMask;
}


/* _mmsEstimateHashedItemMask gets the bitmask of an item from its hashed values. */
static uint64 _mmsEstimateHashedItemMask(MinMaskSketch* mms, uint64* hashValueArray)
{
	uint32 hashIndex = 0;
	uint64 minMask = UINT64_MAX;

	for (hashIndex = 0; hashIndex < mms->sketchDepth; hashIndex++)
	{
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 widthIndex = hashValue % mms->sketchWidth;
		uint32 depthOffset = hashIndex * mms->sketchWidth;
		uint32 counterIndex = depthOffset + widthIndex;

		uint64 counterMask = mms->sketch[counterIndex];
		if (_countSetBits(counterMask) < _countSetBits(minMask))
		{
			minMask = counterMask;
		}
	}

	return minMask;
}


/* _mmsEstimateItemMask estimates the bitmask value for the given item and returns the bitmask. */
static uint64 _mmsEstimateItemMask(MinMaskSketch* mms, Datum item,
                             TypeCacheEntry* itemTypeCacheEntry)
{
	uint64 hashValueArray[2] = {0, 0};
	StringInfo itemString = makeStringInfo();
	uint64 mask = 0;

	if (itemTypeCacheEntry->typlen == -1)
	{
		Datum detoastedItem =  PointerGetDatum(PG_DETOAST_DATUM(item));
		_convertDatumToBytes(detoastedItem, itemTypeCacheEntry, itemString);
	}
	else
	{
		_convertDatumToBytes(item, itemTypeCacheEntry, itemString);
	}
	
	MurmurHash3_x64_128(itemString->data, itemString->len, MURMUR_SEED, &hashValueArray);
	mask = _mmsEstimateHashedItemMask(mms, hashValueArray);

	return mask;
}


/* _countSetBits counts the number of set bits (1's) in the given binary number and returns the count. */
static uint64 _countSetBits(uint64 mask)
{
	int count = 0;
	while(mask)
	{
		count += mask & 1;
		mask >>= 1;
	}

	return count;
}
