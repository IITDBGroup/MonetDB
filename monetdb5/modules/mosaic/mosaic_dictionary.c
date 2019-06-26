/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */


/*
 * 2014-2016 author Martin Kersten
 * Global dictionary encoding
 * Index value zero is not used to easy detection of filler values
 * The dictionary index size is derived from the number of entries covered.
 * It leads to a compact n-bit representation.
 * Floating points are not expected to be replicated 
 * A limit of 256 elements is currently assumed.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_dictionary.h"
#include "mosaic_private.h"

bool MOStypes_dictionary(BAT* b) {
	switch(ATOMbasetype(getBatType(b->ttype))){
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
	case TYPE_flt: return true;
	case TYPE_dbl: return true;
#ifdef HAVE_HGE
	case TYPE_hge: return true;
#endif
	case  TYPE_str:
		switch(b->twidth){
		case 1: return true;
		case 2: return true;
		case 4: return true;
		case 8: return true;
		}
		break;
	}

	return false;
}

void
MOSadvance_dictionary(MOStask task)
{
	int *dst = (int*)  MOScodevector(task);
	BUN cnt = MOSgetCnt(task->blk);
	long bytes;

	assert(cnt > 0);
	task->start += (oid) cnt;
	task->stop = task->stop;
	bytes =  (long) (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, int)); 
}

void
MOSlayout_dictionary_hdr(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng zero=0;
	int i;
	char buf[BUFSIZ];
	char bufv[BUFSIZ];

	for(i=0; i< task->hdr->dictsize; i++){
		snprintf(buf, BUFSIZ,"dictionary[%d]",i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &zero, false) != GDK_SUCCEED ||
			BUNappend(binput, &zero, false) != GDK_SUCCEED ||
			BUNappend(boutput, &task->hdr->dictfreq[i], false) != GDK_SUCCEED ||
			BUNappend(bproperties, bufv, false) != GDK_SUCCEED)
		return;
	}
}


void
MOSlayout_dictionary(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	output =  MosaicBlkSize + (cnt * task->hdr->bits)/8 + (((cnt * task->hdr->bits) %8) != 0);
	if( BUNappend(btech, "dictionary blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_dictionary(MOStask task)
{
	MOSadvance_dictionary(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define MOSfind(Res,DICT,VAL,F,L)\
{ int m,f= F, l=L; \
   while( l-f > 0 ) { \
	m = f + (l-f)/2;\
	if ( VAL < DICT[m] ) l=m-1; else f= m;\
	if ( VAL > DICT[m] ) f=m+1; else l= m;\
   }\
   Res= f;\
}

#define estimateDict(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	if( task->range[MOSAIC_DICT] > task->start){\
		i = task->range[MOSAIC_DICT] - task->start;\
		if ( i > MOSAICMAXCNT ) i = MOSAICMAXCNT;\
		if( i * sizeof(TPE) <= wordaligned( MosaicBlkSize + (i*hdr->bits)/8,TPE))\
			return 0.0;\
		if( task->dst +  wordaligned(MosaicBlkSize + (i*hdr->bits)/8,sizeof(TPE)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
			return 0.0;\
		if(i) factor = ((flt) i * sizeof(TPE))/ wordaligned(MosaicBlkSize + sizeof(int) + (i*hdr->bits)/8,TPE);\
		return factor;\
	}\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,hdr->dict.val##TPE,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || hdr->dict.val##TPE[j] != *val )\
			break;\
	}\
	if( i * sizeof(TPE) <= wordaligned( MosaicBlkSize + (i * hdr->bits)/8 ,TPE))\
		return 0.0;\
	if(i) factor = (flt) ((int)i * sizeof(TPE)) / wordaligned( MosaicBlkSize + (i * hdr->bits)/8,TPE);\
}

// Create a larger dictionary buffer then we allow for in the mosaic header first
// Store the most frequent ones in the compressed heap header directly based on estimated savings
// Improve by using binary search rather then linear scan
#define TMPDICT 16*256

#define makeDict(TPE)\
{	TPE *val,v,w;\
	BUN limit = task->stop - task->start > TMPDICT ? TMPDICT:  task->stop - task->start;\
	BAT* bsample = BATsample_with_seed(task->bsrc, limit, (16-07-1985));\
	lng cw,cv; \
	for(i = 0; i< limit; i++){\
		oid sample = BUNtoid(bsample,i);\
		val = ((TPE*)task->src) + (sample - task->bsrc->hseqbase);\
		MOSfind(j,dict.val##TPE,*val,0,dictsize);\
		if(j == dictsize && dictsize == 0 ){\
			dict.val##TPE[j]= *val;\
			cnt[j] = 1;\
			dictsize++;\
		} else  \
		if( dictsize < TMPDICT && dict.val##TPE[j] != *val){\
			w= *val; cw= 1;\
			for( ; j< dictsize; j++)\
			if (dict.val##TPE[j] > w){\
				v =dict.val##TPE[j];\
				dict.val##TPE[j]= w;\
				w = v;\
				cv = cnt[j];\
				cnt[j]= cw;\
				cw = cv;\
			}\
			dictsize++;\
			dict.val##TPE[j]= w;\
			cnt[j] = 1;\
		} else if (dictsize < TMPDICT) cnt[j]++;\
	}\
	BBPunfix(bsample->batCacheid);\
	/* find the 256 most frequent values and save them in the mosaic header */ \
	if( dictsize <= 256){ \
		memcpy((char*)&task->hdr->dict,  (char*)&dict, dictsize * sizeof(TPE)); \
		memcpy((char*)task->hdr->dictfreq,  (char*)&cnt, dictsize * sizeof(lng)); \
		hdr->dictsize = dictsize; \
	} else { \
		/* brute force search of the top-k */ \
		for(j=0; j< 256; j++){ \
			for(k=0; k< dictsize; k++) \
			if( keep[k]==0){ \
				if( cnt[k]> cnt[max]) max = k; \
			} \
			keep[max]=1; \
		} \
		/* keep the top-k, in order */ \
		for( j=k=0; k<dictsize; k++) \
		if( keep[k]){ \
			task->hdr->dict.val##TPE[j] = dict.val##TPE[k]; \
			task->hdr->dictfreq[j] = cnt[k]; \
			j++; \
		} \
		hdr->dictsize = j; \
		assert(j<=256); \
	} \
}


/* Take a larger sample before fixing the dictionary */
void
MOScreatedictionary(MOStask task)
{
	BUN i;
	int j, k, max;
	MosaicHdr hdr = task->hdr;
    union{
        bte valbte[TMPDICT];
        sht valsht[TMPDICT];
        int valint[TMPDICT];
        lng vallng[TMPDICT];
        oid valoid[TMPDICT];
        flt valflt[TMPDICT];
        dbl valdbl[TMPDICT];
#ifdef HAVE_HGE
        hge valhge[TMPDICT];
#endif
    }dict;
	lng cnt[TMPDICT];
	bte keep[TMPDICT];
	int dictsize = 0;

	memset((char*) &dict, 0, TMPDICT * sizeof(lng));
	memset((char*) cnt, 0, sizeof(cnt));
	memset((char*) keep, 0, sizeof(keep));

	hdr->dictsize = dictsize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: makeDict(bte); break;
	case TYPE_sht: makeDict(sht); break;
	case TYPE_int: makeDict(int); break;
	case TYPE_lng: makeDict(lng); break;
	case TYPE_oid: makeDict(oid); break;
	case TYPE_flt: makeDict(flt); break;
	case TYPE_dbl: makeDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: makeDict(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: makeDict(bte); break;
		case 2: makeDict(sht); break;
		case 4: makeDict(int); break;
		case 8: makeDict(lng); break;
		}
		break;
	}
	/* calculate the bit-width */
	hdr->bits = 1;
	hdr->mask =1;
	for( j=2 ; j < dictsize; j *=2){
		hdr->bits++;
		hdr->mask = (hdr->mask <<1) | 1;
	}
}

// calculate the expected reduction using DICT in terms of elements compressed
flt
MOSestimate_dictionary(MOStask task)
{	
	BUN i = 0;
	int j;
	flt factor= 0.0;
	MosaicHdr hdr = task->hdr;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimateDict(bte); break; 
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_int: estimateDict(int); break;
	case TYPE_oid: estimateDict(oid); break;
	case TYPE_lng: estimateDict(lng); break;
	case TYPE_flt: estimateDict(flt); break;
	case TYPE_dbl: estimateDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: //estimateDict(bte); break;
{	bte *val = ((bte*)task->src) + task->start;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	if( task->range[MOSAIC_DICT] > task->start){\
		i = task->range[MOSAIC_DICT] - task->start;\
		if ( i > MOSAICMAXCNT ) i = MOSAICMAXCNT;\
		if( i * sizeof(bte) <= wordaligned( MosaicBlkSize + i,bte))\
			return 0.0;\
		if( task->dst +  wordaligned(MosaicBlkSize + i,sizeof(bte)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
			return 0.0;\
		if(i) factor = (flt) ((int)i * sizeof(bte)) / wordaligned( MosaicBlkSize + (i*hdr->bits)/8,bte);
		return factor;\
	}\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,hdr->dict.valbte,*val,0,hdr->dictsize);\
		if( j == hdr->dictsize || hdr->dict.valbte[j] != *val )\
			break;\
	}\
	if( i * sizeof(bte) <= wordaligned( MosaicBlkSize + (i * hdr->bits)/8,bte))\
		return 0.0;\
	if(i) factor = (flt) ((int) i * sizeof(bte)) / wordaligned( MosaicBlkSize + (i*hdr->bits)/8,bte);\
}
break;
		case 2: estimateDict(sht); break;
		case 4: estimateDict(int); break;
		case 8: estimateDict(lng); break;
		}
		break;
	}
	task->factor[MOSAIC_DICT] = factor;
	task->range[MOSAIC_DICT] = task->start + i;
	return factor; 
}

// insert a series of values into the compressor block using dictionary

#define DICTcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start;\
	BitVector base = (BitVector) MOScodevector(task);\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(i =0; i<limit; i++, val++){\
		MOSfind(j,task->hdr->dict.val##TPE,*val,0,hdr->dictsize);\
		if(j == hdr->dictsize || task->hdr->dict.val##TPE[j] !=  *val) \
			break;\
		else {\
			hdr->checksum.sum##TPE += *val;\
			hdr->dictfreq[j]++;\
			MOSincCnt(blk,1);\
			setBitVector(base,i,hdr->bits,(unsigned int)j);\
		}\
	}\
	assert(i);\
}


void
MOScompress_dictionary(MOStask task)
{
	BUN i;
	int j;
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;

	task->dst = MOScodevector(task);

	MOSsetTag(blk,MOSAIC_DICT);
	MOSsetCnt(blk,0);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTcompress(sht); break;
	case TYPE_sht: DICTcompress(sht); break;
	case TYPE_int: DICTcompress(int); break;
	case TYPE_lng: DICTcompress(lng); break;
	case TYPE_oid: DICTcompress(oid); break;
	case TYPE_flt: DICTcompress(flt); break;
	case TYPE_dbl: DICTcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTcompress(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: DICTcompress(bte); break;
		case 2: DICTcompress(sht); break;
		case 4: DICTcompress(int); break;
		case 8: DICTcompress(lng); break;
		}
		break;
	}
}

// the inverse operator, extend the src

#define DICTdecompress(TPE)\
{	BUN lim = MOSgetCnt(blk);\
	base = (BitVector) MOScodevector(task);\
	for(i = 0; i < lim; i++){\
		j= getBitVector(base,i,(int) hdr->bits); \
		((TPE*)task->src)[i] = task->hdr->dict.val##TPE[j];\
		hdr->checksum2.sum##TPE += task->hdr->dict.val##TPE[j];\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_dictionary(MOStask task)
{
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;
	BUN i;
	int j;
	BitVector base;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: DICTdecompress(bte); break;
	case TYPE_sht: DICTdecompress(sht); break;
	case TYPE_int: DICTdecompress(int); break;
	case TYPE_lng: DICTdecompress(lng); break;
	case TYPE_oid: DICTdecompress(oid); break;
	case TYPE_flt: DICTdecompress(flt); break;
	case TYPE_dbl: DICTdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTdecompress(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: DICTdecompress(bte); break;
		case 2: DICTdecompress(sht); break;
		case 4: DICTdecompress(int); break;
		case 8: DICTdecompress(lng); break;
		}
		break;
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define select_dictionary_str(TPE) \
	throw(MAL,"mosaic.dictionary","TBD");
#define select_dictionary(TPE) {\
	base = (BitVector) MOScodevector(task);\
	if( !*anti){\
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh )) &&\
						((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			/* nothing is matching */\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				j= getBitVector(base,i,(int) hdr->bits); \
				cmp  =  ((*hi && task->hdr->dict.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && task->hdr->dict.val##TPE[j] < *(TPE*)hgh )) &&\
						((*li && task->hdr->dict.val##TPE[j] >= * (TPE*)low ) || (!*li && task->hdr->dict.val##TPE[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSselect_dictionary(MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN i, first,last;
	MosaicHdr hdr = task->hdr;
	int cmp;
	bte j;
	BitVector base;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_dictionary(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: select_dictionary(bte); break;
	case TYPE_sht: select_dictionary(sht); break;
	case TYPE_int: select_dictionary(int); break;
	case TYPE_lng: select_dictionary(lng); break;
	case TYPE_oid: select_dictionary(oid); break;
	case TYPE_flt: select_dictionary(flt); break;
	case TYPE_dbl: select_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_dictionary(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: select_dictionary_str(bte); break;
		case 2: select_dictionary_str(sht); break;
		case 4: select_dictionary_str(int); break;
		case 8: select_dictionary_str(lng); break;
		}
		break;
	}
	MOSskip_dictionary(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_dictionary_str(TPE)\
	throw(MAL,"mosaic.dictionary","TBD");

#define thetaselect_dictionary(TPE)\
{ 	TPE low,hgh;\
	base = (BitVector) MOScodevector(task);\
	low= hgh = TPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) val;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) val;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TPE*) val;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TPE*) val;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		hgh= low= *(TPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) val;\
	} \
	for( ; first < last; first++){\
		MOSskipit();\
		j= getBitVector(base,first,(int) hdr->bits); \
		if( (is_nil(TPE, low) || task->hdr->dict.val##TPE[j] >= low) && (task->hdr->dict.val##TPE[j] <= hgh || is_nil(TPE, hgh)) ){\
			if ( !anti) {\
				*o++ = (oid) first;\
			}\
		} else\
			if( anti){\
				*o++ = (oid) first;\
			}\
	}\
} 

str
MOSthetaselect_dictionary( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	MosaicHdr hdr = task->hdr;
	bte j;
	BitVector base;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_dictionary(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: thetaselect_dictionary(bte); break;
	case TYPE_sht: thetaselect_dictionary(sht); break;
	case TYPE_int: thetaselect_dictionary(int); break;
	case TYPE_lng: thetaselect_dictionary(lng); break;
	case TYPE_oid: thetaselect_dictionary(oid); break;
	case TYPE_flt: thetaselect_dictionary(flt); break;
	case TYPE_dbl: thetaselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_dictionary(hge); break;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: thetaselect_dictionary_str(bte); break;
		case 2: thetaselect_dictionary_str(sht); break;
		case 4: thetaselect_dictionary_str(int); break;
		case 8: thetaselect_dictionary_str(lng); break;
		}
		break;
	}
	MOSskip_dictionary(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_dictionary_str(TPE)\
	throw(MAL,"mosaic.dictionary","TBD");
#define projection_dictionary(TPE)\
{	TPE *v;\
	base = (BitVector) MOScodevector(task);\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		j= getBitVector(base,i,(int) hdr->bits); \
		*v++ = task->hdr->dict.val##TPE[j];\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_dictionary( MOStask task)
{
	BUN i,first,last;
	MosaicHdr hdr = task->hdr;
	unsigned short j;
	BitVector base;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_dictionary(bte); break;
		case TYPE_sht: projection_dictionary(sht); break;
		case TYPE_lng: projection_dictionary(lng); break;
		case TYPE_oid: projection_dictionary(oid); break;
		case TYPE_flt: projection_dictionary(flt); break;
		case TYPE_dbl: projection_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_dictionary(hge); break;
#endif
		case TYPE_int:
		{	int *v;
			base  = (BitVector) (((char*) task->blk) + MosaicBlkSize);
			v= (int*) task->src;
			for(i=0 ; first < last; first++, i++){
				MOSskipit();
				j= getBitVector(base,i,(int) hdr->bits); \
				*v++ = task->hdr->dict.valint[j];
				task->cnt++;
			}
			task->src = (char*) v;
		}
		break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: projection_dictionary_str(bte); break;
		case 2: projection_dictionary_str(sht); break;
		case 4: projection_dictionary_str(int); break;
		case 8: projection_dictionary_str(lng); break;
		}
		break;
	}
	MOSskip_dictionary(task);
	return MAL_SUCCEED;
}

#define join_dictionary_str(TPE)\
	throw(MAL,"mosaic.dictionary","TBD");

#define join_dictionary(TPE)\
{	TPE  *w;\
	BitVector base = (BitVector) MOScodevector(task);\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->stop; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			j= getBitVector(base,i,(int) hdr->bits); \
			if ( *w == task->hdr->dict.val##TPE[j]){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED)\
				throw(MAL,"mosaic.dictionary",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

str
MOSjoin_dictionary( MOStask task)
{
	BUN i,n,limit;
	oid o, oo;
	MosaicHdr hdr = task->hdr;
	int j;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_dictionary(bte); break;
		case TYPE_sht: join_dictionary(sht); break;
		case TYPE_int: join_dictionary(int); break;
		case TYPE_lng: join_dictionary(lng); break;
		case TYPE_oid: join_dictionary(oid); break;
		case TYPE_flt: join_dictionary(flt); break;
		case TYPE_dbl: join_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_dictionary(hge); break;
#endif
		case TYPE_str:
			switch(task->bsrc->twidth){
			case 1: join_dictionary_str(bte); break;
			case 2: join_dictionary_str(sht); break;
			case 4: join_dictionary_str(int); break;
			case 8: join_dictionary_str(lng); break;
			}
			break;
	}
	MOSskip_dictionary(task);
	return MAL_SUCCEED;
}
