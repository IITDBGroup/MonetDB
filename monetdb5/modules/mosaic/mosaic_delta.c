/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Byte-wise delta encoding for SHT,INT,LNG, OID, WRD, STR-offsets, TIMESTAMP
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_delta.h"
#include "mosaic_private.h"

//#define _DEBUG_MOSAIC_

void
MOSadvance_delta(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	(void) cntxt;

	task->start += MOSgetCnt(blk);
	task->stop = task->elm;
	switch(task->type){
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(sizeof(sht) + MosaicBlkSize + MOSgetCnt(blk)-1,sht)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(sizeof(int) + MosaicBlkSize + MOSgetCnt(blk)-1,int)); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(sizeof(oid) + MosaicBlkSize + MOSgetCnt(blk)-1,oid)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(sizeof(lng) + MosaicBlkSize + MOSgetCnt(blk)-1,lng)); break ;
	//case TYPE_flt: case TYPE_dbl: to be looked into.
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) blk) + wordaligned(MosaicBlkSize + sizeof(hge) + MOSgetCnt(blk)-1,hge)); break ;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 2: task->blk = (MosaicBlk)( ((char*) blk) + wordaligned(sizeof(sht)+ MosaicBlkSize + MOSgetCnt(blk)-1,sht)); break ;
		case 4: task->blk = (MosaicBlk)( ((char*) blk) + wordaligned(sizeof(int)+ MosaicBlkSize + MOSgetCnt(blk)-1,int)); break ;
		case 8: task->blk = (MosaicBlk)( ((char*) blk) + wordaligned(sizeof(lng)+ MosaicBlkSize + MOSgetCnt(blk)-1,lng)); break ;
		}
		break;
	default:
		if( task->type == TYPE_timestamp)
			task->blk = (MosaicBlk)( ((char*) blk) + MosaicBlkSize + wordaligned(sizeof(timestamp) + MOSgetCnt(blk)-1,timestamp)); 
		if( task->type == TYPE_date)
			task->blk = (MosaicBlk)( ((char*) blk) + MosaicBlkSize + wordaligned(sizeof(date) + MOSgetCnt(blk)-1,date)); 
		if( task->type == TYPE_daytime)
			task->blk = (MosaicBlk)( ((char*) blk) + MosaicBlkSize + wordaligned(sizeof(daytime) + MOSgetCnt(blk)-1,daytime)); 
	}
}


void
MOSdump_delta(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	mnstr_printf(cntxt->fdout,"#delta "BUNFMT"\n", MOSgetCnt(blk));
}

void
MOSlayout_delta(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	(void) cntxt;
	input = cnt * ATOMsize(task->type);
	switch(task->type){
	case TYPE_sht: output = wordaligned(sizeof(sht) + MosaicBlkSize + MOSgetCnt(blk)-1,sht); break ;
	case TYPE_int: output = wordaligned(sizeof(int) + MosaicBlkSize + MOSgetCnt(blk)-1,int); break ;
	case TYPE_oid: output = wordaligned(sizeof(oid) + MosaicBlkSize + MOSgetCnt(blk)-1,oid); break ;
	case TYPE_lng: output = wordaligned(sizeof(lng) + MosaicBlkSize + MOSgetCnt(blk)-1,lng); break ;
	//case TYPE_flt: case TYPE_dbl: to be looked into.
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned(MosaicBlkSize + sizeof(hge) + MOSgetCnt(blk)-1,hge); break ;
#endif
	case TYPE_str:
		switch(task->bsrc->twidth){
		//case 1: output = wordaligned(sizeof(bte)+ MosaicBlkSize + MOSgetCnt(blk)-1,bte); break ;
		case 2: output = wordaligned(sizeof(sht)+ MosaicBlkSize + MOSgetCnt(blk)-1,sht); break ;
		case 4: output = wordaligned(sizeof(int)+ MosaicBlkSize + MOSgetCnt(blk)-1,int); break ;
		case 8: output = wordaligned(sizeof(lng)+ MosaicBlkSize + MOSgetCnt(blk)-1,lng); break ;
		}
	}
	if( BUNappend(btech, "delta", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_delta(Client cntxt, MOStask task)
{
	MOSadvance_delta(cntxt, task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

/* we can not re-use the old stat, because the starting value may be different
	if( task->range[MOSAIC_DELTA] > task->start + 1){\
		i = task->range[MOSAIC_DELTA] - task->start;\
		if( i * sizeof(TYPE) <= wordaligned(MosaicBlkSize + sizeof(TYPE) + i-1,MosaicBlkRec))\
			return 0.0;\
		if(i) factor = ((flt) i * sizeof(TYPE))/ wordaligned(MosaicBlkSize + sizeof(TYPE) + i-1,MosaicBlkRec);\
		return factor;\
	}\
*/
// append a series of values into the non-compressed block
#define Estimate_delta(TYPE, EXPR)\
{	TYPE *v = ((TYPE*)task->src) + task->start, val= *v, delta = 0;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop-task->start;\
	for(v++,i =1; i<limit; i++,v++){\
		delta = *v -val;\
		if ( EXPR)\
			break;\
		val = *v;\
	}\
	if( i == 1 || i * sizeof(TYPE) <= wordaligned(MosaicBlkSize + sizeof(TYPE) + i-1,MosaicBlkRec))\
		return 0.0;\
	if( task->dst +  wordaligned(MosaicBlkSize + sizeof(int) + i-1,MosaicBlkRec) >=task->bsrc->tmosaic->base+ task->bsrc->tmosaic->size)\
		return 0.0;\
	factor = ((flt) i * sizeof(TYPE))/ wordaligned(MosaicBlkSize + sizeof(TYPE) + i-1,MosaicBlkRec);\
}

// estimate the compression level 
flt
MOSestimate_delta(Client cntxt, MOStask task)
{	BUN i = 0;
	flt factor = 1.0;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
		//case TYPE_bte: case TYPE_bit: no compression achievable
		case TYPE_sht: Estimate_delta(sht,  (delta < -127 || delta >127)); break;
		case TYPE_int: Estimate_delta(int,  (delta < -127 || delta >127)); break;
		case TYPE_oid: Estimate_delta(oid,  (delta > 255)); break;
		case TYPE_lng: Estimate_delta(lng,  (delta < -127 || delta >127)); break;
	#ifdef HAVE_HGE
		case TYPE_hge: Estimate_delta(hge,  (delta < -127 || delta >127)); break;
	#endif
		case  TYPE_str:
			// we only have to look at the index width, not the values
			switch(task->bsrc->twidth){
			//case 1:  no compression achievable
			case 2: Estimate_delta(sht, (delta < -127 || delta > 127)); break;
			case 4: Estimate_delta(int, (delta < -127 || delta > 127)); break;
			case 8: Estimate_delta(lng, (delta < -127 || delta > 127)); break;
		}
	break;
	//case TYPE_flt: case TYPE_dbl: to be looked into.
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate delta "BUNFMT" elm %.3f factor\n",i,factor);
#endif
	task->factor[MOSAIC_DELTA] = factor;
	task->range[MOSAIC_DELTA] = task->start + i;
	return factor;
}

#define DELTAcompress(TYPE,EXPR)\
{	TYPE *v = ((TYPE*)task->src) + task->start, val= *v, delta =0;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit():task->stop - task->start;\
	task->dst = MOScodevector(task); \
	*(TYPE*)task->dst = val;\
	hdr->checksum.sum##TYPE += *v;\
	task->dst += sizeof(TYPE);\
	for(v++,i =1; i<limit; i++,v++){\
		delta = *v -val;\
		if ( EXPR )\
			break;\
		hdr->checksum.sum##TYPE += *v;\
		*(bte*)task->dst++ = (bte) delta;\
		val = *v;\
	}\
	MOSsetCnt(blk,i);\
}

// rather expensive simple value non-compressed store
void
MOScompress_delta(Client cntxt, MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i = 0;

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_DELTA);

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: case TYPE_bit: no compression achievable
	case TYPE_sht: DELTAcompress(sht,(delta < -127 || delta >127)); break;
	case TYPE_int: DELTAcompress(int,(delta < -127 || delta >127)); break;
	case TYPE_oid: DELTAcompress(oid,(delta > 127)); break;
	case TYPE_lng: DELTAcompress(lng,(delta < -127 || delta > 127)); break;
#ifdef HAVE_HGE
	case TYPE_hge: DELTAcompress(hge,(delta < -127 || delta < -127 || delta >127)); break;
#endif
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		//case 1: no compression achievable
		case 2: DELTAcompress(sht,(delta < 0 || delta > 127)); break;
		case 4: DELTAcompress(int,(delta < 0 || delta > 127)); break;
		case 8: DELTAcompress(lng,(delta < 0 || delta > 127)); break;
		}
	//case TYPE_flt: case TYPE_dbl: to be looked into.
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_delta(cntxt, task);
#endif
}

// the inverse operator, extend the src
#define DELTAdecompress(TYPE)\
{ 	TYPE val;\
	BUN lim = MOSgetCnt(blk);\
	task->dst = MOScodevector(task);\
	val = *(TYPE*)task->dst ;\
	task->dst += sizeof(TYPE);\
	for(i = 0; i < lim; i++) {\
		hdr->checksum2.sum##TYPE += val;\
		((TYPE*)task->src)[i] = val;\
		val = val + *(bte*) (task->dst);\
		task->dst++;\
	}\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_delta(Client cntxt, MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: case TYPE_bit: no compression achievable
	case TYPE_sht: DELTAdecompress(sht); break;
	case TYPE_int: DELTAdecompress(int); break;
	case TYPE_oid: DELTAdecompress(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: DELTAdecompress(hge); break;
#endif
	case TYPE_lng: DELTAdecompress(lng); break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		// case 1: no compression achievable
		case 2: DELTAdecompress(sht); break;
		case 4: DELTAdecompress(int); break;
		case 8: DELTAdecompress(lng); break;
		}
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a DELTA compressed chunk

	
#define select_delta(TPE) {\
		TPE val= * (TPE*) (((char*) task->blk) + MosaicBlkSize);\
		task->dst = MOScodevector(task)  + sizeof(TYPE);\
		if( !*anti){\
			if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
				for( ; first < last; first++){\
					MOSskipit();\
					*o++ = (oid) first;\
				}\
			} else\
			if( *(TPE*) low == TPE##_nil ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( *(TPE*) hgh == TPE##_nil ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh )) &&\
							((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			}\
		} else {\
			if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
				/* nothing is matching */\
			} else\
			if( *(TPE*) low == TPE##_nil ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( *(TPE*) hgh == TPE##_nil ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh )) &&\
							((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			}\
		}\
	}

str
MOSselect_delta(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_delta(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: select_delta(sht); break;
	case TYPE_lng: select_delta(lng); break;
	case TYPE_oid: select_delta(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_delta(hge); break;
#endif
	case TYPE_int:
	// Expanded MOSselect_delta for debugging
		{ 	int val= *(int*) (((char*) task->blk) + MosaicBlkSize);
			task->dst = MOScodevector(task) + sizeof(int);

			if( !*anti){
				if( *(int*) low == int_nil && *(int*) hgh == int_nil){
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( *(int*) low == int_nil ){
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( *(int*) hgh == int_nil ){
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						cmp  =  ((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh )) &&
								((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				}
			} else {
				if( *(int*) low == int_nil && *(int*) hgh == int_nil){
					/* nothing is matching */
				} else
				if( *(int*) low == int_nil ){
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( *(int*) hgh == int_nil ){
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						cmp  =  ((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
						MOSskipit();
						cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh )) &&
								((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
	break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 2: break;
		case 4: break;
		case 8: break;
		}
		break;
	default:
		if( task->type == TYPE_daytime)
			select_delta(daytime);
		if( task->type == TYPE_date)
			select_delta(date);
		if( task->type == TYPE_timestamp)
			{ 	lng val= *(lng*) (((char*) task->blk) + MosaicBlkSize);
				int lownil = timestamp_isnil(*(timestamp*)low);
				int hghnil = timestamp_isnil(*(timestamp*)hgh);

				if( !*anti){
					if( lownil && hghnil){
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							*o++ = (oid) first;
						}
					} else
					if( lownil){
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh ));
							if (cmp )
								*o++ = (oid) first;
						}
					} else
					if( hghnil){
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							cmp  =  ((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
							if (cmp )
								*o++ = (oid) first;
						}
					} else{
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh )) &&
									((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
							if (cmp )
								*o++ = (oid) first;
						}
					}
				} else {
					if( lownil && hghnil){
						/* nothing is matching */
					} else
					if( lownil){
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh ));
							if ( !cmp )
								*o++ = (oid) first;
						}
					} else
					if( hghnil){
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							cmp  =  ((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
							if ( !cmp )
								*o++ = (oid) first;
						}
					} else{
						for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){
							MOSskipit();
							cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh )) &&
									((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
							if ( !cmp )
								*o++ = (oid) first;
						}
					}
				}
			}
	}
	MOSskip_delta(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_delta(TPE)\
{ 	TPE low,hgh, v;\
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
	v= *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	task->dst = MOScodevector(task) + sizeof(int);\
	for( ; first < last; first++, v+= *(bte*)task->dst, task->dst++){\
		if( (low == TPE##_nil || v >= low) && (v <= hgh || hgh == TPE##_nil) ){\
			if ( !anti) {\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( anti){\
			MOSskipit();\
			*o++ = (oid) first;\
		}\
	}\
} 

str
MOSthetaselect_delta(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_delta(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: thetaselect_delta(sht); break;
	case TYPE_int: thetaselect_delta(int); break;
	case TYPE_lng: thetaselect_delta(lng); break;
	case TYPE_oid: thetaselect_delta(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_delta(hge); break;
#endif
/*
	case TYPE_int:
		{ 	int low,hgh, v;
			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(int*) val;
				hgh = PREVVALUEint(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(int*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(int*) val;
				low = NEXTVALUEint(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(int*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(int*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(int*) val;
			} 
		 	v= *(int*) (((char*) task->blk) + MosaicBlkSize);
			task->dst = MOScodevector(task) + sizeof(int);
			for( ; first < last; first++, v+= *(bte*)task->dst, task->dst++){
				if( (low == int_nil || v >= low) && (v <= hgh || hgh == int_nil) ){
					if ( !anti) {
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( anti){
					MOSskipit();
					*o++ = (oid) first;
				}
			}
		} 
		break;
*/
	default:
			if( task->type == TYPE_daytime)
				thetaselect_delta(daytime); 
			if( task->type == TYPE_date)
				thetaselect_delta(date); 
			if( task->type == TYPE_timestamp)
				thetaselect_delta(lng); 
		break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 2: break;
		case 4: break;
		case 8: break;
		}
	}
	MOSskip_delta(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_delta(TPE)\
{	TPE val, *v;\
	bte *delta;\
	v= (TPE*) task->src;\
	val = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	delta = (bte*) (((char*)task->blk + MosaicBlkSize) + sizeof(TPE));\
	for(; first < last; first++, val+= *delta,delta++){\
		MOSskipit();\
		*v++ = val;\
		task->n--;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_delta(Client cntxt,  MOStask task)
{
	BUN first, last;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(task->type){
		case TYPE_sht: projection_delta(sht); break;
		case TYPE_int: projection_delta(int); break;
		case TYPE_lng: projection_delta(lng); break;
		case TYPE_oid: projection_delta(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_delta(hge); break;
#endif
/*
		case TYPE_int:
		{	int val, *v;
			bte *delta;
			v= (int*) task->src;
			val = *(int*) (((char*) task->blk) + MosaicBlkSize);
			delta = (bte*) (((char*)task->blk + MosaicBlkSize) + sizeof(int));
			for(; first < last; first++, val+= *delta, delta++){
				MOSskipit();
				*v++ = val;
				task->n--;
				task->cnt++;
			}
			task->src = (char*) v;
		}
		break;
*/
		case  TYPE_str:
			// we only have to look at the index width, not the values
			switch(task->bsrc->twidth){
			case 2: projection_delta(sht); break;
			case 4: projection_delta(int); break;
			case 8: projection_delta(lng); break;
			}
		break;
		default:
			if (task->type == TYPE_daytime)
				projection_delta(daytime); 
			if (task->type == TYPE_date)
				projection_delta(date); 
			if (task->type == TYPE_timestamp)
				projection_delta(lng); 
	}
	MOSskip_delta(cntxt,task);
	return MAL_SUCCEED;
}

#define join_delta(TPE)\
{	TPE *w,base;\
	bte *v;\
	base = *(int*) (((char*) task->blk) + MosaicBlkSize);\
	v = (bte*) (((char*) task->blk) + MosaicBlkSize + sizeof(int));\
	for(oo= (oid) first; first < last; first++, base += *v,v++, oo++){\
		w = (TPE*) task->src;\
		for(n = task->elm, o = 0; n -- > 0; w++,o++)\
		if ( *w == base){\
			if( BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
			BUNappend(task->rbat, &o, false) !=GDK_SUCCEED) \
			throw(MAL,"mosaic.delta",MAL_MALLOC_FAIL);\
		}\
	}\
}

str
MOSjoin_delta(Client cntxt,  MOStask task)
{
	BUN n, first, last;
	oid o, oo;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(task->type){
		case TYPE_sht: join_delta(sht); break;
		case TYPE_int: join_delta(int); break;
		case TYPE_lng: join_delta(lng); break;
		case TYPE_oid: join_delta(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_delta(hge); break;
#endif
		case  TYPE_str:
		// we only have to look at the index width, not the values
			switch(task->bsrc->twidth){
				case 2: break;
				case 4: break;
				case 8: break;
			}
		break;
		default:
			if (task->type == TYPE_daytime)
				join_delta(daytime); 
			if (task->type == TYPE_date)
				join_delta(date); 
			if (task->type == TYPE_timestamp)
				join_delta(lng); 
	}
	MOSskip_delta(cntxt,task);
	return MAL_SUCCEED;
}
