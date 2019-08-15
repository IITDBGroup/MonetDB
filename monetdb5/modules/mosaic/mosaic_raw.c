/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Use a chunk that has not been compressed
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_raw.h"
#include "mosaic_private.h"

bool MOStypes_raw(BAT* b) {
	switch(ATOMbasetype(getBatType(b->ttype))){
	case TYPE_bte: return true;
	case TYPE_bit: return true;
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
MOSlayout_raw(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

		input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break ;
	case TYPE_bit: output = wordaligned( MosaicBlkSize + sizeof(bit)* MOSgetCnt(blk),bit); break ;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break ;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break ;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid); break ;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break ;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge); break ;
#endif
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt); break ;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl); break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break ;
		case 2: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break ;
		case 4: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break ;
		case 8: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break ;
		}
	}
	if( BUNappend(btech, "raw blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSadvance_raw(MOStask task)
{
	MosaicBlk blk = task->blk;

	task->start += MOSgetCnt(blk);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte)); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bit)* MOSgetCnt(blk),bit)); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int)); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng)); break ;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge)); break ;
#endif
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt)); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl)); break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte)); break ;
		case 2: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht)); break ;
		case 4: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int)); break ;
		case 8: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng)); break ;
		}
	}
}

void
MOSskip_raw( MOStask task)
{
	MOSadvance_raw(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}


// append a series of values into the non-compressed block

#define RAWcompress(TYPE)\
{	*(TYPE*) task->dst = ((TYPE*) task->src)[task->start];\
	hdr->checksum.sum##TYPE += *(TYPE*) task->dst;\
	task->dst += sizeof(TYPE);\
	MOSincCnt(blk,1);\
}

// rather expensive simple value non-compressed store
void
MOScompress_raw(MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;

		MOSsetTag(blk,MOSAIC_RAW);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RAWcompress(bte); break ;
	case TYPE_bit: RAWcompress(bit); break ;
	case TYPE_sht: RAWcompress(sht); break;
	case TYPE_oid: RAWcompress(oid); break;
	case TYPE_lng: RAWcompress(lng); break;
	case TYPE_flt: RAWcompress(flt); break;
	case TYPE_dbl: RAWcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RAWcompress(hge); break;
#endif
	case TYPE_int:
	{	*(int*) task->dst = ((int*) task->src)[task->start];
		hdr->checksum.sumint += *(int*) task->dst;
		task->dst += sizeof(int);
		MOSincCnt(blk,1);
	}
	break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: RAWcompress(bte); break ;
		case 2: RAWcompress(sht); break ;
		case 4: RAWcompress(int); break ;
		case 8: RAWcompress(lng); break ;
		}
	}
}

// the inverse operator, extend the src
#define RAWdecompress(TYPE)\
{ BUN lim = MOSgetCnt(blk); \
	for(i = 0; i < lim; i++) {\
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	hdr->checksum2.sum##TYPE += ((TYPE*)compressed)[i]; \
	}\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_raw(MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	char *compressed;

	compressed = ((char*)blk) + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RAWdecompress(bte); break ;
	case TYPE_bit: RAWdecompress(bit); break ;
	case TYPE_sht: RAWdecompress(sht); break;
	case TYPE_oid: RAWdecompress(oid); break;
	case TYPE_lng: RAWdecompress(lng); break;
	case TYPE_flt: RAWdecompress(flt); break;
	case TYPE_dbl: RAWdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RAWdecompress(hge); break;
#endif
	case TYPE_int:
	{ BUN lim = MOSgetCnt(blk);	
		for(i = 0; i < lim; i++) {
			((int*)task->src)[i] = ((int*)compressed)[i];
			hdr->checksum2.sumint += ((int*)compressed)[i]; \
		}
		task->src += i * sizeof(int);
	}
	break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: RAWdecompress(bte); break ;
		case 2: RAWdecompress(sht); break ;
		case 4: RAWdecompress(int); break ;
		case 8: RAWdecompress(lng); break ;
		}
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a RAW compressed chunk

	
#define select_raw(TPE) {\
		TPE *val= (TPE*) (((char*) task->blk) + MosaicBlkSize);\
		if( !*anti){\
			if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
				for( ; first < last; first++){\
					MOSskipit();\
					*o++ = (oid) first;\
				}\
			} else\
			if( is_nil(TPE, *(TPE*) low) ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( is_nil(TPE, *(TPE*) hgh) ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh )) &&\
							((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			}\
		} else {\
			if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
				/* nothing is matching */\
			} else\
			if( is_nil(TPE, *(TPE*) low) ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( is_nil(TPE, *(TPE*) hgh) ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh )) &&\
							((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			}\
		}\
	}

str
MOSselect_raw( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_raw(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_bit: select_raw(bit); break;
	case TYPE_bte: select_raw(bte); break;
	case TYPE_sht: select_raw(sht); break;
	case TYPE_oid: select_raw(oid); break;
	case TYPE_lng: select_raw(lng); break;
	case TYPE_flt: select_raw(flt); break;
	case TYPE_dbl: select_raw(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_raw(hge); break;
#endif
	case TYPE_int:
	// Expanded MOSselect_raw for debugging
		{ 	int *val= (int*) (((char*) task->blk) + MosaicBlkSize);

			if( !*anti){
				if( *(int*) low == int_nil && *(int*) hgh == int_nil){
					for( ; first < last; first++, val++){
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( *(int*) low == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( *(int*) hgh == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
								((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				}
			} else {
				if( *(int*) low == int_nil && *(int*) hgh == int_nil){
					/* nothing is matching */
				} else
				if( *(int*) low == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( *(int*) hgh == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
								((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
			break;
	case TYPE_str:
		// beware we should look at the value
		switch(task->bsrc->twidth){
		case 1: break ;
		case 2: break ;
		case 4: break ;
		case 8: break ;
	}
	default: {
		if( task->type == TYPE_date)
			select_raw(date); 
		if( task->type == TYPE_daytime)
			select_raw(daytime); 
		
		if( task->type == TYPE_timestamp)
			{ 	lng *val= (lng*) (((char*) task->blk) + MosaicBlkSize);
				int lownil = is_timestamp_nil(*(timestamp*)low);
				int hghnil = is_timestamp_nil(*(timestamp*)hgh);

				if( !*anti){
					if( lownil && hghnil){
						for( ; first < last; first++, val++){
							MOSskipit();
							*o++ = (oid) first;
						}
					} else
					if( lownil){
						for( ; first < last; first++, val++){
							MOSskipit();
							cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh ));
							if (cmp )
								*o++ = (oid) first;
						}
					} else
					if( hghnil){
						for( ; first < last; first++, val++){
							MOSskipit();
							cmp  =  ((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
							if (cmp )
								*o++ = (oid) first;
						}
					} else{
						for( ; first < last; first++, val++){
							MOSskipit();
							cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh )) &&
									((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
							if (cmp )
								*o++ = (oid) first;
						}
					}
				} else {
					if( lownil && hghnil){
						/* nothing is matching */
					} else
					if( lownil){
						for( ; first < last; first++, val++){
							MOSskipit();
							cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh ));
							if ( !cmp )
								*o++ = (oid) first;
						}
					} else
					if( hghnil){
						for( ; first < last; first++, val++){
							MOSskipit();
							cmp  =  ((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
							if ( !cmp )
								*o++ = (oid) first;
						}
					} else{
						for( ; first < last; first++, val++){
							MOSskipit();
							cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh )) &&
									((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
							if ( !cmp )
								*o++ = (oid) first;
						}
					}
				}
			}
	}
	}
	MOSskip_raw(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_raw(TPE)\
{ 	TPE low,hgh, *v;\
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
	v = (TPE*) (((char*)task->blk) + MosaicBlkSize);\
	for( ; first < last; first++, v++){\
		if( (is_nil(TPE, low) || * v >= low) && (is_nil(TPE, hgh)  || * v <= hgh) ){\
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
MOSthetaselect_raw( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_raw(task);
		return MAL_SUCCEED;
	}
	if ( first + MOSgetCnt(task->blk) > last)
		last = MOSgetCnt(task->blk);
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bit: thetaselect_raw(bit); break;
	case TYPE_bte: thetaselect_raw(bte); break;
	case TYPE_sht: thetaselect_raw(sht); break;
	case TYPE_oid: thetaselect_raw(oid); break;
	case TYPE_lng: thetaselect_raw(lng); break;
	case TYPE_flt: thetaselect_raw(flt); break;
	case TYPE_dbl: thetaselect_raw(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_raw(hge); break;
#endif
	case TYPE_int:
		{ 	int low,hgh, *v;
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
			v = (int*) (((char*)task->blk) + MosaicBlkSize);
			for( ; first < last; first++, v++){
				if( (is_nil(int, low) || * v >= low) && (is_nil(int, hgh) || * v <= hgh) ){
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
	case TYPE_str:
		// beware we should look at the value
		switch(task->bsrc->twidth){
		case 1: break ;
		case 2: break ;
		case 4: break ;
		case 8: break ;
		}
	}
	MOSskip_raw(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_raw(TPE)\
{	TPE *val, *v;\
	v= (TPE*) task->src;\
	val = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(; first < last; first++, val++){\
		MOSskipit();\
		*v++ = *val;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_raw( MOStask task)
{
	BUN first,last;
		// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bit: projection_raw(bit); break;
		case TYPE_bte: projection_raw(bte); break;
		case TYPE_sht: projection_raw(sht); break;
		case TYPE_oid: projection_raw(oid); break;
		case TYPE_lng: projection_raw(lng); break;
		case TYPE_flt: projection_raw(flt); break;
		case TYPE_dbl: projection_raw(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_raw(hge); break;
#endif
		case TYPE_int:
		{	int *val, *v;
			v= (int*) task->src;
			val = (int*) (((char*) task->blk) + MosaicBlkSize);
			for(; first < last; first++, val++){
				MOSskipit();
				*v++ = *val;
				task->cnt++;
			}
			task->src = (char*) v;
		}
	break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: projection_raw(bte); break ;
		case 2: projection_raw(sht); break ;
		case 4: projection_raw(int); break ;
		case 8: projection_raw(lng); break ;
		}
	}
	MOSskip_raw(task);
	return MAL_SUCCEED;
}

#define join_raw(TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(oo= (oid) first; first < last; first++, v++, oo++){\
		w = (TPE*) task->src;\
		for(n = task->stop, o = 0; n -- > 0; w++,o++)\
		if ( *w == *v){\
			if( BUNappend(task->lbat, &oo, false)!= GDK_SUCCEED ||\
			BUNappend(task->rbat, &o, false) != GDK_SUCCEED)\
			throw(MAL,"mosaic.raw",MAL_MALLOC_FAIL);\
		}\
	}\
}

str
MOSjoin_raw( MOStask task)
{
	BUN n,first,last;
	oid o, oo;
		// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bit: join_raw(bit); break;
		case TYPE_bte: join_raw(bte); break;
		case TYPE_sht: join_raw(sht); break;
		case TYPE_oid: join_raw(oid); break;
		case TYPE_lng: join_raw(lng); break;
		case TYPE_flt: join_raw(flt); break;
		case TYPE_dbl: join_raw(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_raw(hge); break;
#endif
		case TYPE_int:
		{	int *v, *w;
			v = (int*) (((char*) task->blk) + MosaicBlkSize);
			for(oo= (oid) first; first < last; first++, v++, oo++){
				w = (int*) task->src;
				for(n = task->stop, o = 0; n -- > 0; w++,o++)
				if ( *w == *v){
					if( BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||
						BUNappend(task->rbat, &o, false) != GDK_SUCCEED )
						throw(MAL,"mosaic.raw",MAL_MALLOC_FAIL);
				}
			}
		}
	break;
	case TYPE_str:
		// beware we should look at the value
		switch(task->bsrc->twidth){
		case 1: join_raw(bte); break ;
		case 2: join_raw(sht); break ;
		case 4: join_raw(int); break ;
		case 8: join_raw(lng); break ;
		}
	}
	MOSskip_raw(task);
	return MAL_SUCCEED;
}
