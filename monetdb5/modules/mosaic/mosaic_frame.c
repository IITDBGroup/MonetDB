/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 *2014-2016 author Martin Kersten
 * Frame of reference compression with dictionary
 * A codevector chunk is beheaded by a reference value F from the column. The elements V in the
 * chunk are replaced by an index into a global dictionary of V-F offsets.
 *
 * The dictionary is limited to 256 entries and all indices are at most one byte.
 * The maximal achievable compression ratio depends on the size of the dictionary
 *
 * This scheme is particularly geared at evolving time series, e.g. stock markets.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_bitvector.h"
#include "mosaic.h"
#include "mosaic_frame.h"
#include "mosaic_private.h"

bool MOStypes_frame(BAT* b) {
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
	}

	return false;
}

// we use longs as the basis for bit vectors
#define chunk_size(Task,Cnt) wordaligned(MosaicBlkSize + (Cnt * Task->hdr->framebits)/8 + (((Cnt * Task->hdr->framebits) %8) != 0), lng)

void
MOSadvance_frame(Client cntxt, MOStask task)
{
	int *dst = (int*)  (((char*) task->blk) + MosaicBlkSize);
	long cnt = MOSgetCnt(task->blk);
	long bytes;
	(void) cntxt;

	assert(cnt > 0);
	task->start += (oid) cnt;
	bytes =  (cnt * task->hdr->framebits)/8 + (((cnt * task->hdr->framebits) %8) != 0) + sizeof(ulng);
	task->blk = (MosaicBlk) (((char*) dst)  + wordaligned(bytes, lng)); 
}

/* Beware, the dump routines use the compressed part of the task */
static void
MOSdump_frameInternal(char *buf, size_t len, MOStask task, int i)
{
	switch(ATOMbasetype(task->type)){
	case TYPE_bte:
		snprintf(buf,len,"%hhd", task->hdr->frame.valbte[i]); break;
	case TYPE_sht:
		snprintf(buf,len,"%hd", task->hdr->frame.valsht[i]); break;
	case TYPE_int:
		snprintf(buf,len,"%d", task->hdr->frame.valint[i]); break;
	case  TYPE_oid:
		snprintf(buf,len,OIDFMT, task->hdr->frame.valoid[i]); break;
	case  TYPE_lng:
		snprintf(buf,len,LLFMT, task->hdr->frame.vallng[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		snprintf(buf,len,"%.40g", (dbl)task->hdr->frame.valhge[i]); break;
#endif
	case TYPE_flt:
		snprintf(buf,len,"%f", task->hdr->frame.valflt[i]); break;
	case TYPE_dbl:
		snprintf(buf,len,"%g", task->hdr->frame.valdbl[i]); break;
	}
}

void
MOSlayout_frame_hdr(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	lng cnt=0,j=0;
	int i;
	char buf[BUFSIZ];

	(void) cntxt;
	for(i=0; i< task->hdr->framesize; i++, j++){
		snprintf(buf,BUFSIZ,"frame[%d]",i);
		MOSdump_frameInternal(buf, BUFSIZ, task,i);
		if( BUNappend(btech, buf, false) != GDK_SUCCEED ||
			BUNappend(bcount, &j, false) != GDK_SUCCEED ||
			BUNappend(binput, &cnt, false) != GDK_SUCCEED ||
			BUNappend(boutput, &task->hdr->framefreq[i], false) != GDK_SUCCEED ||
			BUNappend(bproperties, buf, false) != GDK_SUCCEED )
			return;
	}
}

void
MOSlayout_frame(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	(void) cntxt;
	input = cnt * ATOMsize(task->type);
	output = chunk_size(task,cnt);
	if( BUNappend(btech, "frame blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_frame(Client cntxt, MOStask task)
{
	MOSadvance_frame(cntxt, task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define MOSfind(RES,DICT,VAL,F,L)\
{ int m,f= F, l=L; \
   while( l-f > 0 ) { \
	m = f + (l-f)/2;\
	if ( VAL < DICT[m] ) l=m-1; else f= m;\
	if ( VAL > DICT[m] ) f=m+1; else l= m;\
   }\
   RES= f;\
}

#define estimateFrame(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start, frame = *val, delta;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(i =0; i<limit; i++, val++){\
		delta = *val - frame;\
		MOSfind(j,task->hdr->frame.val##TPE,delta,0,hdr->framesize);\
		if( j == hdr->framesize || task->hdr->frame.val##TPE[j] != delta )\
			break;\
	}\
	if( i * sizeof(TPE) <= chunk_size(task,i) )\
		return 0.0;\
	if( task->dst +  chunk_size(task,i) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
		return 0.0;\
	if(i) factor = (flt) ((int)i * sizeof(TPE)) / chunk_size(task,i);\
}

// store it in the compressed heap header directly
// filter out the most frequent ones
#define makeFrame(TPE)\
{	TPE v,*val = ((TPE*)task->src) + task->start, frame = *val, delta;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(i =0; i< limit; i++, val++){\
		delta = *val - frame;\
		for(j= 0; j< hdr->framesize; j++)\
			if( task->hdr->frame.val##TPE[j] == delta) break;\
		if ( j == hdr->framesize){\
			if ( hdr->framesize == 256){\
				int min = 0;\
				for(j=1;j<256;j++)\
					if( cnt[min] <cnt[j]) min = j;\
				j=min;\
				cnt[j]=0;\
				break;\
			}\
			task->hdr->frame.val##TPE[j] = delta;\
			cnt[j]++;\
			hdr->framesize++;\
		} else\
			cnt[j]++;\
	}\
	for(i=0; i< (BUN) hdr->framesize; i++)\
		for(j=(int)(i+1); j< hdr->framesize; j++)\
			if(task->hdr->frame.val##TPE[i] >task->hdr->frame.val##TPE[j]){\
				v= task->hdr->frame.val##TPE[i];\
				task->hdr->frame.val##TPE[i] = task->hdr->frame.val##TPE[j];\
				task->hdr->frame.val##TPE[j] = v;\
			}\
	hdr->framebits = 1;\
	hdr->mask =1;\
	for( i=2 ; i < (BUN) hdr->framesize; i *=2){\
		hdr->framebits++;\
		hdr->mask = (hdr->mask <<1) | 1;\
	}\
}


void
MOScreateframeDictionary(Client cntxt, MOStask task)
{	BUN i;
	int j;
	MosaicHdr hdr = task->hdr;
	lng cnt[256];

	(void) cntxt;
	memset((char*)cnt,0, sizeof(lng) * 256);
	hdr->framesize = 0;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: makeFrame(bte); break;
	case TYPE_sht: makeFrame(sht); break;
	case TYPE_int: makeFrame(int); break;
	case TYPE_lng: makeFrame(lng); break;
	case TYPE_oid: makeFrame(oid); break;
	case TYPE_flt: makeFrame(flt); break;
	case TYPE_dbl: makeFrame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: makeFrame(hge); break;
#endif
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_frame(cntxt, task);
#endif
}

// calculate the expected reduction using dictionary in terms of elements compressed
flt
MOSestimate_frame(Client cntxt, MOStask task)
{	BUN i = 0;
	int j;
	flt factor= 0.0;
	MosaicHdr hdr = task->hdr;
	(void) cntxt;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: estimateFrame(bte); break;
	case TYPE_sht: estimateFrame(sht); break;
	case TYPE_lng: estimateFrame(lng); break;
	case TYPE_oid: estimateFrame(oid); break;
	case TYPE_flt: estimateFrame(flt); break;
	case TYPE_dbl: estimateFrame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateFrame(hge); break;
#endif
	case TYPE_int:
		{	int *val = ((int*)task->src) + task->start, frame = *val, delta;
			BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;

			/* frame mask may not be valid anymore
			if( task->range[MOSAIC_FRAME] > task->start){
				i = task->range[MOSAIC_FRAME] - task->start;
				if(i) factor = (flt) ((int) i * sizeof(int))/ chunk_size(task,i); 
				if( i * sizeof(int) < chunk_size(task,i) )
					return 0.0;
				return factor;
			}
			*/

			for(i =0; i<limit; i++, val++){
				delta= *val - frame;
				MOSfind(j,task->hdr->frame.valint,delta,0,hdr->framesize);
				if( j == hdr->framesize || task->hdr->frame.valint[j] != delta)
					break;
			}
			if ( i > MOSAICMAXCNT ) i = MOSAICMAXCNT;
			if( i * sizeof(int) <= chunk_size(task,i) )
				return 0.0;
			if( task->dst +  chunk_size(task,i) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			if(i) factor = (flt) ((int)i * sizeof(int)) / chunk_size(task,i);\
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	task->factor[MOSAIC_FRAME] = factor;
	task->range[MOSAIC_FRAME] = task->start + i;
	return factor; 
}

// insert a series of values into the compressor block using frame
#define framecompress(Vector,I,Bits,Value) setBitVector(Vector,I,Bits,Value)

#define FRAMEcompress(TPE)\
{	TPE *val = ((TPE*)task->src) + task->start, frame = *val, delta;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	task->dst = MOScodevector(task); \
    *(TPE*) task->dst = frame;\
	task->dst += sizeof(TPE);\
	base = (BitVector) (((char*) task->blk) +  MosaicBlkSize + wordaligned(sizeof(TPE),lng));\
	base[0]=0;\
	for(i =0; i<limit; i++, val++){\
		delta = *val - frame;\
		MOSfind(j,task->hdr->frame.val##TPE,delta,0,hdr->framesize);\
		if(j == hdr->framesize || task->hdr->frame.val##TPE[j] != delta) \
			break;\
		else {\
			hdr->checksum.sum##TPE += delta;\
			hdr->framefreq[j]++;\
			MOSincCnt(blk,1);\
			framecompress(base,i,hdr->framebits,j);\
	} }\
	task->dst += wordaligned((i * hdr->framebits / 8) + ( (i * hdr->framebits) % 8 ) != 0, lng);\
}

void
MOScompress_frame(Client cntxt, MOStask task)
{
	BUN i;
	int j;
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;
	BitVector base;

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_FRAME);
	MOSsetCnt(blk,0);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: FRAMEcompress(bte); break;
	case TYPE_sht: FRAMEcompress(sht); break;
	case TYPE_int: FRAMEcompress(int); break;
	case TYPE_lng: FRAMEcompress(lng); break;
	case TYPE_oid: FRAMEcompress(oid); break;
	case TYPE_flt: FRAMEcompress(flt); break;
	case TYPE_dbl: FRAMEcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define framedecompress(I) j = getBitVector(base,I,hdr->framebits)

#define FRAMEdecompress(TPE)\
{	BUN lim = MOSgetCnt(blk);\
    TPE frame = *(TPE*)MOScodevector(task);\
	base = (BitVector) (((char*) blk) +  MosaicBlkSize + wordaligned(sizeof(TPE),lng));\
	for(i = 0; i < lim; i++){\
		framedecompress(i);\
		((TPE*)task->src)[i] = frame + task->hdr->frame.val##TPE[j];\
		hdr->checksum2.sum##TPE += task->hdr->frame.val##TPE[j];\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_frame(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	MosaicHdr hdr = task->hdr;
	BUN i;
	unsigned int j;
	BitVector base;
	(void) cntxt;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: FRAMEdecompress(bte); break;
	case TYPE_sht: FRAMEdecompress(sht); break;
	case TYPE_int: FRAMEdecompress(int); break;
	case TYPE_lng: FRAMEdecompress(lng); break;
	case TYPE_oid: FRAMEdecompress(oid); break;
	case TYPE_flt: FRAMEdecompress(flt); break;
	case TYPE_dbl: FRAMEdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: FRAMEdecompress(hge); break;
#endif
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define select_frame(TPE) {\
    TPE frame = *(TPE*)MOScodevector(task);\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(TPE),lng));\
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
				framedecompress(i); \
				cmp  =  ((*hi && frame + task->hdr->frame.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && frame + task->hdr->frame.val##TPE[j] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*li && frame + task->hdr->frame.val##TPE[j] >= * (TPE*)low ) || (!*li && frame + task->hdr->frame.val##TPE[j] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*hi && frame + task->hdr->frame.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && frame + task->hdr->frame.val##TPE[j] < *(TPE*)hgh )) &&\
						((*li && frame + task->hdr->frame.val##TPE[j] >= * (TPE*)low ) || (!*li && frame + task->hdr->frame.val##TPE[j] > *(TPE*)low ));\
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
				framedecompress(i); \
				cmp  =  ((*hi && frame + task->hdr->frame.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && frame + task->hdr->frame.val##TPE[j] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*li && frame +task->hdr->frame.val##TPE[j] >= * (TPE*)low ) || (!*li && frame +task->hdr->frame.val##TPE[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for(i=0 ; first < last; first++, i++){\
				MOSskipit();\
				framedecompress(i); \
				cmp  =  ((*hi && frame +task->hdr->frame.val##TPE[j] <= * (TPE*)hgh ) || (!*hi && frame +task->hdr->frame.val##TPE[j] < *(TPE*)hgh )) &&\
						((*li && frame +task->hdr->frame.val##TPE[j] >= * (TPE*)low ) || (!*li && frame +task->hdr->frame.val##TPE[j] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSselect_frame(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN i, first,last;
	MosaicHdr hdr = task->hdr;
	bool cmp;
	bte j;
	BitVector base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_frame(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_frame(bte); break;
	case TYPE_sht: select_frame(sht); break;
	case TYPE_int: select_frame(int); break;
	case TYPE_lng: select_frame(lng); break;
	case TYPE_oid: select_frame(oid); break;
	case TYPE_flt: select_frame(flt); break;
	case TYPE_dbl: select_frame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_frame(hge); break;
#endif
	}
	MOSskip_frame(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_frame(TPE)\
{ 	TPE low,hgh;\
	TPE frame = *(TPE*) MOScodevector(task);\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(TPE),lng));\
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
		framedecompress(first); \
		if( (is_nil(TPE, low) || frame + task->hdr->frame.val##TPE[j] >= low) && (frame + task->hdr->frame.val##TPE[j] <= hgh || is_nil(TPE, hgh)) ){\
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
MOSthetaselect_frame(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	int j;
	MosaicHdr hdr = task->hdr;
	BitVector base;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_frame(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_frame(bte); break;
	case TYPE_sht: thetaselect_frame(sht); break;
	case TYPE_lng: thetaselect_frame(lng); break;
	case TYPE_int: thetaselect_frame(int); break;
	case TYPE_oid: thetaselect_frame(oid); break;
	case TYPE_flt: thetaselect_frame(flt); break;
	case TYPE_dbl: thetaselect_frame(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_frame(hge); break;
#endif
	}
	MOSskip_frame(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_frame(TPE)\
{	TPE *v;\
	TPE frame = *(TPE*) MOScodevector(task);\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(TPE),lng));\
	v= (TPE*) task->src;\
	for(i=0; first < last; first++,i++){\
		MOSskipit();\
		framedecompress(i);\
		*v++ = frame + task->hdr->frame.val##TPE[j];\
		task->cnt++;\
	}\
}

str
MOSprojection_frame(Client cntxt,  MOStask task)
{
	BUN i,first,last;
	MosaicHdr hdr = task->hdr;
	int j;
	BitVector base;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_frame(bte); break;
		case TYPE_sht: projection_frame(sht); break;
		case TYPE_lng: projection_frame(lng); break;
		case TYPE_int: projection_frame(int); break;
		case TYPE_oid: projection_frame(oid); break;
		case TYPE_flt: projection_frame(flt); break;
		case TYPE_dbl: projection_frame(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_frame(hge); break;
#endif
	}
	MOSskip_frame(cntxt,task);
	return MAL_SUCCEED;
}

#define join_frame(TPE)\
{	TPE *w;\
	TPE frame = *(TPE*) MOScodevector(task);\
	base = (BitVector) (((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(TPE),lng));\
	w = (TPE*) task->src;\
	limit= MOSgetCnt(task->blk);\
	for( o=0, n= task->stop; n-- > 0; o++,w++ ){\
		for(oo = task->start,i=0; i < limit; i++,oo++){\
			framedecompress(i);\
			if ( *w == frame + task->hdr->frame.val##TPE [j]){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false)!= GDK_SUCCEED)\
				throw(MAL,"mosaic.frame",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

str
MOSjoin_frame(Client cntxt,  MOStask task)
{
	BUN i,n,limit;
	oid o, oo;
	MosaicHdr hdr = task->hdr;
	int j;
	BitVector base;
	(void) cntxt;

	// set the oid range covered and advance scan range
	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_frame(bte); break;
		case TYPE_sht: join_frame(sht); break;
		case TYPE_int: join_frame(int); break;
		case TYPE_lng: join_frame(lng); break;
		case TYPE_oid: join_frame(oid); break;
		case TYPE_flt: join_frame(flt); break;
		case TYPE_dbl: join_frame(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_frame(hge); break;
#endif
		}
	MOSskip_frame(cntxt,task);
	return MAL_SUCCEED;
}
