/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_evaluate.h"
#include "opt_aliases.h"

static int
OPTallConstant(Client cntxt, MalBlkPtr mb, InstrPtr p)
{
	int i;
	(void)cntxt;

	if ( !(p->token == ASSIGNsymbol ||
		   getModuleId(p) == calcRef ||
		   getModuleId(p) == strRef ||
		   getModuleId(p) == mtimeRef ||
		   getModuleId(p) == mmathRef))
		return FALSE;
	if (getModuleId(p) == mmathRef && strcmp(getFunctionId(p), "rand") == 0)
		return FALSE;

	for (i = p->retc; i < p->argc; i++)
		if (isVarConstant(mb, getArg(p, i)) == FALSE)
			return FALSE;
	for (i = 0; i < p->retc; i++) {
		if (isaBatType(getArgType(mb, p, i)))
			return FALSE;
		if ( mb->unsafeProp ) 
			return FALSE;
	}
	return TRUE;
}

static int OPTsimpleflow(MalBlkPtr mb, int pc)
{
    int i, block =0, simple= TRUE;
    InstrPtr p;

    for ( i= pc; i< mb->stop; i++){
        p =getInstrPtr(mb,i);
        if (blockStart(p))
            block++;
        if ( blockExit(p))
            block--;
        if ( blockCntrl(p))
            simple= FALSE;
        if ( block == 0){
            return simple;
        }
    }
    return FALSE;
}

/* barrier blocks can only be dropped when they are fully excluded.  */
static str
OPTremoveUnusedBlocks(Client cntxt, MalBlkPtr mb)
{
	/* catch and remove constant bounded blocks */
	int i, j = 0, action = 0, block = -1, skip = 0, multipass = 1;
	InstrPtr p;
	str msg = MAL_SUCCEED;

	while(multipass--){
		block = -1;
		skip = 0;
		j = 0;
		for (i = 0; i < mb->stop; i++) {
			p = mb->stmt[i];
			if (blockExit(p) && block == getArg(p,0) ){
					block = -1;
					skip = 0;
					freeInstruction(p);
					mb->stmt[i]= 0;
					continue;
			}
			if (p->argc == 2 && blockStart(p) && block < 0 && isVarConstant(mb, getArg(p, 1)) && getArgType(mb, p, 1) == TYPE_bit ){
				if( getVarConstant(mb, getArg(p, 1)).val.btval == 0)
				{
					block = getArg(p,0);
					skip ++;
					action++;
				}
				// Try to remove the barrier statement itself (when true).
				if ( getVarConstant(mb, getArg(p, 1)).val.btval == 1  && OPTsimpleflow(mb,i))
				{
					block = getArg(p,0);
					skip = 0;
					action++;
					freeInstruction(p);
					mb->stmt[i]= 0;
					continue;
				}
			} else 
			if( p->argc == 2 &&  blockStart(p) && block >= 0 && skip == 0 && isVarConstant(mb, getArg(p, 1)) && getArgType(mb, p, 1) == TYPE_bit && multipass == 0)
				multipass++;
			if (skip){
				freeInstruction(p);
				mb->stmt[i]= 0;
			} else
				mb->stmt[j++] = p;
		}
		mb->stop = j;
		for (; j < i; j++)
			mb->stmt[j] = NULL;
	}
	if (action)
		chkTypes(cntxt->usermodule, mb, TRUE);
	return msg;
}

str
OPTevaluateImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p;
	int i, k, limit, *alias = 0, barrier;
	MalStkPtr env = NULL;
	int profiler, sqlprofiler;
	int debugstate = cntxt->itrace, actions = 0, constantblock = 0;
	int *assigned = 0, use; 
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void)stk;
	(void)pci;

	if ( mb->inlineProp )
		return MAL_SUCCEED;

	cntxt->itrace = 0;

    if( OPTdebug &  OPTevaluate){
		fprintf(stderr, "Constant expression optimizer started\n");
	}

	assigned = (int*) GDKzalloc(sizeof(int) * mb->vtop);
	if (assigned == NULL)
		throw(MAL,"optimzier.evaluate", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	alias = (int*)GDKzalloc(mb->vsize * sizeof(int) * 2); /* we introduce more */
	if (alias == NULL){
		GDKfree(assigned);
		throw(MAL,"optimzier.evaluate", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	// arguments are implicitly assigned by context
	p = getInstrPtr(mb, 0);
	for ( k =p->retc;  k < p->argc; k++)
		assigned[getArg(p,k)]++;
	limit = mb->stop;
	for (i = 1; i < limit; i++) {
		p = getInstrPtr(mb, i);
		// The double count emerging from a barrier exit is ignored.
		if (! blockExit(p) || (blockExit(p) && p->retc != p->argc))
		for ( k =0;  k < p->retc; k++)
		if ( p->retc != p->argc || p->token != ASSIGNsymbol )
			assigned[getArg(p,k)]++;
	}

	for (i = 1; i < limit && cntxt->mode != FINISHCLIENT; i++) {
		p = getInstrPtr(mb, i);
		// to avoid management of duplicate assignments over multiple blocks
		// we limit ourselves to evaluation of the first assignment only.
		use = assigned[getArg(p,0)] == 1 && !(p->argc == p->retc && blockExit(p));
		for (k = p->retc; k < p->argc; k++)
			if (alias[getArg(p, k)])
				getArg(p, k) = alias[getArg(p, k)];
		if( OPTdebug &  OPTevaluate){
			fprintInstruction(stderr , mb, 0, p, LIST_MAL_ALL);
		}
		/* be aware that you only assign once to a variable */
		if (use && p->retc == 1 && OPTallConstant(cntxt, mb, p) && !isUnsafeFunction(p)) {
			barrier = p->barrier;
			p->barrier = 0;
			profiler = malProfileMode;	/* we don't trace it */
			sqlprofiler = cntxt->sqlprofiler;
			malProfileMode = 0;
			if ( env == NULL) {
				env = prepareMALstack(mb,  2 * mb->vsize);
				if (!env) {
					msg = createException(MAL,"optimizer.evaluate", SQLSTATE(HY001) MAL_MALLOC_FAIL);
					goto wrapup;
				}
				env->keepAlive = TRUE;
			}
			msg = reenterMAL(cntxt, mb, i, i + 1, env);
			malProfileMode= profiler;
			cntxt->sqlprofiler = sqlprofiler;
			p->barrier = barrier;
			if( OPTdebug &  OPTevaluate){
				fprintf(stderr, "#retc var %s\n", getVarName(mb, getArg(p, 0)));
				fprintf(stderr, "#result:%s\n", msg == MAL_SUCCEED ? "ok" : msg);
			}
			if (msg == MAL_SUCCEED) {
				int nvar;
				ValRecord cst;

				actions++;
				cst.vtype = 0;
				VALcopy(&cst, &env->stk[getArg(p, 0)]);
				/* You may not overwrite constants.  They may be used by
				 * other instructions */
				nvar = getArg(p, 1) = defConstant(mb, getArgType(mb, p, 0), &cst);
				if (nvar >= env->stktop) {
					VALcopy(&env->stk[getArg(p, 1)], &getVarConstant(mb, getArg(p, 1)));
					env->stktop = getArg(p, 1) + 1;
				}
				alias[getArg(p, 0)] = getArg(p, 1);
				p->argc = 2;
				p->token = ASSIGNsymbol;
				clrFunction(p);
				p->barrier = barrier;
				/* freeze the type */
				setVarFixed(mb,getArg(p,1));
				setVarUDFtype(mb,getArg(p,1));
				if( OPTdebug &  OPTevaluate){
					str tpename;
					fprintf(stderr, "Evaluated new constant=%d -> %d:%s\n",
						getArg(p, 0), getArg(p, 1), tpename = getTypeName(getArgType(mb, p, 1)));
					GDKfree(tpename);
				}
			} else {
				/* if there is an error, we should postpone message handling,
					as the actual error (eg. division by zero ) may not happen) */
				if( OPTdebug &  OPTevaluate){
					fprintf(stderr, "Evaluated %s\n", msg);
				}
				freeException(msg);
				msg= MAL_SUCCEED;
				mb->errors = 0;
			}
		}
		constantblock +=  blockStart(p) && OPTallConstant(cntxt, mb, p);	/* default */
	}
	// produces errors in SQL when enabled
	if ( constantblock)
		msg = OPTremoveUnusedBlocks(cntxt, mb);
	cntxt->itrace = debugstate;

    /* Defense line against incorrect plans */
	/* Plan is unaffected */
	chkTypes(cntxt->usermodule, mb, FALSE);
	chkFlow(mb);
	chkDeclarations(mb);
    
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","evaluate",actions,usec);
    newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

wrapup:
	if ( env) freeStack(env);
	if(assigned) GDKfree(assigned);
	if(alias)	GDKfree(alias);
    if( OPTdebug &  OPTevaluate){
        fprintf(stderr, "#EVALUATE optimizer exit\n");
        fprintFunction(stderr, mb, 0,  LIST_MAL_ALL);
    }
	return msg;
}
