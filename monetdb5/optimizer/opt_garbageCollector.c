/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_garbageCollector.h"
#include "mal_interpreter.h"
#include "mal_builder.h"
#include "mal_function.h"
#include "opt_prelude.h"

/* The garbage collector is focused on removing temporary BATs only.
 * Leaving some garbage on the stack is an issue.
 *
 * The end-of-life of a BAT may lay within block bracket. This calls
 * for care, as the block may trigger a loop and then the BATs should
 * still be there.
 *
 * The life time of such BATs is forcefully terminated after the block exit.
 */
str
OPTgarbageCollectorImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, limit, slimit, vlimit;
	InstrPtr p, *old;
	int actions = 0;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;
	//str nme;
	char *used;

	(void) pci;
	(void) cntxt;
	(void) stk;
	if ( mb->inlineProp)
		return 0;
	
	used = (char*) GDKzalloc(sizeof(char) * mb->vtop);
	if ( used == NULL)
		throw(MAL, "optimizer.garbagecollector", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	old= mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;
	vlimit = mb->vtop;

	/* rename all temporaries used for ease of debugging and profile interpretation */

 	for( i = 0; i < mb->vtop; i++)
		if (getVarName(mb,i)[0] == 'X' && getVarName(mb,i)[1] == '_')
			snprintf(getVarName(mb,i),IDLENGTH,"X_%d",i);
		else
		if (getVarName(mb,i)[0] == 'C' && getVarName(mb,i)[1] == '_')
			snprintf(getVarName(mb,i),IDLENGTH,"C_%d",i);

	// move SQL query definition to the front for event profiling tools
	p = NULL;
	for(i = 0; i < limit; i++)
		if(mb->stmt[i] && getModuleId(mb->stmt[i]) == querylogRef && getFunctionId(mb->stmt[i]) == defineRef ){
			p = getInstrPtr(mb,i);
			break;
		}
	
	if( p != NULL){
		for(  ; i > 1; i--)
			mb->stmt[i] = mb->stmt[i-1];
		mb->stmt[1] = p;
		actions =1;
	}

	// Actual garbage collection stuff

	if ( newMalBlkStmt(mb,mb->ssize) < 0) 
		throw(MAL, "optimizer.garbagecollector", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	p = NULL;
	for (i = 0; i < limit; i++) {
		p = old[i];
		p->gc &=  ~GARBAGECONTROL;
		p->typechk = TYPE_UNKNOWN;
		/* Set the program counter to ease profiling */
		p->pc = i;

		if ( p->barrier == RETURNsymbol){
			pushInstruction(mb, p);
			continue;
		}
		if ( p->token == ENDsymbol)
			break;
		
		pushInstruction(mb, p);

		/* A block exit is never within a parallel block,
		 * otherwise we could not inject the assignment */
		/* force garbage collection of all declared within output block and ending here  */
		/* ignore for the time being, it requires a more thorough analysis of dependencies.
		if (blockExit(p) ){
			for( k = stmtlnk[i]; k; k = varlnk[k])
			if( isaBatType(getVarType(mb,k)) ){
				q = newAssignment(mb);
				getArg(q,0)= k;
				q= pushNil(mb,q, getVarType(mb,k));
				setVarUDFtype(mb,k);
				setVarFixed(mb,k);
				q->gc |= GARBAGECONTROL;
				setVarEolife(mb,k,mb->stop-1);
				actions++;
			}
		}
		*/
	}
	/* A good MAL plan should end with an END instruction */
	pushInstruction(mb, p);
	if( p && p->token != ENDsymbol){
		throw(MAL, "optimizer.garbagecollector", SQLSTATE(42000) "Incorrect MAL plan encountered");
	}
	for (i++; i < limit; i++) 
		pushInstruction(mb, old[i]);
	for (; i < slimit; i++) 
		if (old[i])
			freeInstruction(old[i]);
	getInstrPtr(mb,0)->gc |= GARBAGECONTROL;
	//GDKfree(varlnk);
	//GDKfree(stmtlnk);
	GDKfree(old);
	GDKfree(used);
    if( OPTdebug &  OPTgarbagecollector)
	{ 	int k;
		fprintf(stderr, "#Garbage collected BAT variables \n");
		for ( k =0; k < vlimit; k++)
		fprintf(stderr,"%10s eolife %3d  begin %3d lastupd %3d end %3d\n",
			getVarName(mb,k), getVarEolife(mb,k),
			getBeginScope(mb,k), getLastUpdate(mb,k), getEndScope(mb,k));
		chkFlow(mb);
		if ( mb->errors != MAL_SUCCEED ){
			fprintf(stderr,"%s\n",mb->errors);
			freeException(mb->errors);
			mb->errors = MAL_SUCCEED;
		}
		fprintFunction(stderr,mb, 0, LIST_MAL_ALL);
		fprintf(stderr, "End of GCoptimizer\n");
	}

	/* leave a consistent scope admin behind */
	setVariableScope(mb);
	/* Defense line against incorrect plans */
	if( actions > 0){
		chkTypes(cntxt->usermodule, mb, FALSE);
		chkFlow(mb);
		chkDeclarations(mb);
	}

	/* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","garbagecollector",actions, usec);
	newComment(mb,buf);
	if( actions >= 0)
		addtoMalBlkHistory(mb);

    if( OPTdebug &  OPTgarbagecollector){
        fprintf(stderr, "#GARBAGECOLLECTOR optimizer exit\n");
        fprintFunction(stderr, mb, 0,  LIST_MAL_ALL);
    }
	return msg;
}

