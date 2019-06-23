/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_qc.h"
#include "sql_mvc.h"
#include "sql_atom.h"
#include "rel_exp.h"

qc *
qc_create(int clientid, int seqnr)
{
	qc *r = MNEW(qc);
	if(!r)
		return NULL;
	r->clientid = clientid;
	r->id = seqnr;
	r->nr = 0;

	r->q = NULL;
	return r;
}

static void
cq_delete(int clientid, cq *q)
{
	if (q->name)
		backend_freecode(clientid, q->name);
	/* q, params and name are allocated using sa, ie need to be delete last */
	if (q->sa) 
		sa_destroy(q->sa);
}

void
qc_delete(qc *cache, cq *q)
{
	cq *n, *p = NULL;

	for (n = cache->q; n; p = n, n = n->next) {
		if (n == q) {
			if (p) {
				p->next = q->next;
			} else {
				cache->q = q->next;
			}
			cq_delete(cache->clientid, q);
			cache->nr--;
			break;
		}
	}
}

void
qc_destroy(qc *cache)
{
	cq *q, *n;
	for (q = cache->q; q; q = n) {
		n = q->next;

		cq_delete(cache->clientid, q);
		cache->nr--;
	}
	_DELETE(cache);
}

cq *
qc_find(qc *cache, int id)
{
	cq *q;

	for (q = cache->q; q; q = q->next) {
		if (q->id == id) {
			q->count++;
			return q;
		}
	}
	return NULL;
}

cq *
qc_insert(qc *cache, sql_allocator *sa, sql_rel *r, symbol *s, list *params, int type, char *cmd, int no_mitosis)
{
	int namelen;
	sql_func *f = SA_ZNEW(sa, sql_func);
	cq *n = SA_ZNEW(sa, cq);
	list *res = NULL;

	if(!n || !f)
		return NULL;
	n->id = cache->id++;
	cache->nr++;

	n->sa = sa;
	n->rel = r;
	n->s = s;

	n->next = cache->q;
	n->type = type;
	n->count = 1;
	namelen = 5 + ((n->id+7)>>3) + ((cache->clientid+7)>>3);
	n->name = sa_alloc(sa, namelen);
	n->no_mitosis = no_mitosis;
	if(!n->name)
		return NULL;
	(void) snprintf(n->name, namelen, "p%d_%d", n->id, cache->clientid);
	cache->q = n;

	if (r && is_project(r->op) && !list_empty(r->exps)) {
		sql_arg *a;
		node *m;

		res = sa_list(sa);
		for(m = r->exps->h; m; m = m->next) {
			sql_exp *e = m->data;
			sql_subtype *t = exp_subtype(e);

			a = NULL;
			if (t)
				a = sql_create_arg(sa, NULL, t, ARG_OUT);
			append(res, a);
		}
	}

	base_init(sa, &f->base, 0, TR_NEW, NULL);
	f->base.id = n->id;
	f->base.name = f->imp = n->name;
	f->mod = NULL;
	f->type = F_PROC;
	f->query = cmd;
	f->lang = 0;
	f->sql = 0;
	f->side_effect = 0;
	f->varres = 0;
	f->vararg = 0;
	f->ops = params;
	f->res = res;
	f->fix_scale = 0;
	f->system = 0;
	n->f = f;
	return n;
}

int
qc_size(qc *cache)
{
	return cache->nr;
}
