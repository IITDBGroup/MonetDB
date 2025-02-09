/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_catalog.h"

const char *TID = "%TID%";

int
base_key( sql_base *b )
{
	return hash_key(b->name);
}

static void *
_list_find_name(list *l, const char *name)
{
	node *n;

	if (l) {
		MT_lock_set(&l->ht_lock);
		if ((!l->ht || l->ht->size*16 < list_length(l)) && list_length(l) > HASH_MIN_SIZE && l->sa) {
			l->ht = hash_new(l->sa, list_length(l), (fkeyvalue)&base_key);
			if (l->ht == NULL) {
				MT_lock_unset(&l->ht_lock);
				return NULL;
			}

			for (n = l->h; n; n = n->next ) {
				sql_base *b = n->data;
				int key = base_key(b);

				if (hash_add(l->ht, key, b) == NULL) {
					MT_lock_unset(&l->ht_lock);
					return NULL;
				}
			}
		}
		if (l->ht) {
			int key = hash_key(name);
			sql_hash_e *he = l->ht->buckets[key&(l->ht->size-1)]; 

			for (; he; he = he->chain) {
				sql_base *b = he->value;

				if (b->name && strcmp(b->name, name) == 0) {
					MT_lock_unset(&l->ht_lock);
					return b;
				}
			}
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}
		MT_lock_unset(&l->ht_lock);
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				return b;
			}
		}
	}
	return NULL;
}

static void *
_cs_find_name(changeset * cs, const char *name)
{
	return _list_find_name(cs->set, name);
}


node *
cs_find_name(changeset * cs, const char *name)
{
	return list_find_name(cs->set, name);
}

node *
list_find_name(list *l, const char *name)
{
	node *n;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				return n;
			}
		}
	return NULL;
}

node *
cs_find_id(changeset * cs, sqlid id)
{
	node *n;
	list *l = cs->set;

	if (l)
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (b->id == id) {
				return n;
			}
		}
	if (cs->dset) {
		l = cs->dset;
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			/* check if names match */
			if (b->id == id) {
				return n;
			}
		}
	}
	return NULL;
}

node *
list_find_id(list *l, sqlid id)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next) {

			/* check if ids match */
			if (id == *(sqlid *) n->data) {
				return n;
			}
		}
	}
	return NULL;
}

node *
list_find_base_id(list *l, sqlid id)
{
	if (l) {
		node *n;
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;

			if (id == b->id) 
				return n;
		}
	}
	return NULL;
}

sql_key *
find_sql_key(sql_table *t, const char *kname)
{
	return _cs_find_name(&t->keys, kname);
}

sql_idx *
find_sql_idx(sql_table *t, const char *iname)
{
	return _cs_find_name(&t->idxs, iname);
}

sql_column *
find_sql_column(sql_table *t, const char *cname)
{
	return _cs_find_name(&t->columns, cname);
}

sql_part *
find_sql_part(sql_table *t, const char *tname)
{
	return _cs_find_name(&t->members, tname);
}

sql_table *
find_sql_table(sql_schema *s, const char *tname)
{
	return _cs_find_name(&s->tables, tname);
}

sql_table *
find_sql_table_id(sql_schema *s, sqlid id)
{
	node *n = cs_find_id(&s->tables, id);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_table_node(sql_schema *s, sqlid id)
{
	return cs_find_id(&s->tables, id);
}

sql_sequence *
find_sql_sequence(sql_schema *s, const char *sname)
{
	return _cs_find_name(&s->seqs, sname);
}

sql_schema *
find_sql_schema(sql_trans *t, const char *sname)
{
	return _cs_find_name(&t->schemas, sname);
}

sql_schema *
find_sql_schema_id(sql_trans *t, sqlid id)
{
	node *n = cs_find_id(&t->schemas, id);

	if (n)
		return n->data;
	return NULL;
}

node *
find_sql_schema_node(sql_trans *t, sqlid id)
{
	return cs_find_id(&t->schemas, id);
}

static sql_type *
find_sqlname(list *l, const char *name)
{
	if (l) {
		node *n;

		for (n = l->h; n; n = n->next) {
			sql_type *t = n->data;

			if (strcmp(t->sqlname, name) == 0)
				return t;
		} 
	}
	return NULL;
}

node *
find_sql_type_node(sql_schema * s, sqlid id)
{
	return cs_find_id(&s->types, id);
}

sql_type *
find_sql_type(sql_schema * s, const char *tname)
{
	return find_sqlname(s->types.set, tname);
}

sql_type *
sql_trans_bind_type(sql_trans *tr, sql_schema *c, const char *name)
{
	node *n;
	sql_type *t = NULL;

	if (tr->schemas.set)
		for (n = tr->schemas.set->h; n && !t; n = n->next) {
			sql_schema *s = n->data;

			t = find_sql_type(s, name);
		}

	if (!t && c)
		t = find_sql_type(c, name);
	return t;
}

node *
find_sql_func_node(sql_schema * s, sqlid id)
{
	return cs_find_id(&s->funcs, id);
}

sql_func *
find_sql_func(sql_schema * s, const char *tname)
{
	return _cs_find_name(&s->funcs, tname);
}

list *
find_all_sql_func(sql_schema * s, const char *name, sql_ftype type)
{
	list *l = s->funcs.set, *res = NULL;
	node *n = NULL;

	if (l) {
		for (n = l->h; n; n = n->next) {
			sql_base *b = n->data;
			sql_func *f = n->data;

			/* check if names match */
			if (f->type == type && name[0] == b->name[0] && strcmp(name, b->name) == 0) {
				if (!res)
					res = list_create((fdestroy)NULL);
				if (!res) {
					return NULL;
				}
				list_append(res, f);
			}
		}
	}
	return res;
}

sql_func *
sql_trans_bind_func(sql_trans *tr, const char *name)
{
	node *n;
	sql_func *t = NULL;

	if (tr->schemas.set)
		for (n = tr->schemas.set->h; n && !t; n = n->next) {
			sql_schema *s = n->data;

			t = find_sql_func(s, name);
		}
	if (!t)
		return NULL;
	return t;
}

sql_func *
sql_trans_find_func(sql_trans *tr, sqlid id)
{
	node *n, *m;
	sql_func *t = NULL;

	if (tr->schemas.set) {
		for (n = tr->schemas.set->h; n && !t; n = n->next) {
			m = find_sql_func_node(n->data, id);
			if (m)
				t = m->data;
		}
	}
	return t;
}

void*
sql_values_list_element_validate_and_insert(void *v1, void *v2, int* res)
{
	sql_part_value* pt = (sql_part_value*) v1, *newp = (sql_part_value*) v2;

	assert(pt->tpe.type->localtype == newp->tpe.type->localtype);
	*res = ATOMcmp(pt->tpe.type->localtype, newp->value, pt->value);
	return *res == 0 ? pt : NULL;
}

void*
sql_range_part_validate_and_insert(void *v1, void *v2)
{
	sql_part* pt = (sql_part*) v1, *newp = (sql_part*) v2;
	int res1, res2;

	if (pt == newp) /* same pointer, skip (used in updates) */
		return NULL;

	assert(pt->tpe.type->localtype == newp->tpe.type->localtype);
	if (newp->with_nills && pt->with_nills) //only one partition at most has null values
		return pt;

	res1 = ATOMcmp(pt->tpe.type->localtype, pt->part.range.minvalue, newp->part.range.maxvalue);
	res2 = ATOMcmp(pt->tpe.type->localtype, newp->part.range.minvalue, pt->part.range.maxvalue);
	if (res1 < 0 && res2 < 0) //overlap: x1 < y2 && y1 < x2
		return pt;
	return NULL;
}

void*
sql_values_part_validate_and_insert(void *v1, void *v2)
{
	sql_part* pt = (sql_part*) v1, *newp = (sql_part*) v2;
	list* b1 = pt->part.values, *b2 = newp->part.values;
	node *n1 = b1->h, *n2 = b2->h;
	int res;

	if (pt == newp) /* same pointer, skip (used in updates) */
		return NULL;

	assert(pt->tpe.type->localtype == newp->tpe.type->localtype);
	if (newp->with_nills && pt->with_nills)
		return pt; //check for nulls first

	while (n1 && n2) {
		sql_part_value *p1 = (sql_part_value *) n1->data, *p2 = (sql_part_value *) n2->data;
		res = ATOMcmp(pt->tpe.type->localtype, p1->value, p2->value);
		if (!res) { //overlap -> same value in both partitions
			return pt;
		} else if(res < 0) {
			n1 = n1->next;
		} else {
			n2 = n2->next;
		}
	}
	return NULL;
}
