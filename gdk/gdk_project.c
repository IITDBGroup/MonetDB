/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

/*
 * BATproject returns a BAT aligned with the left input whose values
 * are the values from the right input that were referred to by the
 * OIDs in the left input.
 */

#define project_loop(TYPE)						\
static gdk_return							\
project_##TYPE(BAT *bn, BAT *l, struct canditer *restrict ci, BAT *r, bool nilcheck) \
{									\
	BUN lo, hi;							\
	const TYPE *restrict rt;					\
	TYPE *restrict bt;						\
	TYPE v;								\
	oid rseq, rend;							\
	bool hasnil = false;						\
									\
	rt = (const TYPE *) Tloc(r, 0);					\
	bt = (TYPE *) Tloc(bn, 0);					\
	rseq = r->hseqbase;						\
	rend = rseq + BATcount(r);					\
	if (ci) {							\
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {		\
			oid o = canditer_next(ci);			\
			if (o < rseq || o >= rend) {			\
				GDKerror("BATproject: does not match always\n"); \
				return GDK_FAIL;			\
			}						\
			v = rt[o - rseq];				\
			bt[lo] = v;					\
			hasnil |= is_##TYPE##_nil(v);			\
		}							\
	} else {							\
		const oid *restrict o = (const oid *) Tloc(l, 0);	\
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {		\
			if (is_oid_nil(o[lo])) {			\
				assert(nilcheck);			\
				bt[lo] = TYPE##_nil;			\
				hasnil = true;				\
			} else if (o[lo] < rseq || o[lo] >= rend) {	\
				GDKerror("BATproject: does not match always\n"); \
				return GDK_FAIL;			\
			} else {					\
				v = rt[o[lo] - rseq];			\
				bt[lo] = v;				\
				hasnil |= is_##TYPE##_nil(v);		\
			}						\
		}							\
	}								\
	if (nilcheck && hasnil) {					\
		bn->tnonil = false;					\
		bn->tnil = true;					\
	}								\
	BATsetcount(bn, lo);						\
	return GDK_SUCCEED;						\
}


/* project type switch */
project_loop(bte)
project_loop(sht)
project_loop(int)
project_loop(flt)
project_loop(dbl)
project_loop(lng)
#ifdef HAVE_HGE
project_loop(hge)
#endif

static gdk_return
project_void(BAT *bn, BAT *l, struct canditer *restrict ci, BAT *r)
{
	BUN lo, hi;
	oid *restrict bt;
	oid rseq, rend;

	assert(BATtdense(r));
	rseq = r->hseqbase;
	rend = rseq + BATcount(r);
	bt = (oid *) Tloc(bn, 0);
	bn->tsorted = l->tsorted;
	bn->trevsorted = l->trevsorted;
	bn->tkey = l->tkey;
	bn->tnonil = true;
	bn->tnil = false;
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < rseq || o >= rend) {
				GDKerror("BATproject: does not match always\n");
				return GDK_FAIL;
			}
			bt[lo] = o - rseq + r->tseqbase;
		}
	} else {
		const oid *o = (const oid *) Tloc(l, 0);
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			if (o[lo] < rseq || o[lo] >= rend) {
				if (is_oid_nil(o[lo])) {
					bt[lo] = oid_nil;
					bn->tnonil = false;
					bn->tnil = true;
				} else {
					GDKerror("BATproject: does not match always\n");
					return GDK_FAIL;
				}
			} else {
				bt[lo] = o[lo] - rseq + r->tseqbase;
			}
		}
	}
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
}

static gdk_return
project_cand(BAT *bn, BAT *l, struct canditer *restrict lci, BAT *r)
{
	BUN lo, hi;
	oid *restrict bt;
	oid rseq, rend;
	struct canditer rci;

	rseq = r->hseqbase;
	rend = rseq + BATcount(r);
	canditer_init(&rci, NULL, r);
	bt = (oid *) Tloc(bn, 0);
	bn->tsorted = l->tsorted;
	bn->trevsorted = l->trevsorted;
	bn->tkey = l->tkey;
	bn->tnonil = true;
	bn->tnil = false;
	if (lci) {
		for (lo = 0, hi = lci->ncand; lo < hi; lo++) {
			oid o = canditer_next(lci);
			if (o < rseq || o >= rend) {
				GDKerror("BATproject: does not match always\n");
				return GDK_FAIL;
			}
			bt[lo] = canditer_idx(&rci, o - rseq);
		}
	} else {
		const oid *o = (const oid *) Tloc(l, 0);
		for (lo = 0, hi = BATcount(l); lo < hi; lo++) {
			if (o[lo] < rseq || o[lo] >= rend) {
				if (is_oid_nil(o[lo])) {
					bt[lo] = oid_nil;
					bn->tnonil = false;
					bn->tnil = true;
				} else {
					GDKerror("BATproject: does not match always\n");
					return GDK_FAIL;
				}
			} else {
				bt[lo] = canditer_idx(&rci, o[lo] - rseq);
			}
		}
	}
	BATsetcount(bn, lo);
	return GDK_SUCCEED;
}

static gdk_return
project_any(BAT *bn, BAT *l, struct canditer *restrict ci, BAT *r, bool nilcheck)
{
	BUN lo, hi;
	BATiter ri;
	int (*cmp)(const void *, const void *) = ATOMcompare(r->ttype);
	const void *nil = ATOMnilptr(r->ttype);
	const void *v;
	oid rseq, rend;

	ri = bat_iterator(r);
	rseq = r->hseqbase;
	rend = rseq + BATcount(r);
	if (ci) {
		for (lo = 0, hi = ci->ncand; lo < hi; lo++) {
			oid o = canditer_next(ci);
			if (o < rseq || o >= rend) {
				GDKerror("BATproject: does not match always\n");
				goto bunins_failed;
			} else {
				v = BUNtail(ri, o - rseq);
				tfastins_nocheck(bn, lo, v, Tsize(bn));
				if (nilcheck && bn->tnonil && cmp(v, nil) == 0) {
					bn->tnonil = false;
					bn->tnil = true;
				}
			}
		}
	} else {
		const oid *o = (const oid *) Tloc(l, 0);

		for (lo = 0, hi = lo + BATcount(l); lo < hi; lo++) {
			if (o[lo] < rseq || o[lo] >= rend) {
				if (is_oid_nil(o[lo])) {
					tfastins_nocheck(bn, lo, nil, Tsize(bn));
					bn->tnonil = false;
					bn->tnil = true;
				} else {
					GDKerror("BATproject: does not match always\n");
					goto bunins_failed;
				}
			} else {
				v = BUNtail(ri, o[lo] - rseq);
				tfastins_nocheck(bn, lo, v, Tsize(bn));
				if (nilcheck && bn->tnonil && cmp(v, nil) == 0) {
					bn->tnonil = false;
					bn->tnil = true;
				}
			}
		}
	}
	BATsetcount(bn, lo);
	bn->theap.dirty = true;
	return GDK_SUCCEED;
bunins_failed:
	return GDK_FAIL;
}

BAT *
BATproject(BAT *l, BAT *r)
{
	BAT *bn;
	oid lo, hi;
	gdk_return res;
	int tpe = ATOMtype(r->ttype);
	bool nilcheck = true, stringtrick = false;
	BUN lcount = BATcount(l), rcount = BATcount(r);
	struct canditer ci, *lci = NULL;
	lng t0 = 0;

	ALGODEBUG t0 = GDKusec();

	ALGODEBUG fprintf(stderr, "#BATproject(l=" ALGOBATFMT ","
			  "r=" ALGOBATFMT ")\n",
			  ALGOBATPAR(l), ALGOBATPAR(r));

	assert(ATOMtype(l->ttype) == TYPE_oid);

	if (BATtdense(l) && lcount > 0) {
		lo = l->tseqbase;
		hi = l->tseqbase + lcount;
		if (lo < r->hseqbase || hi > r->hseqbase + rcount) {
			GDKerror("BATproject: does not match always\n");
			return NULL;
		}
		bn = BATslice(r, lo - r->hseqbase, hi - r->hseqbase);
		BAThseqbase(bn, l->hseqbase);
		ALGODEBUG fprintf(stderr, "#BATproject(l=%s,r=%s)=" ALGOOPTBATFMT " (slice)\n",
				  BATgetId(l), BATgetId(r),  ALGOOPTBATPAR(bn));
		return bn;
	}
	if (l->ttype == TYPE_void && l->tvheap != NULL) {
		/* l is candidate list with exceptions */
		lcount = canditer_init(&ci, NULL, l);
		lci = &ci;
	}
	/* if l has type void, it is either empty or not dense (i.e. nil) */
	if (lcount == 0 || (l->ttype == TYPE_void && lci == NULL) ||
	    (r->ttype == TYPE_void && is_oid_nil(r->tseqbase))) {
		/* trivial: all values are nil (includes no entries at all) */
		const void *nil = ATOMnilptr(r->ttype);

		bn = BATconstant(l->hseqbase, r->ttype == TYPE_oid ? TYPE_void : r->ttype,
				 nil, lcount, TRANSIENT);
		if (bn != NULL &&
		    ATOMtype(bn->ttype) == TYPE_oid &&
		    BATcount(bn) == 0) {
			BATtseqbase(bn, 0);
		}
		ALGODEBUG fprintf(stderr, "#BATproject(l=%s,r=%s)=" ALGOOPTBATFMT " (constant)\n",
				  BATgetId(l), BATgetId(r), ALGOOPTBATPAR(bn));
		return bn;
	}

	if (ATOMstorage(tpe) == TYPE_str &&
	    l->tnonil &&
	    (rcount == 0 ||
	     lcount > (rcount >> 3) ||
	     r->batRestricted == BAT_READ)) {
		/* insert strings as ints, we need to copy the string
		 * heap whole sale; we can't do this if there are nils
		 * in the left column, and we won't do it if the left
		 * is much smaller than the right and the right is
		 * writable (meaning we have to actually copy the
		 * right string heap) */
		tpe = r->twidth == 1 ? TYPE_bte : (r->twidth == 2 ? TYPE_sht : (r->twidth == 4 ? TYPE_int : TYPE_lng));
		/* int's nil representation is a valid offset, so
		 * don't check for nils */
		nilcheck = false;
		stringtrick = true;
	}
	bn = COLnew(l->hseqbase, tpe, lcount, TRANSIENT);
	if (bn == NULL) {
		ALGODEBUG fprintf(stderr, "#BATproject(l=%s,r=%s)=0\n",
				  BATgetId(l), BATgetId(r));
		return NULL;
	}
	if (stringtrick) {
		/* "string type" */
		bn->tsorted = false;
		bn->trevsorted = false;
		bn->tkey = false;
		bn->tnonil = false;
	} else {
		/* be optimistic, we'll clear these if necessary later */
		bn->tnonil = true;
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tkey = true;
		if (l->tnonil && r->tnonil)
			nilcheck = false; /* don't bother checking: no nils */
		if (tpe != TYPE_oid &&
		    tpe != ATOMstorage(tpe) &&
		    !ATOMvarsized(tpe) &&
		    ATOMcompare(tpe) == ATOMcompare(ATOMstorage(tpe)) &&
		    (!nilcheck ||
		     ATOMnilptr(tpe) == ATOMnilptr(ATOMstorage(tpe)))) {
			/* use base type if we can:
			 * only fixed sized (no advantage for variable sized),
			 * compare function identical (for sorted check),
			 * either no nils, or nil representation identical,
			 * not oid (separate case for those) */
			tpe = ATOMstorage(tpe);
		}
	}
	bn->tnil = false;

	switch (tpe) {
	case TYPE_bte:
		res = project_bte(bn, l, lci, r, nilcheck);
		break;
	case TYPE_sht:
		res = project_sht(bn, l, lci, r, nilcheck);
		break;
	case TYPE_int:
		res = project_int(bn, l, lci, r, nilcheck);
		break;
	case TYPE_flt:
		res = project_flt(bn, l, lci, r, nilcheck);
		break;
	case TYPE_dbl:
		res = project_dbl(bn, l, lci, r, nilcheck);
		break;
	case TYPE_lng:
		res = project_lng(bn, l, lci, r, nilcheck);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		res = project_hge(bn, l, lci, r, nilcheck);
		break;
#endif
	case TYPE_oid:
		if (BATtdense(r)) {
			res = project_void(bn, l, lci, r);
		} else if (r->ttype == TYPE_void) {
			assert(r->tvheap != NULL);
			res = project_cand(bn, l, lci, r);
		} else {
#if SIZEOF_OID == SIZEOF_INT
			res = project_int(bn, l, lci, r, nilcheck);
#else
			res = project_lng(bn, l, lci, r, nilcheck);
#endif
		}
		break;
	default:
		res = project_any(bn, l, lci, r, nilcheck);
		break;
	}

	if (res != GDK_SUCCEED)
		goto bailout;

	/* handle string trick */
	if (stringtrick) {
		if (r->batRestricted == BAT_READ) {
			/* really share string heap */
			assert(r->tvheap->parentid > 0);
			BBPshare(r->tvheap->parentid);
			bn->tvheap = r->tvheap;
		} else {
			/* make copy of string heap */
			bn->tvheap = (Heap *) GDKzalloc(sizeof(Heap));
			if (bn->tvheap == NULL)
				goto bailout;
			bn->tvheap->parentid = bn->batCacheid;
			bn->tvheap->farmid = BBPselectfarm(bn->batRole, TYPE_str, varheap);
			stpconcat(bn->tvheap->filename, BBP_physical(bn->batCacheid), ".theap", NULL);
			if (HEAPcopy(bn->tvheap, r->tvheap) != GDK_SUCCEED)
				goto bailout;
		}
		bn->ttype = r->ttype;
		bn->tvarsized = true;
		bn->twidth = r->twidth;
		bn->tshift = r->tshift;

		bn->tnil = false; /* we don't know */
	}
	/* some properties follow from certain combinations of input
	 * properties */
	if (BATcount(bn) <= 1) {
		bn->tkey = true;
		bn->tsorted = true;
		bn->trevsorted = true;
	} else {
		bn->tkey = l->tkey && r->tkey;
		bn->tsorted = (l->tsorted & r->tsorted) | (l->trevsorted & r->trevsorted);
		bn->trevsorted = (l->tsorted & r->trevsorted) | (l->trevsorted & r->tsorted);
	}
	bn->tnonil |= l->tnonil & r->tnonil;

	if (!BATtdense(r))
		BATtseqbase(bn, oid_nil);
	ALGODEBUG fprintf(stderr, "#BATproject(l=%s,r=%s)=" ALGOBATFMT "%s " LLFMT "us\n",
			  BATgetId(l), BATgetId(r), ALGOBATPAR(bn),
			  bn->ttype == TYPE_str && bn->tvheap == r->tvheap ? " shared string heap" : "",
			  GDKusec() - t0);
	return bn;

  bailout:
	BBPreclaim(bn);
	return NULL;
}

/* Calculate a chain of BATproject calls.
 * The argument is a NULL-terminated array of BAT pointers.
 * This function is equivalent (apart from reference counting) to a
 * sequence of calls
 * bn = BATproject(bats[0], bats[1]);
 * bn = BATproject(bn, bats[2]);
 * ...
 * bn = BATproject(bn, bats[n-1]);
 * return bn;
 * where none of the intermediates are actually produced (and bats[n]==NULL).
 * Note that all BATs except the last must have type oid/void.
 */
BAT *
BATprojectchain(BAT **bats)
{
	/* For each BAT we remember some important details, however,
	 * dense-tailed BATs are optimized away in this list by
	 * combining their details with the following BAT's details.
	 * For each element in the chain, the value must be in the
	 * range [hlo..hlo+cnt) of the following element.  If a BAT in
	 * the chain is dense-tailed, the value tseq is the lowest
	 * value (corresponding with hlo).  Since dense-tailed BATs
	 * are combined with their successors, tseq will only be used
	 * for the last element. */
	struct {
		const oid *vals; /* if not dense, start of relevant tail values */
		BAT *b;		/* the BAT */
		oid hlo;	/* lowest allowed oid to index the BAT */
		BUN cnt;	/* size of allowed index range */
		struct canditer ci; /* candidate iterator for cand w/ except. */
	} *ba;
	int i, n, tpe;
	BAT *b, *bn;
	oid o;
	const void *nil;	/* nil representation for last BAT */
	BUN p, cnt, off;
	oid hseq, tseq;
	bool allnil = false, nonil = true;
	bool stringtrick = false;
	bool issorted = true;	/* result sorted if all bats sorted */

	/* count number of participating BATs and allocate some
	 * temporary work space */
	for (n = 0; bats[n]; n++)
		;
	ba = GDKzalloc(sizeof(*ba) * n);
	if (ba == NULL)
		return NULL;
	b = *bats++;
	cnt = BATcount(b);	/* this will be the size of the output */
	hseq = b->hseqbase;	/* this will be the seqbase of the output */
	tseq = oid_nil;		/* initialize, but overwritten before use */
	off = 0;		/* this will be the BUN offset into last BAT */
	for (i = n = 0; b != NULL; n++, i++) {
		nonil &= b->tnonil; /* not guaranteed without nils */
		issorted &= b->tsorted;
		if (!allnil) {
			if (n > 0 && ba[i-1].vals == NULL) {
				/* previous BAT was dense: we will
				 * combine it with this one */
				i--;
				assert(off == 0);
				if (tseq + ba[i].cnt > b->hseqbase + BATcount(b)) {
					if (tseq > b->hseqbase + BATcount(b))
						ba[i].cnt = 0;
					else
						ba[i].cnt = b->hseqbase + BATcount(b) - tseq;
				}
				if (BATtdense(b)) {
					if (tseq > b->hseqbase) {
						tseq = tseq - b->hseqbase + b->tseqbase;
					} else if (tseq < b->hseqbase) {
						if (b->hseqbase - tseq > ba[i].cnt) {
							ba[i].cnt = 0;
						} else {
							ba[i].hlo += b->hseqbase - tseq;
							ba[i].cnt -= b->hseqbase - tseq;
							tseq = b->tseqbase;
						}
					} else {
						tseq = b->tseqbase;
					}
				} else {
					if (tseq > b->hseqbase) {
						off = tseq - b->hseqbase;
					} else if (tseq < b->hseqbase) {
						if (b->hseqbase - tseq > ba[i].cnt) {
							ba[i].cnt = 0;
						} else {
							ba[i].hlo += b->hseqbase - tseq;
							ba[i].cnt -= b->hseqbase - tseq;
						}
					}
					if (b->ttype == TYPE_void &&
					    is_oid_nil(b->tseqbase)) {
						tseq = oid_nil;
						allnil = true;
					} else if (b->ttype == TYPE_void) {
						assert(b->tvheap != NULL);
						canditer_init(&ba[i].ci, NULL, b);
						/* make sure .vals != NULL */
						ba[i].vals = ba[i].ci.oids;
						canditer_setidx(&ba[i].ci, off);
					} else
						ba[i].vals = (const oid *) Tloc(b, off);
				}
			} else {
				ba[i].hlo = b->hseqbase;
				ba[i].cnt = BATcount(b);
				off = 0;
				if (BATtdense(b)) {
					tseq = b->tseqbase;
					ba[i].vals = NULL;
				} else {
					tseq = oid_nil;
					if (b->ttype == TYPE_void &&
					    is_oid_nil(b->tseqbase))
						allnil = true;
					else if (b->ttype == TYPE_void) {
						assert(b->tvheap != NULL);
						canditer_init(&ba[i].ci, NULL, b);
						/* make sure .vals != NULL */
						ba[i].vals = ba[i].ci.oids;
					} else
						ba[i].vals = (const oid *) Tloc(b, 0);
				}
			}
		}
		ba[i].b = b;
		if ((ba[i].cnt == 0 && cnt > 0) ||
		    (i == 0 && (ba[0].cnt < cnt || ba[0].hlo != hseq))) {
			GDKerror("BATprojectchain: does not match always\n");
			GDKfree(ba);
			return NULL;
		}
		b = *bats++;
	}
	assert(n >= 1);		/* not too few inputs */
	b = bats[-2];		/* the last BAT in the list (bats[-1]==NULL) */
	tpe = ATOMtype(b->ttype); /* its type */
	nil = ATOMnilptr(tpe);
	if (allnil) {
		/* somewhere on the way we encountered a void-nil BAT */
		ALGODEBUG fprintf(stderr, "#BATprojectchain with %d BATs, size "BUNFMT", type %s, all nil\n", n, cnt, ATOMname(tpe));
		GDKfree(ba);
		return BATconstant(hseq, tpe == TYPE_oid ? TYPE_void : tpe, nil, cnt, TRANSIENT);
	}
	if (i == 1) {
		/* only dense-tailed BATs before last: we can return a
		 * slice and manipulate offsets and head seqbase */
		GDKfree(ba);
		if (BATtdense(b)) {
			bn = BATdense(hseq, tseq, cnt);
		} else {
			bn = BATslice(b, off, off + cnt);
			if (bn == NULL)
				return NULL;
			BAThseqbase(bn, hseq);
		}
		ALGODEBUG fprintf(stderr, "#BATprojectchain with %d BATs, "
				  "size " BUNFMT ", type %s, using "
				  "BATslice(" BUNFMT "," BUNFMT ")="
				  ALGOOPTBATFMT "\n",
				  n, cnt, ATOMname(tpe), off, off + cnt,
				  ALGOOPTBATPAR(bn));
		return bn;
	}
	ALGODEBUG fprintf(stderr, "#BATprojectchain with %d (%d) BATs, size "BUNFMT", type %s\n", n, i, cnt, ATOMname(tpe));

	if (nonil &&
	    cnt > 0 &&
	    ATOMstorage(b->ttype) == TYPE_str &&
	    b->batRestricted == BAT_READ) {
		stringtrick = true;
		tpe = b->twidth == 1 ? TYPE_bte : (b->twidth == 2 ? TYPE_sht : (b->twidth == 4 ? TYPE_int : TYPE_lng));
	}

	bn = COLnew(hseq, tpe, cnt, TRANSIENT);
	if (bn == NULL || cnt == 0) {
		GDKfree(ba);
		ALGODEBUG fprintf(stderr, "#BATprojectchain with %d BATs, "
				  "size " BUNFMT ", type %s="
				  ALGOOPTBATFMT "\n",
				  n, cnt, ATOMname(tpe), ALGOOPTBATPAR(bn));
		return bn;
	}
	bn->tnil = false;	/* we're not paying attention to this */
	bn->tnonil = nonil;
	n = i - 1;		/* ba[n] is last BAT */

/* figure out the "other" type, i.e. not compatible with oid */
#if SIZEOF_OID == SIZEOF_INT
#define OTPE	lng
#define TOTPE	TYPE_lng
#else
#define OTPE	int
#define TOTPE	TYPE_int
#endif
	if (ATOMstorage(bn->ttype) == ATOMstorage(TYPE_oid)) {
		/* oids all the way (or the final tail type is a fixed
		 * sized atom the same size as oid) */
		oid *restrict v = (oid *) Tloc(bn, 0);

		if (ba[n].vals == NULL) {
			/* last BAT is dense-tailed */
			lng offset = 0;

			offset = (lng) tseq - (lng) ba[n].hlo;
			ba[n].cnt += ba[n].hlo; /* upper bound of last BAT */
			for (p = 0; p < cnt; p++) {
				o = ba[0].ci.oids ? canditer_next(&ba[0].ci) : ba[0].vals[p];
				for (i = 1; i < n; i++) {
					if (is_oid_nil(o)) {
						bn->tnil = true;
						o = oid_nil;
						break;
					}
					o -= ba[i].hlo;
					if (o >= ba[i].cnt) {
						GDKerror("BATprojectchain: does not match always\n");
						goto bunins_failed;
					}
					o = ba[i].ci.oids ? canditer_idx(&ba[i].ci, (BUN) o + ba[i].ci.next) : ba[i].vals[o];
				}
				if (is_oid_nil(o)) {
					*v++ = *(oid *) nil;
				} else {
					if (o < ba[n].hlo || o >= ba[n].cnt) {
						GDKerror("BATprojectchain: does not match always\n");
						goto bunins_failed;
					}
					*v++ = (oid) (o + offset);
				}
			}
		} else {
			/* last BAT is materialized */
			for (p = 0; p < cnt; p++) {
				o = ba[0].ci.oids ? canditer_next(&ba[0].ci) : ba[0].vals[p];
				for (i = 1; i <= n; i++) { /* note "<=" */
					if (is_oid_nil(o)) {
						bn->tnil = true;
						o = oid_nil;
						break;
					}
					o -= ba[i].hlo;
					if (o >= ba[i].cnt) {
						GDKerror("BATprojectchain: does not match always\n");
						goto bunins_failed;
					}
					o = ba[i].ci.oids ? canditer_idx(&ba[i].ci, (BUN) o + ba[i].ci.next) : ba[i].vals[o];
				}
				*v++ = (is_oid_nil(o)) & !stringtrick ? *(oid *) nil : o;
			}
		}
		assert(v == (oid *) Tloc(bn, cnt));
	} else if (ATOMstorage(b->ttype) == ATOMstorage(TOTPE)) {
		/* one special case for a fixed sized BAT */
		const OTPE *src = (const OTPE *) Tloc(b, off);
		OTPE *restrict dst = (OTPE *) Tloc(bn, 0);

		for (p = 0; p < cnt; p++) {
			o = ba[0].ci.oids ? canditer_next(&ba[0].ci) : ba[0].vals[p];
			for (i = 1; i < n; i++) {
				if (is_oid_nil(o)) {
					bn->tnil = true;
					o = oid_nil;
					break;
				}
				o -= ba[i].hlo;
				if (o >= ba[i].cnt) {
					GDKerror("BATprojectchain: does not match always\n");
					goto bunins_failed;
				}
				o = ba[i].ci.oids ? canditer_idx(&ba[i].ci, (BUN) o + ba[i].ci.next) : ba[i].vals[o];
			}
			if (is_oid_nil(o)) {
				*dst++ = * (OTPE *) nil;
			} else {
				o -= ba[n].hlo;
				if (o >= ba[n].cnt) {
					GDKerror("BATprojectchain: does not match always\n");
					goto bunins_failed;
				}
				*dst++ = src[o];
			}
		}
	} else if (ATOMvarsized(tpe)) {
		/* generic code for var-sized atoms */
		BATiter bi = bat_iterator(b);
		const void *v;

		assert(!stringtrick);
		for (p = 0; p < cnt; p++) {
			o = ba[0].ci.oids ? canditer_next(&ba[0].ci) : ba[0].vals[p];
			for (i = 1; i < n; i++) {
				if (is_oid_nil(o)) {
					bn->tnil = true;
					o = oid_nil;
					break;
				}
				o -= ba[i].hlo;
				if (o >= ba[i].cnt) {
					GDKerror("BATprojectchain: does not match always\n");
					goto bunins_failed;
				}
				o = ba[i].ci.oids ? canditer_idx(&ba[i].ci, (BUN) o + ba[i].ci.next) : ba[i].vals[o];
			}
			if (is_oid_nil(o)) {
				v = nil;
			} else {
				o -= ba[n].hlo;
				if (o >= ba[n].cnt) {
					GDKerror("BATprojectchain: does not match always\n");
					goto bunins_failed;
				}
				v = BUNtvar(bi, o + off);
			}
			bunfastappVAR(bn, v);
		}
	} else {
		/* generic code for fixed-sized atoms */
		BATiter bi = bat_iterator(b);
		const void *v;

		for (p = 0; p < cnt; p++) {
			o = ba[0].ci.oids ? canditer_next(&ba[0].ci) : ba[0].vals[p];
			for (i = 1; i < n; i++) {
				if (is_oid_nil(o)) {
					bn->tnil = true;
					o = oid_nil;
					break;
				}
				o -= ba[i].hlo;
				if (o >= ba[i].cnt) {
					GDKerror("BATprojectchain: does not match always\n");
					goto bunins_failed;
				}
				o = ba[i].ci.oids ? canditer_idx(&ba[i].ci, (BUN) o + ba[i].ci.next) : ba[i].vals[o];
			}
			if (is_oid_nil(o)) {
				v = nil;
			} else {
				o -= ba[n].hlo;
				if (o >= ba[n].cnt) {
					GDKerror("BATprojectchain: does not match always\n");
					goto bunins_failed;
				}
				v = BUNtloc(bi, o + off);
			}
			bn->theap.free += Tsize(bn);
			ATOMputFIX(bn->ttype, Tloc(bn, bn->batCount), v);
			bn->batCount++;
		}
	}
	bn->theap.dirty = true;
	BATsetcount(bn, cnt);
	if (stringtrick) {
		bn->tnil = false;
		bn->tnonil = nonil;
		bn->tkey = false;
		BBPshare(b->tvheap->parentid);
		bn->tvheap = b->tvheap;
		bn->ttype = b->ttype;
		bn->tvarsized = true;
		bn->twidth = b->twidth;
		bn->tshift = b->tshift;
	}
	bn->tsorted = bn->trevsorted = cnt <= 1;
	bn->tsorted |= issorted;
	bn->tseqbase = oid_nil;
	GDKfree(ba);
	ALGODEBUG fprintf(stderr, "#BATprojectchain()=" ALGOBATFMT "\n",
			  ALGOBATPAR(bn));
	return bn;

  bunins_failed:
	GDKfree(ba);
	BBPreclaim(bn);
	return NULL;
}
