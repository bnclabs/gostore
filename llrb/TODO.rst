load:

* pass new snapshot versions to a subset of reader-routines.
* load llrb with mvcc, with tunable number of readers.
* use production file to load key,value. include lookups and ranges.

validate :

* variable key size and value size for llrb validate.
* validate upsertStats[samples] == avg.upsert.height.samples
* validate heightstats[samples] == llrb.Count()
* validate memory leak
* manage seqno accounting in the validate code.
* set metadata vbno, access, vbuuid, bornseqno, deadseqno.
