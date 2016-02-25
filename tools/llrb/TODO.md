load:

* add dot-file dump to load.

validate :

* variable key size and value size for llrb validate.
* validate upsertStats[samples] == avg.upsert.height.samples
* validate heightstats[samples] == llrb.Count()
* validate memory leak
* manage seqno accounting in the validate code.
* set metadata vbno, access, vbuuid, bornseqno, deadseqno.
