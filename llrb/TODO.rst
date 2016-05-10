load:

* pass new snapshot versions to a subset of reader-routines.
* load llrb with mvcc, with tunable number of readers.
* use production file to load key,value. include lookups and ranges.

verify :

* verify memory leak
* manage seqno accounting in the verify code.
* set metadata vbno, access, vbuuid, bornseqno, deadseqno.
