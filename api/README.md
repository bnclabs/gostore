Package api/ define the common interface points with gostore library.
Except for creating a new storage instance, applications should use methods
defined under this package.

**Memory APIs**

* Allocate memory chunks.
* Free memory once it become unused.
* Statistics on memory arena.

**Although minor updates are to be expected on APIs, they are stable enough
for building storage application**.

Panic and Recovery
------------------

**Package api/ never panics.**
