To build llrb/
.. code-block:: bash
    $ go build -tags dict

To run test cases
.. code-block:: bash
    $ go test -v -race -tags dict -test.run=. -test.bench=. -test.benchmem=true
