To build llrb/
.. code-block:: bash
    $ go build -tags dict

To run test cases
.. code-block:: bash
    $ go test -v -race -tags dict -test.run=. -test.bench=. -test.benchmem=true

Contributing:

Fork the project (storage.go) under your-name <myaccount>.
https://github.com/<myaccount>/storage.go

$ cd <gopath>/src/github.com/prataprc
$ git clone <project-url>
$ git remote add upstream <https://github.com/prataprc/storage.go>
$ git remote add origin <https://github.com/<myaccount>/storage.go>

And there after make the modification and push to
$ git push -u origin master

And raise a pull request via github.

Important point to note is that in your local clone (in laptop) you
will be working under <gopath>/src/github.com/prataprc but you will
be pushing your changesets to
https://github.com/<myaccount>/storage.go

Otherwise you will have "import" issues.

Thanks,
