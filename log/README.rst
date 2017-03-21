packages can import log and use its methods like,

.. code-block:: go

    import github.com/prataprc/log

    func myfunc() {
        ..
        log.Fatalf(...)
        ..
        log.Warnf(...)
        ..
        log.Debugf(...)
    }

note here that *log* is not a object name, it resolves to the imported *log*
package that has exported methods *Fatalf()* *Warnf()* etc ... For more
information please the godoc-umentation for log package.

By default, importing the package will initialize the logger to
default-logger that shall log to standard output.

To use custom logger use the following initializer function in your package or
application

.. code-block:: go

    import github.com/prataprc/log

    var mylogger = newmylogger()

    func init() {
        setts := map[string]interface{}{
            "log.level": "info",
            "log.file":  "",
        }
        SetLogger(mylogger, setts)
    }

*mylogger* should implement the *Logger* interface{}.
