from logicaldecoding._logicaldecoding import Reader as _Reader

class Reader(_Reader):
    def __init__(self, host=None, port=None, username=None,
                 dbname=u"postgres", password=None,
                 progname=u"pylogicaldecoding"):
        super(Reader, self).__init__(dict(
            host=host,
            port=port,
            dbname=dbname,
            username=username,
            password=password,
            progname=progname,
        ))

    def event(self, value):
        raise NotImplementedError("event must be overridden in client")
