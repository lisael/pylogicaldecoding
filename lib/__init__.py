from logicaldecoding._logicaldecoding import Reader as _Reader


class Reader(_Reader):

    def event(self, value):
        raise NotImplementedError("event must be overridden in client")
