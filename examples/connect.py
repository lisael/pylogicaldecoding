from logicaldecoding import Reader


class MyReader(Reader):
    def __init__(self, *args, **kwargs):
        super(MyReader, self).__init__(*args, **kwargs)
        self.commits = 0

    def event(self, value):
        print "got event:"
        print value
        print
        if value.startswith("COMMIT"):
            self.commits += 1
            if self.commits % 5 == 0:
                self.ack()


if __name__ == '__main__':
    r = MyReader()
    r.stream()
