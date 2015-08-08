# pylogicaldecoding
Python interface to PostgreSQL [logical decoding](
http://www.postgresql.org/docs/9.4/static/logicaldecoding.html)

*Status : Very early proof of concept. Beware.*

# Quick start

I suppose you managed to install build dependences, PGHacks, and
pylogicaldecoding (read INSTALL.md).

Open a postgres shell

```
postgres=# CREATE TABLE data(id serial primary key, data text);
CREATE TABLE
```

Run the script into your virtualenv:

```
sudo -u postgres `which python` examples/connect.py
```

From now on , every change to your database will be reflected here

```
postgres=# INSERT INTO data(data) VALUES('1');
INSERT 0 1
```

In pylogicaldecoding console, you should see:

```
got event:
BEGIN 724

got event:
table public.data: INSERT: id[integer]:1 data[text]:'1'

got event:
COMMIT 724
```

Let's play a little, now:

```
postgres=# BEGIN;
BEGIN
postgres=# UPDATE data SET data='2' WHERE id=1;
UPDATE 1
postgres=# DELETE FROM data WHERE id=1;
DELETE 1
postgres=# COMMIT;
COMMIT
postgres=# BEGIN;
BEGIN
postgres=# INSERT INTO data(data) VALUES('1');
INSERT 0 1
postgres=# ROLLBACK;
ROLLBACK
```

```
got event:
BEGIN 728

got event:
table public.data: UPDATE: id[integer]:1 data[text]:'2'

got event:
table public.data: DELETE: id[integer]:1

got event:
COMMIT 728

```

Note that the rollbacked transaction has not been recieved. It's a feature
of test_decoding (it can be changed passing options when the slot is created
and soon in pylogicaldecoding itself)

You may also need to know the values of updated and deleted fields. Postgres
does that, at the expense of larger WAL files and a little overhead. However
these issues are mitigated by postgres using per table settings:

```
postgres=# ALTER TABLE data REPLICA IDENTITY FULL;
ALTER TABLE
```

Check http://www.postgresql.org/docs/9.4/static/sql-altertable.html for more
info.

```
postgres=# BEGIN;
BEGIN
postgres=# INSERT INTO data(data) VALUES('1');
INSERT 0 1
postgres=# UPDATE data SET data='2' WHERE id=2;
UPDATE 1
postgres=# DELETE FROM data WHERE id=2;
DELETE 1
postgres=# COMMIT;
COMMIT
```

```
got event:
BEGIN 732

got event:
table public.data: INSERT: id[integer]:5 data[text]:'1'

got event:
table public.data: UPDATE: old-key: id[integer]:2 data[text]:'1' new-tuple: id[integer]:4 data[text]:'2'

got event:
table public.data: DELETE: id[integer]:2 data[text]:'2'

got event:
COMMIT 732
```

Yay!

# logical decoding vs. NOTIFY

Logical decoding uses streaming replication protocol. The origin
DB server is aware of the WAL entries effectively read and 
treated by the consumer. The guaranties that the comsumer
will never miss an event. In `example/connection.py` you can read

```python
        if value.startswith("COMMIT"):
            self.commits += 1
            if self.commits % 5 == 0:
                self.ack()
```

every 5 DB COMMIT we call `Reader.ack()` which acknowledges the
DB. The database is now free to drop the acknowledged log line. If we
had not call `ack()` (ctl-C before 5 postgres commands), the server
would keep these logs and re-send them when the consumer restart:

terminal 1:

```
(pyld)lisael@laptop:pylogicaldecoding$ sudo -u postgres `which python` examples/connect.py 
```

terminal 2:

```
postgres=# INSERT INTO data(data) VALUES('1');
INSERT 0 1
postgres=# INSERT INTO data(data) VALUES('1');
INSERT 0 1
```

terminal 1:
```
got event:
BEGIN 734

got event:
table public.data: INSERT: id[integer]:7 data[text]:'1'

got event:
COMMIT 734

got event:
BEGIN 735

got event:
table public.data: INSERT: id[integer]:8 data[text]:'1'

got event:
COMMIT 735

^CTraceback (most recent call last):
  File "examples/connect.py", line 21, in <module>
      r.start()
      ValueError: unexpected termination of replication stream:
(pyld)lisael@laptop:pylogicaldecoding$ sudo -u postgres `which python` examples/connect.py 
got event:
BEGIN 734

got event:
table public.data: INSERT: id[integer]:7 data[text]:'1'

got event:
COMMIT 734

got event:
BEGIN 735

got event:
table public.data: INSERT: id[integer]:8 data[text]:'1'

got event:
COMMIT 735
```

The server resent transactions 734 and 735 without any logic on consumer
side.

You can also stop the consumer, send commands in postgres, and watch them
being consumed as soon as you restart the consumer.

OTOH, `NOTIFY` and channels work only during the connection and there's
no way to re-read missed events.

Once again, be careful, it's the killer feature but it can bite you. As soon
as the slot is created, the origin DB keep all unacknowledged WAL on disk.
If your consumer is dead, stuck or if you forget to call `ack()` you run into
big troubles.

# Play, fork, hack, PR, and have fun
Pylogicaldecoding is in its very early stage and there is many improvements
to come.

Every comment, question, critics and code is warmly welcome in github issues.

The core C library is designed to be easily merged in psycopg2, please keep 
this fact in mind in your pull requests (however I dropped some of py3 support
and all of MS windows support as long as the project is a proof of concept)


