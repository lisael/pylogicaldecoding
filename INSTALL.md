__Sorry, no pypi package at the moment...

Tested only on Debian Jessie and Ubuntu Trusty (should work on any version
of these distribs, and many more UNIXes-like systems).

# packages

- postgresql >= 9.4 (on the master)
- libpq5-dev (on the comsumer)

please use pg provided packages (TODO: URL)

# pylogicaldecoding

Clone the repository and create and new Python2.7 virtualenv.

```
cd pylogicaldecoding
make install
```

# prepare postgres

## PG configuration

```
sudo vi /etc/postgresql/9.4/main/postgresql.conf
# change max_replication_slots to > 0
# change wal_level to logical
# change max___wal_senders to > 0
sudo vi /etc/postgresql/9.4/main/pg_hba.conf
# uncomment or create the line 
# local   replication     postgres    peer
sudo /etc/init.d/postgresql restart
```

## create a replication slot

Of course you can write your own decoder... however, for testing purpose, you
can use the one provided by postgresql. Unfortunatly it may not be packaged yet,
you may have to compile and install it by hand.

If you're lucky this may be enough ("test_slot" is mandatory, it's hard coded
in the C code at the moment TODO).

```
sudo -u postgres psql
postgres=# SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
 slot_name | xlog_position 
-----------+---------------
 test_slot | 0/16ACC80
(1 row)
```

if you get something like

`ERROR:  could not access file "test_decoding": No such file or directory`

Your distrib did not compile the test decoder. 

# install postgres’ `test_decoding.so` contrib

## lib requirements

- libreadline-dev
- postgresql-server-dev-9.4

## Download the source

TODO: URL

Be careful to download exactly the same version as your postgresql package.

Untar, Unzip...

## Compile

```
cd postgresql-9.4.X/
./configure --with-includes=`pg_config --includedir-server`
cd contrig/test_decoding/
make all
sudo cp test_decoding.so `pg_config --pkglibdir`
```

