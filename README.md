# PyTPCC for TypeDB

## TPCC Benchmark

### Notes and changes

TODO


## Running the Benchmark locall on MacOS

### TypeDB

Run `server` as usual.

See `launch.json` config for TPCC running options.

### Postgres

TODO

### MySQL

TODO

### Sqlite

Install using `brew`. Run using
```
python tpcc.py --config=sqlite.config --no-load sqlite   
```

Also see `launch.json` config.

### MongoDB

Install using `brew`. Then start server with replicas enabled:

```
mongod --replSet rs0 --bind_ip localhost --config /opt/homebrew/etc/mongod.conf
```

Install `pymongo`. Then run benchmark:
```
python tpcc.py --config=mongodb.config --no-load mongodb   
```

Use flag `--no-load` if the database has already been created.