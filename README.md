# TypeDB


## Running the Benchmark

### Sqlite

Install using `brew`. Run using
```
python tpcc.py --config=sqlite.config --no-load sqlite   
```

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