# PyTPCC for TypeDB

## TPCC Benchmark

### Notes and changes

TODO


## Running the Benchmark locall on MacOS

### TypeDB

* `brew install typedb`
* `typedb server`

See `launch.json` config for TPCC running options.

### Postgres

* `brew install postgresql` (tested with `postgresql@14`)
* `brew services start postgresql`
* `pip install psycopg2-binary` 

Then see `launch.json` for launch configurations.

### MongoDB

* Install using `brew`. 
* `pip install pymongo`

### With transactions (see config)

```
mongod --replSet rs0 --bind_ip localhost --config /opt/homebrew/etc/mongod.conf
```

Then see `launch.json`.

Use flag `--no-load` if the database has already been created.

### Without transactions 

```
mongod --bind_ip localhost --config /opt/homebrew/etc/mongod.conf
```

Then see `launch.json`.

## Neo4j

* `brew install neo4j`
* `brew services start neo4j`
* `pip install neo4j`
* 

