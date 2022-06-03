# MongoDB input plugin for Embulk

[![Build Status](https://travis-ci.org/hakobera/embulk-input-mongodb.svg)](https://travis-ci.org/hakobera/embulk-input-mongodb)

MongoDB input plugin for Embulk loads records from MongoDB.
This plugin loads documents as single-column records (column name is "record"). You can use filter plugins such as [embulk-filter-expand_json](https://github.com/civitaspo/embulk-filter-expand_json) or [embulk-filter-add_time](https://github.com/treasure-data/embulk-filter-add_time) to convert the json column to typed columns. [Rename filter](https://www.embulk.org/docs/built-in.html#rename-filter-plugin) is also useful to rename the typed columns.

## Overview

This plugin only works with embulk >= 0.8.8.

* **Plugin type**: input
* **Guess supported**: no

## Configuration

- Connection parameters
  One of them is required.
  
  - use MongoDB connection string URI
    - **uri**: [MongoDB connection string URI](https://docs.mongodb.org/manual/reference/connection-string/) (e.g. 'mongodb://localhost:27017/mydb') (string, required)
  - use separated URI parameters
    - **hosts**: list of hosts. `hosts` are pairs of host(string, required) and port(integer, optional, default: 27017)
    - **auth_method**: Auth method. One of `scram-sha-1`, `mongodb-cr`, `auto` (string, optional, default: null)
    - **auth_source**: Auth source. The database name where the user is defined (string, optional, default: null)
    - **user**: (string, optional)
    - **password**:  (string, optional)
    - **database**:  (string, required)
    - **tls**: `true` to use TLS to connect to the host (boolean, optional, default: `false`)
    - **tls_insecure**: `true` to disable various certificate validations (boolean, optional, default: `false`)
      - The option is similar to an option of the official `mongo` command.
      - See also: https://www.mongodb.com/docs/manual/reference/connection-string/#mongodb-urioption-urioption.tlsInsecure
- **collection**: source collection name (string, required)
- **fields**: **(deprecated)** ~~hash records that has the following two fields (array, required)~~
  ~~- name: Name of the column~~
  ~~- type: Column types as follows~~
    ~~- boolean~~
    ~~- long~~
    ~~- double~~
    ~~- string~~
    ~~- timestamp~~
- **id_field_name** Name of Object ID field name. Set if you want to change the default name `_id` (string, optional, default: "_id")
- **query**: A JSON document used for [querying](https://docs.mongodb.com/manual/tutorial/query-documents/) on the source collection. Documents are loaded from the colleciton if they match with this condition. (string, optional)
- **projection**: A JSON document used for [projection](https://docs.mongodb.com/manual/reference/operator/projection/positional/) on query results. Fields in a document are used only if they match with this condition. (string, optional)
- **sort**: Ordering of results (string, optional)
- **aggregation**: Aggregation query (string, optional) See [Aggregation query](#aggregation-query) for more detail.
- **batch_size**: Limits the number of objects returned in one [batch](https://mongodb.github.io/mongo-java-driver/3.8/javadoc/com/mongodb/DBCursor.html#batchSize-int-) (integer, optional, default: 10000)
- **incremental_field** List of field name (list, optional, can't use with sort option)
- **last_record** Last loaded record for incremental load (hash, optional)
- **stop_on_invalid_record** Stop bulk load transaction if a document includes invalid record (such as unsupported object type) (boolean, optional, default: false)
- **json_column_name**: column name used in outputs (string, optional, default: "record")

## Example

### Authentication

#### Use separated URI prameters

```yaml
in:
  type: mongodb
  hosts:
  - {host: localhost, port: 27017}
  user:  myuser
  password: mypassword
  database: my_database
  auth_method: scram-sha-1
  auth_source: auth_db
  collection: "my_collection"
```

If you set `auth_method: auto`, The client will negotiate the best mechanism based on the version of the server that the client is authenticating to.

If the server version is 3.0 or higher, the driver will authenticate using the SCRAM-SHA-1 mechanism.

Otherwise, the driver will authenticate using the MONGODB_CR mechanism. 

#### Use URI String

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database?authMechanism=SCRAM-SHA-1&authSource=another_database
```

### Exporting all objects

#### Specify with MongoDB connection string URI.

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
  collection: "my_collection"
```

#### Specify with separated URI parameters.

```yaml
in:
  type: mongodb
  hosts:
  - {host: localhost, port: 27017}
  - {host: example.com, port: 27017}
  user: myuser
  password: mypassword
  database: my_database
  collection: "my_collection"
```

### Filtering documents by query and projection

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
  collection: "my_collection"
  query: '{ field1: { $gte: 3 } }'
  projection: '{ "_id": 1, "field1": 1, "field2": 0 }'
  sort: '{ "field1": 1 }'
```

### Incremental loading

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
  collection: "my_collection"
  query: '{ field1: { $gt: 3 } }'
  projection: '{ "_id": 1, "field1": 1, "field2": 1 }'
  incremental_field:
    - "field2"
  last_record: {"field2": 13215}
```

Plugin will create new query and sort value.
You can't use `incremental_field` option with `sort` option at the same time.

```
query { field1: { $gt: 3 }, field2: { $gt: 13215}}
sort {"field2", 1} # field2 ascending
```

You have to specify last_record with special characters when field type is `ObjectId` or `DateTime`.

```yaml
# ObjectId field
in:
  type: mongodb
  incremental_field:
    - "_id"
  last_record: {"_id": {"$oid": "5739b2261c21e58edfe39716"}}

# DateTime field
in:
  type: mongodb
  incremental_field:
    - "time_field"
  last_record: {"time_field": {"$date": "2015-01-25T13:23:15.000Z"}}
```

#### Run Incremental load

```
$ embulk run /path/to/config.yml -c config-diff.yml
```

### Aggregation query

This plugin supports aggregation query. You can write complex query like below.

`aggregation` option can't be used with `sort`, `limit`, `skip`, `query` option. Incremental load also doesn't work with aggregation query.

```yaml
in:
  type: mongodb
  aggregation: { $match: {"int32_field":{"$gte":5 },} }
```

See also [Aggregation — MongoDB Manual](https://docs.mongodb.com/manual/aggregation/) and [Aggregation Pipeline Stages — MongoDB Manual](https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/)

### Advanced usage with filter plugins

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
  collection: "my_collection"
  query: '{ "age": { $gte: 3 } }'
  projection: '{ "_id": 1, "age": 1, "ts": 1, "firstName": 1, "lastName": 1 }'

filters:
  # convert json column into typed columns
  - type: expand_json
    json_column_name: record
    expanded_columns:
      - {name: _id, type: long}
      - {name: ts, type: string}
      - {name: firstName, type: string}
      - {name: lastName, type: string}

  # rename column names
  - type: rename
    columns:
      _id: id
      firstName: first_name
      lastName: last_name

  # convert string "ts" column into timestamp "time" column
  - type: add_time
    from_column:
      name: ts
      timestamp_format: "%Y-%m-%dT%H:%M:%S.%N%z"
    to_column:
      name: time
      type: timestamp
```

## Build

```
$ ./gradlew gem
```

## Test

Firstly install Docker and Docker compose then `docker-compose up -d`,
so that an MongoDB server will be locally launched then you can run tests with `./gradlew test`.

```sh
$ docker-compose up -d
Creating embulk-input-mongodb_server ... done
Creating mongo-express               ... done
Creating mongoClientTemp             ... done

$ docker-compose ps
           Name                          Command                 State                            Ports
------------------------------------------------------------------------------------------------------------------------------
embulk-input-mongodb_server   docker-entrypoint.sh mongod      Up           0.0.0.0:27017->27017/tcp, 0.0.0.0:27018->27018/tcp
mongo-express                 tini -- /docker-entrypoint ...   Up           0.0.0.0:8081->8081/tcp
mongoClientTemp               docker-entrypoint.sh mongo ...   Restarting

$ ./gradlew test  # -t to watch change of files and rebuild continuously
```
