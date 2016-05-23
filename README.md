# MongoDB input plugin for Embulk

[![Build Status](https://travis-ci.org/hakobera/embulk-input-mongodb.svg)](https://travis-ci.org/hakobera/embulk-input-mongodb)

MongoDB input plugin for Embulk loads records from MongoDB.
This plugin loads documents as single-column records (column name is "record"). You can use filter plugins such as [embulk-filter-expand_json](https://github.com/civitaspo/embulk-filter-expand_json) or [embulk-filter-add_time](https://github.com/treasure-data/embulk-filter-add_time) to convert the json column to typed columns. [Rename filter](http://www.embulk.org/docs/built-in.html#rename-filter-plugin) is also useful to rename the typed columns.

## Overview

This plugin only works with embulk >= 0.8.8.

* **Plugin type**: input
* **Guess supported**: no

## Configuration

- **uri**: [MongoDB connection string URI](http://docs.mongodb.org/manual/reference/connection-string/) (e.g. 'mongodb://localhost:27017/mydb') (string, required)
- **collection**: source collection name (string, required)
- **fields**: **(deprecated)** ~~hash records that has the following two fields (array, required)~~
  ~~- name: Name of the column~~
  ~~- type: Column types as follows~~
    ~~- boolean~~
    ~~- long~~
    ~~- double~~
    ~~- string~~
    ~~- timestamp~~
- **query**: a JSON document used for [querying](https://docs.mongodb.com/manual/tutorial/query-documents/) on the source collection. Documents are loaded from the colleciton if they match with this condition. (string, optional)
- **projection**: A JSON document used for [projection](https://docs.mongodb.com/manual/reference/operator/projection/positional/) on query results. Fields in a document are used only if they match with this condition. (string, optional)
- **sort**: ordering of results (string, optional)
- **stop_on_invalid_record** Stop bulk load transaction if a document includes invalid record (such as unsupported object type) (boolean, optional, default: false)
- **json_column_name**: column name used in outputs (string, optional, default: "json")

## Example

### Exporting all objects

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
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
