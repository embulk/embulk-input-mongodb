# MongoDB input plugin for Embulk

[![Build Status](https://travis-ci.org/hakobera/embulk-input-mongodb.svg)](https://travis-ci.org/hakobera/embulk-input-mongodb)

MongoDB input plugin for Embulk loads records from MongoDB.
This plugin loads documents as single-column records (column name is "record"). You can use filter plugins such as [embulk-filter-expand_json](https://github.com/civitaspo/embulk-filter-expand_json) or [embulk-filter-add_time](https://github.com/treasure-data/embulk-filter-add_time) to convert the json column to typed columns. [Rename filter](http://www.embulk.org/docs/built-in.html#rename-filter-plugin) is also useful to rename the typed columns.

## Overview

This plugin only works with embulk >= 0.8.8.

* **Plugin type**: input
* **Guess supported**: no

## Configuration

- Connection parameters
  One of them is required.
  
  - use MongoDB connection string URI
    - **uri**: [MongoDB connection string URI](http://docs.mongodb.org/manual/reference/connection-string/) (e.g. 'mongodb://localhost:27017/mydb') (string, required)
  - use separated URI parameters
    - **hosts**: list of hosts. `hosts` are pairs of host(string, required) and port(integer, optional, default: 27017)
    - **username**: (string, optional)
    - **password**:  (string, optional)
    - **database**:  (string, required)
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
- **batch_size**: Limits the number of objects returned in one [batch](http://api.mongodb.com/java/current/com/mongodb/DBCursor.html#batchSize-int-) (integer, optional, default: 10000)
- **incremental_field** List of field name (list, optional, can't use with sort option)
- **last_record** Last loaded record for incremental load (hash, optional)
- **stop_on_invalid_record** Stop bulk load transaction if a document includes invalid record (such as unsupported object type) (boolean, optional, default: false)
- **json_column_name**: column name used in outputs (string, optional, default: "json")

## Example

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
  username: myuser
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

```
$ ./gradlew test  # -t to watch change of files and rebuild continuously
```

To run unit tests, we need to configure the following environment variables.

When environment variables are not set, skip almost test cases.

```
MONGO_URI
MONGO_COLLECTION
```

If you're using Mac OS X El Capitan and GUI Applications(IDE), like as follows.
```xml
$ vi ~/Library/LaunchAgents/environment.plist
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>my.startup</string>
  <key>ProgramArguments</key>
  <array>
    <string>sh</string>
    <string>-c</string>
    <string>
      launchctl setenv MONGO_URI mongodb://myuser:mypassword@localhost:27017/my_database
      launchctl setenv MONGO_COLLECTION my_collection
    </string>
  </array>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>

$ launchctl load ~/Library/LaunchAgents/environment.plist
$ launchctl getenv MONGO_URI //try to get value.

Then start your applications.
```
