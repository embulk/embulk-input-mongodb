# MongoDB input plugin for Embulk

[![Build Status](https://travis-ci.org/hakobera/embulk-input-mongodb.svg)](https://travis-ci.org/hakobera/embulk-input-mongodb)

MongoDB input plugin for Embulk loads records from MongoDB.

## Overview

This plugin only works with embulk >= 0.8.8.

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **uri**: [MongoDB connection string URI](http://docs.mongodb.org/manual/reference/connection-string/) (e.g. 'mongodb://localhost:27017/mydb') (string, required)
- **collection**: source collection name (string, required)
- **fields**: hash records that has the following two fields (array, required)
  - name: Name of the column
  - type: Column types as follows
    - boolean
    - long
    - double
    - string
    - timestamp
- **query**: provides a JSON document as a query that optionally limits the documents returned (string, optional)
- **sort**: specifies an ordering for exported results (string, optional)

## Example

### Export all objects

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
  collection: "my_collection"
  fields:
    - { name: id, type: string }
    - { name: field1, type: long }
    - { name: field2, type: timestamp }
    - { name: field3, type: json }
```

### Filter object by query and sort

```yaml
in:
  type: mongodb
  uri: mongodb://myuser:mypassword@localhost:27017/my_database
  collection: "my_collection"
  fields:
    - { name: id, type: string }
    - { name: field1, type: long }
    - { name: field2, type: timestamp }
    - { name: field3, type: json }
  query: '{ field1: { $gte: 3 } }'
  sort: '{ field1: 1 }'
```

## Build

```
$ ./gradlew gem
```
