# MongoDB input plugin for Embulk

MongoDB input plugin for Embulk loads records from MongoDB.

**CAUTION:** this plugin does not support array and object fields, 
because embulk does not supported these types yet.
But these types will be supported, so when it supported I add support these types.
For more detail see following issues.

- https://github.com/embulk/embulk/issues/120
- https://github.com/embulk/embulk/issues/121

## Overview

This plugin only works with embulk >= 0.7.4.

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
  uri: mongodb://myuser@mypassword:localhost:27017/my_database
  collection: "my_collection"
  fields:
    - { name: id, type: string }
    - { name: field1, type: long }
    - { name: field2, type: timestamp }
```

### Filter object by query and sort


```yaml
in:
  type: mongodb
  uri: mongodb://myuser@mypassword:localhost:27017/my_database
  collection: "my_collection"
  fields:
    - { name: id, type: string }
    - { name: field1, type: long }
    - { name: field2, type: timestamp }
  query: '{ field1: { $gte: 3 } }'
  sort: '{ field1: 1 }'
```

## Build

```
$ ./gradlew gem
```
