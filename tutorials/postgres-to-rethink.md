# Using Transporter to Export Data from Postgres -> RethinkDB

This tutorial will transfer data from a Postgres database to a  RethinkDB.

1. Setup a working directory for this transporter example

2. Download the `transporter` binary into the directory from https://github.com/Winslett/transporter/releases/tag/1.0-postgres

3. Configure local Postgres

  * set `wal_level='logical'`
  * set `max_replication_slots=1`
  * Run the following to initialize your source db:
  ```sql
  CREATE DATABASE my_source_db;
  \connect my_source_db
  CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255), created_at TIMESTAMP);
  SELECT * FROM pg_create_logical_replication_slot('rethink_transporter', 'test_decoding');

  INSERT INTO users (name, created_at) VALUES ('Chris', now()), ('Kyle', now()), ('Michele', now());
  ```

4. Start local Rethink

  * go to http://localhost:8080
  * create a new database called 'myDestDB'
  * create a table for `users` with Primary Index for `_id`

5. Create a file called `config.yaml`, which will hold the data adapter
   configurations

```yaml
nodes:
  postgres:
    type: postgres
    uri: "host=localhost sslmode=disable dbname=my_source_db"
  rethink:
    type: rethinkdb
    uri: rethink://localhost:28015/
```

6. Create a file called `application.js` which will define the data movement:

```js
pipeline = Source({
  name: "postgres",
  namespace: "my_source_db.public..*",
  tail: true,
  replication_slot: "rethink_transporter"
})

pipeline.save({
  name: "rethink",
  namespace: "myDestDB..*"
})
```

7. Go to Rethink and run query for:

```
r.db("myDestDB").table("users")
```

8. Go to Postgres, and update users:

```sql
UPDATE users SET name = 'Jason' WHERE id = 1;
```

9. Go back to Rethink, and rerun your query.  The name should have
   updated.  You'll see duplicates.  The problem is mis-matching primary
   keys.  This is where transformations can help.

10.  Create a `transform.js` file like this:

```js
module.exports = function(doc) {
  doc.data._id = doc.data["id"]
  delete doc.data["id"]
  return doc
}
```

11. Add the following lines to `application.js`:

```js
pipeline.transform({
  namespace: "public..*",
  filename: "transform.js"
}).save({
  name: "rethink",
  namespace: "myDestDB..*"
})
```

12. Drop the RethinkDB table and re-create.  Then, rerun transporter.

13. Go to Postgres, and re-update user:

```sql
UPDATE users SET name = 'Chris' WHERE id = 1;
```
