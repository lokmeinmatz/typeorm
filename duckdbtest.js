const duckdb = require("duckdb")
const db = new duckdb.Database(":memory:")
const _all = db.all
db.all = (...args) => {
  console.log(args)
  return _all.call(db, ...args)
}
var queryAll = (sql, ...params) => new Promise((res, rej) => db.all(sql, ...(params ?? []), (err, result) => { if (err) rej(err); res(result);}))
await queryAll("CREATE TABLE \"post\" (\"id\" integer PRIMARY KEY NOT NULL, \"title\" varchar NOT NULL, \"text\" varchar NOT NULL, \"index\" integer NOT NULL)")

await queryAll("INSERT INTO \"post\"(\"id\", \"title\", \"text\", \"index\") VALUES (DEFAULT, ?, ?, DEFAULT)", "my-title", "my-text")