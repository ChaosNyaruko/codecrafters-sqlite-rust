in Rust, i wanna use https://crates.io/crates/regex crate to implement a simple SQL statement parser. It should support: 
1. get column names, table name, and filter conditions from SELECT statement, e.g:
  `SELECT name, number from apples WHERE name = 'john'`
2. Get the column names by CREATE TABLE statements, e.g. 

  `CREATE TABLE oranges
  (
    id integer primary key autoincrement,
    name text,
    description text
  )`

I want to have two separate functions:  parse_select and parse_create, which give me a structural result.
