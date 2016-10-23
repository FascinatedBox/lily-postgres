# postgres

This package provides Lily with a simple wrapper over libpq. You'll need libpq's
development headers, but there are no other requirements. You can install this
using Lily's `garden` via:

`garden install github Fascinatedbox/postgres`.

## class Result

The `Result` class provides a wrapper over the result of querying the postgres
database. The class provides a very basic set of methods to allow interaction
with the rows as a `List[String]`.

### method Result.close`(self: Result)`

Close a `Result` and free all data associated with it. If this is not done
manually, then it is done automatically when the `Result` is destroyed through
either the gc or refcounting.

### method Result.each_row`(self: Result, fn: Function(List[String]))`

This loops through each row in `self`, calling `fn` for each row that is found.
If `self` has no rows, or has been closed, then this does nothing.

### method Result.row_count`(self: Result): Integer`

Returns the number of rows present within `self`.

## class Conn

The `Conn` class represents a connection to a postgres server.

### method Conn.query`(self: Conn, format: String, values: String...):Either[String, Result]`

Perform a query using `format`. Any `"?"` value found within `format` will be
replaced with an entry from `values`.

On success, the result is a `Right` containing a `Result`.

On failure, the result is a `Left` containing a `String` describing the error.

### method Conn.open`(host: *String="", port: *String="", dbname: *String="", name: *String="", pass: *String=""):Either[String, Conn]`

Attempt to connect to the postgres server, using the values provided.

On success, the result is a `Right` containing a newly-made `Conn`.

On failure, the result is a `Left` containing an error message.
