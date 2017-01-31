/* This creates a shared library that allows Lily to access postgres, using
   libpq. This can be used by any Lily runner. */
#include <string.h>

#include "libpq-fe.h"

#include "lily_api_msgbuf.h"
#include "lily_api_value.h"

#include "extras_postgres.h"

/**
package postgres

This package provides Lily with a simple wrapper over libpq. You'll need libpq's
development headers, but there are no other requirements. You can install this
using Lily's `garden` via:

`garden install github Fascinatedbox/postgres`.
*/

/**
class Cursor

The `Cursor` class provides a wrapper over the result of querying the postgres
database. The class provides a very basic set of methods to allow interaction
with the rows as a `List[String]`.
*/
typedef struct {
    LILY_FOREIGN_HEADER
    uint64_t column_count;
    uint64_t row_count;
    uint64_t current_row;
    uint64_t is_closed;
    PGresult *pg_result;
} lily_postgres_Cursor;

void close_result(lily_postgres_Cursor *result)
{
    if (result->is_closed == 0)
        PQclear(result->pg_result);

    result->is_closed = 1;
}

void destroy_Cursor(lily_postgres_Cursor *r)
{
    close_result(r);
}

/**
method Cursor.close(self: Cursor)

Close a `Cursor` and free all data associated with it. If this is not done
manually, then it is done automatically when the `Cursor` is destroyed through
either the gc or refcounting.
*/
void lily_postgres_Cursor_close(lily_state *s)
{
    lily_postgres_Cursor *to_close = ARG_Cursor(s, 0);

    close_result(to_close);
    to_close->row_count = 0;
}

/**
method Cursor.each_row(self: Cursor, fn: Function(List[String]))

This loops through each row in `self`, calling `fn` for each row that is found.
If `self` has no rows, or has been closed, then this does nothing.
*/
void lily_postgres_Cursor_each_row(lily_state *s)
{
    lily_postgres_Cursor *boxed_result = ARG_Cursor(s, 0);

    PGresult *raw_result = boxed_result->pg_result;
    if (raw_result == NULL || boxed_result->row_count == 0)
        return;

    lily_call_prepare(s, lily_arg_function(s, 1));

    int row;
    for (row = 0;row < boxed_result->row_count;row++) {
        int num_cols = boxed_result->column_count;
        lily_container_val *lv = lily_new_list(num_cols);

        int col;
        for (col = 0;col < num_cols;col++) {
            char *field_text;

            if (PQgetisnull(raw_result, row, col))
                field_text = "(null)";
            else
                field_text = PQgetvalue(raw_result, row, col);

            lily_nth_set(lv, col,
                    lily_box_string(s, lily_new_string(field_text)));
        }

        lily_push_list(s, lv);
        lily_call_exec_prepared(s, 1);
    }
}

/**
method Cursor.row_count(self: Cursor): Integer

Returns the number of rows present within `self`.
*/
void lily_postgres_Cursor_row_count(lily_state *s)
{
    lily_postgres_Cursor *boxed_result = ARG_Cursor(s, 0);

    lily_return_integer(s, boxed_result->current_row);
}

/**
class Conn

The `Conn` class represents a connection to a postgres server.
*/
typedef struct lily_postgres_Conn_ {
    LILY_FOREIGN_HEADER
    uint64_t is_open;
    PGconn *conn;
} lily_postgres_Conn;

void destroy_Conn(lily_postgres_Conn *conn_value)
{
    PQfinish(conn_value->conn);
}

/**
method Conn.query(self: Conn, format: String, values: String...):Result[String, Cursor]

Perform a query using `format`. Any `"?"` value found within `format` will be
replaced with an entry from `values`.

On success, the result is a `Success` containing a `Cursor`.

On failure, the result is a `Failure` containing a `String` describing the error.
*/
void lily_postgres_Conn_query(lily_state *s)
{
    lily_postgres_Conn *conn_value = ARG_Conn(s, 0);
    char *fmt = lily_arg_string_raw(s, 1);
    lily_container_val *vararg_lv = lily_arg_container(s, 2);

    int arg_pos = 0, fmt_index = 0, text_start = 0, text_stop = 0;
    lily_msgbuf *msgbuf = lily_get_clean_msgbuf(s);

    int num_values = lily_container_num_values(vararg_lv);

    while (1) {
        char ch = fmt[fmt_index];

        if (ch == '?') {
            if (arg_pos == num_values) {
                lily_container_val *variant = lily_new_failure();
                lily_string_val *sv = lily_new_string(
                        "Not enough arguments for format.\n");
                lily_nth_set(variant, 0, lily_box_string(s, sv));
                lily_return_variant(s, variant);
                return;
            }

            lily_mb_add_slice(msgbuf, fmt, text_start, text_stop);
            text_start = fmt_index + 1;
            text_stop = text_start;

            lily_value *v = lily_nth_get(vararg_lv, arg_pos);
            lily_mb_add(msgbuf, lily_value_string_raw(v));
            arg_pos++;
        }
        else {
            if (ch == '\0')
                break;

            text_stop++;
        }

        fmt_index++;
    }

    const char *query_string;

    /* If there are no ?'s in the format string, then it can be used as-is. */
    if (text_start == 0)
        query_string = fmt;
    else {
        lily_mb_add_slice(msgbuf, fmt, text_start, text_stop);
        query_string = lily_mb_get(msgbuf);
    }

    PGresult *raw_result = PQexec(conn_value->conn, query_string);

    ExecStatusType status = PQresultStatus(raw_result);
    if (status == PGRES_BAD_RESPONSE ||
        status == PGRES_NONFATAL_ERROR ||
        status == PGRES_FATAL_ERROR) {
        lily_container_val *variant = lily_new_failure();
        lily_string_val *sv = lily_new_string(
                PQerrorMessage(conn_value->conn));
        lily_nth_set(variant, 0, lily_box_string(s, sv));
        lily_return_variant(s, variant);
        return;
    }

    lily_postgres_Cursor *res = INIT_Cursor(s);
    res->current_row = 0;
    res->is_closed = 0;
    res->pg_result = raw_result;
    res->row_count = PQntuples(raw_result);
    res->column_count = PQnfields(raw_result);

    lily_container_val *variant = lily_new_success();
    lily_nth_set(variant, 0, lily_box_foreign(s, (lily_foreign_val *)res));
    lily_return_variant(s, variant);
}

/**
method Conn.open(host: *String="", port: *String="", dbname: *String="", name: *String="", pass: *String=""):Either[String, Conn]

Attempt to connect to the postgres server, using the values provided.

On success, the result is a `Right` containing a newly-made `Conn`.

On failure, the result is a `Left` containing an error message.
*/
void lily_postgres_Conn_open(lily_state *s)
{
    const char *host = NULL;
    const char *port = NULL;
    const char *dbname = NULL;
    const char *name = NULL;
    const char *pass = NULL;

    switch (lily_arg_count(s)) {
        case 5:
            pass = lily_arg_string_raw(s, 4);
        case 4:
            name = lily_arg_string_raw(s, 3);
        case 3:
            dbname = lily_arg_string_raw(s, 2);
        case 2:
            port = lily_arg_string_raw(s, 1);
        case 1:
            host = lily_arg_string_raw(s, 0);
    }

    PGconn *conn = PQsetdbLogin(host, port, NULL, NULL, dbname, name, pass);
    lily_postgres_Conn *new_val;
    lily_container_val *variant;

    switch (PQstatus(conn)) {
        case CONNECTION_OK:
            new_val = INIT_Conn(s);
            new_val->is_open = 1;
            new_val->conn = conn;

            variant = lily_new_success();
            lily_nth_set(variant, 0,
                    lily_box_foreign(s, (lily_foreign_val *)new_val));
            lily_return_variant(s, variant);
            break;
        default:
            variant = lily_new_failure();
            lily_string_val *sv = lily_new_string(PQerrorMessage(conn));
            lily_nth_set(variant, 0, lily_box_string(s, sv));
            lily_return_variant(s, variant);
            break;
    }
}

#include "dyna_postgres.h"
