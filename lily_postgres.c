/**
library postgres

This provides a very thin wrapper over libpq for Lily.
*/

#include <string.h>

#include "libpq-fe.h"

#include "lily_api_msgbuf.h"
#include "lily_api_value.h"

/** Begin autogen section. **/
typedef struct lily_postgres_Cursor_ {
    LILY_FOREIGN_HEADER
    uint64_t column_count;
    uint64_t row_count;
    uint64_t current_row;
    uint64_t is_closed;
    PGresult *pg_result;
} lily_postgres_Cursor;
#define ARG_Cursor(state, index) \
(lily_postgres_Cursor *)lily_arg_generic(state, index)
#define ID_Cursor(state) lily_cid_at(state, 0)
#define INIT_Cursor(state)\
(lily_postgres_Cursor *) lily_push_foreign(state, ID_Cursor(state), (lily_destroy_func)destroy_Cursor, sizeof(lily_postgres_Cursor))

typedef struct lily_postgres_Conn_ {
    LILY_FOREIGN_HEADER
    uint64_t is_open;
    PGconn *conn;
} lily_postgres_Conn;
#define ARG_Conn(state, index) \
(lily_postgres_Conn *)lily_arg_generic(state, index)
#define ID_Conn(state) lily_cid_at(state, 1)
#define INIT_Conn(state)\
(lily_postgres_Conn *) lily_push_foreign(state, ID_Conn(state), (lily_destroy_func)destroy_Conn, sizeof(lily_postgres_Conn))

const char *lily_postgres_table[] = {
    "\02Cursor\0Conn\0"
    ,"C\03Cursor"
    ,"m\0close\0(Cursor)"
    ,"m\0each_row\0(Cursor,Function(List[String]))"
    ,"m\0row_count\0(Cursor):Integer"
    ,"C\02Conn"
    ,"m\0query\0(Conn,String,String...):Result[String,Cursor]"
    ,"m\0open\0(*String,*String,*String,*String,*String):Result[String,Conn]"
    ,"Z"
};
#define Cursor_OFFSET 1
#define Conn_OFFSET 5
void lily_postgres_Cursor_close(lily_state *);
void lily_postgres_Cursor_each_row(lily_state *);
void lily_postgres_Cursor_row_count(lily_state *);
void lily_postgres_Conn_query(lily_state *);
void lily_postgres_Conn_open(lily_state *);
void *lily_postgres_loader(lily_state *s, int id)
{
    switch (id) {
        case Cursor_OFFSET + 1: return lily_postgres_Cursor_close;
        case Cursor_OFFSET + 2: return lily_postgres_Cursor_each_row;
        case Cursor_OFFSET + 3: return lily_postgres_Cursor_row_count;
        case Conn_OFFSET + 1: return lily_postgres_Conn_query;
        case Conn_OFFSET + 2: return lily_postgres_Conn_open;
        default: return NULL;
    }
}
/** End autogen section. **/

/**
foreign class Cursor {
    layout {
        uint64_t column_count;
        uint64_t row_count;
        uint64_t current_row;
        uint64_t is_closed;
        PGresult *pg_result;
    }
}

The `Cursor` class provides a wrapper over the result of querying the postgres
database. The class provides a very basic set of methods to allow interaction
with the rows as a `List[String]`.
*/

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
define Cursor.close

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
define Cursor.each_row(fn: Function(List[String]))

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
        lily_container_val *lv = lily_push_list(s, num_cols);

        int col;
        for (col = 0;col < num_cols;col++) {
            char *field_text;

            if (PQgetisnull(raw_result, row, col))
                field_text = "(null)";
            else
                field_text = PQgetvalue(raw_result, row, col);

            lily_push_string(s, field_text);
            lily_con_set_from_stack(s, lv, col);
        }

        lily_call(s, 1);
    }
}

/**
define Cursor.row_count: Integer

Returns the number of rows present within `self`.
*/
void lily_postgres_Cursor_row_count(lily_state *s)
{
    lily_postgres_Cursor *boxed_result = ARG_Cursor(s, 0);

    lily_return_integer(s, boxed_result->current_row);
}

/**
foreign class Conn {
    layout {
        uint64_t is_open;
        PGconn *conn;
    }
}

The `Conn` class represents a connection to a postgres server.
*/

void destroy_Conn(lily_postgres_Conn *conn_value)
{
    PQfinish(conn_value->conn);
}

/**
define Conn.query(format: String, values: String...):Result[String, Cursor]

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

    int num_values = lily_con_size(vararg_lv);

    while (1) {
        char ch = fmt[fmt_index];

        if (ch == '?') {
            if (arg_pos == num_values) {
                lily_container_val *variant = lily_push_failure(s);
                lily_push_string(s, "Not enough arguments for format.\n");
                lily_con_set_from_stack(s, variant, 0);
                lily_return_top(s);
                return;
            }

            lily_mb_add_slice(msgbuf, fmt, text_start, text_stop);
            text_start = fmt_index + 1;
            text_stop = text_start;

            lily_value *v = lily_con_get(vararg_lv, arg_pos);
            lily_mb_add(msgbuf, lily_as_string_raw(v));
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
        lily_container_val *variant = lily_push_failure(s);
        lily_push_string(s, PQerrorMessage(conn_value->conn));
        lily_con_set_from_stack(s, variant, 0);
        lily_return_top(s);
        return;
    }

    lily_container_val *variant = lily_push_success(s);

    lily_postgres_Cursor *res = INIT_Cursor(s);
    res->current_row = 0;
    res->is_closed = 0;
    res->pg_result = raw_result;
    res->row_count = PQntuples(raw_result);
    res->column_count = PQnfields(raw_result);

    lily_con_set_from_stack(s, variant, 0);
    lily_return_top(s);
}

/**
static define Conn.open(
    host: *String="",
    port: *String="",
    dbname: *String="",
    name: *String="",
    pass: *String=""):Result[String, Conn]

Attempt to connect to the postgres server, using the values provided.

If able to connect, the result is a `Success` containing the `Conn`.

Otherwise, the result is a `Failure` containing an error message.
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
            variant = lily_push_success(s);

            new_val = INIT_Conn(s);
            new_val->is_open = 1;
            new_val->conn = conn;

            lily_con_set_from_stack(s, variant, 0);
            break;
        default:
            variant = lily_push_failure(s);
            lily_push_string(s, PQerrorMessage(conn));
            lily_con_set_from_stack(s, variant, 0);
            break;
    }

    lily_return_top(s);
}
