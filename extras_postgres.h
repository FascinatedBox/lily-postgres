/* Contents autogenerated by dyna_tools.py */
#include "lily_api_dyna.h"

#define ARG_Conn(state, index) \
(lily_postgres_Conn *)lily_arg_generic(s, index)
#define ARG_Cursor(state, index) \
(lily_postgres_Cursor *)lily_arg_generic(s, index)

#define ID_Conn(state) lily_cid_at(state, 1)
#define ID_Cursor(state) lily_cid_at(state, 0)

#define INIT_Conn(state) \
(lily_postgres_Conn *)lily_new_foreign(state, ID_Conn(s), (lily_destroy_func)destroy_Conn, sizeof(lily_postgres_Conn))

#define INIT_Cursor(state) \
(lily_postgres_Cursor *)lily_new_foreign(state, ID_Cursor(s), (lily_destroy_func)destroy_Cursor, sizeof(lily_postgres_Cursor *))

