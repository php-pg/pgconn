# pgconn

Package pgconn is a low-level PostgreSQL database driver. It operates at nearly the same level as the C library libpq.
Applications should handle normal queries with a higher level library and only use pgconn directly when required for
low-level access to PostgreSQL functionality.

Inspired by Golang package [jackc/pgconn](https://github.com/jackc/pgconn)