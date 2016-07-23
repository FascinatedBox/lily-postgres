# postgres

### Version: 0.1

This package wraps over libpq to provide postgres access to Lily.

You can install this package via

`garden install github FascinatedBox/lily`

The 'packages' directory created by postgres must be in the same directory as
the first script that Lily runs for Lily to be able to find it. From there, you
can add `use postgres` in your first script file, and the postgres namespace
will become available.
