"""Unit tests for connector helpers and the DatabaseConnector.

These tests do NOT require a live PostgreSQL or Snowflake instance.
They validate escaping, quoting, conninfo construction, and the
credential-pass-through fix.
"""

from __future__ import annotations

import pytest

from file_profiler.connectors.base import SourceDescriptor
from file_profiler.connectors.database import (
    DatabaseConnector,
    _escape_libpq_value,
    _escape_sql_string,
    _quote_snowflake_identifier,
    _PG_CONNECT_TIMEOUT,
    _PG_STATEMENT_TIMEOUT_MS,
)


# -----------------------------------------------------------------------
# _escape_libpq_value
# -----------------------------------------------------------------------

class TestEscapeLibpqValue:
    def test_simple_value_unchanged(self):
        assert _escape_libpq_value("localhost") == "localhost"

    def test_empty_string_unchanged(self):
        assert _escape_libpq_value("") == ""

    def test_value_with_space_is_quoted(self):
        assert _escape_libpq_value("my password") == "'my password'"

    def test_value_with_equals_is_quoted(self):
        assert _escape_libpq_value("pass=word") == "'pass=word'"

    def test_value_with_single_quote_escaped(self):
        assert _escape_libpq_value("it's") == "'it\\'s'"

    def test_value_with_backslash_escaped(self):
        assert _escape_libpq_value("back\\slash") == "'back\\\\slash'"

    def test_complex_password(self):
        # password: O'Brien\123 =ok
        result = _escape_libpq_value("O'Brien\\123 =ok")
        assert result == "'O\\'Brien\\\\123 =ok'"

    def test_numeric_value_unchanged(self):
        assert _escape_libpq_value("5432") == "5432"


# -----------------------------------------------------------------------
# _quote_snowflake_identifier
# -----------------------------------------------------------------------

class TestQuoteSnowflakeIdentifier:
    def test_simple_identifier_unchanged(self):
        assert _quote_snowflake_identifier("MY_DATABASE") == "MY_DATABASE"

    def test_identifier_with_dot_unchanged(self):
        assert _quote_snowflake_identifier("DB.SCHEMA") == "DB.SCHEMA"

    def test_identifier_with_dollar_unchanged(self):
        assert _quote_snowflake_identifier("DB$1") == "DB$1"

    def test_identifier_starting_with_digit_is_quoted(self):
        assert _quote_snowflake_identifier("1BAD") == '"1BAD"'

    def test_identifier_with_space_is_quoted(self):
        assert _quote_snowflake_identifier("my db") == '"my db"'

    def test_identifier_with_semicolon_is_quoted(self):
        # Injection attempt: "mydb; DROP TABLE users--"
        result = _quote_snowflake_identifier("mydb; DROP TABLE users--")
        assert result == '"mydb; DROP TABLE users--"'

    def test_identifier_with_double_quotes_escaped(self):
        result = _quote_snowflake_identifier('say"hello')
        assert result == '"say""hello"'

    def test_empty_identifier_is_quoted(self):
        result = _quote_snowflake_identifier("")
        assert result == '""'


# -----------------------------------------------------------------------
# DatabaseConnector._pg_conninfo
# -----------------------------------------------------------------------

class TestPgConninfo:
    def _make_descriptor(self, **overrides) -> SourceDescriptor:
        defaults = dict(
            scheme="postgresql",
            bucket_or_host="localhost:5432",
            path="/mydb",
            raw_uri="postgresql://localhost:5432/mydb",
            database="mydb",
        )
        defaults.update(overrides)
        return SourceDescriptor(**defaults)

    def test_basic_conninfo(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "admin", "password": "secret", "dbname": "mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert "host=localhost" in result
        assert "port=5432" in result
        assert "user=admin" in result
        assert "password=secret" in result
        assert "dbname=mydb" in result
        assert f"connect_timeout={_PG_CONNECT_TIMEOUT}" in result
        assert f"statement_timeout={_PG_STATEMENT_TIMEOUT_MS}" in result

    def test_password_with_spaces_escaped(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "admin", "password": "my secret", "dbname": "mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert "password='my secret'" in result

    def test_password_with_special_chars_escaped(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "admin", "password": "p@ss=w'rd\\1", "dbname": "mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert "password='p@ss=w\\'rd\\\\1'" in result

    def test_connection_string_passthrough(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"connection_string": "postgresql://admin:secret@localhost/mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert result == "postgresql://admin:secret@localhost/mydb"

    def test_empty_password_omitted(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "admin", "password": "", "dbname": "mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert "password" not in result

    def test_options_field_has_statement_timeout(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "admin", "password": "secret", "dbname": "mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert f"options='-c statement_timeout={_PG_STATEMENT_TIMEOUT_MS}'" in result

    def test_credentials_param_used_directly(self):
        """Verify _pg_conninfo uses the passed credentials, not ConnectionManager."""
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "direct_user", "password": "direct_pass", "dbname": "mydb"}
        result = connector._pg_conninfo(desc, creds)

        assert "user=direct_user" in result
        assert "password=direct_pass" in result


# -----------------------------------------------------------------------
# _escape_sql_string
# -----------------------------------------------------------------------

class TestEscapeSqlString:
    def test_no_quotes_unchanged(self):
        assert _escape_sql_string("hello") == "hello"

    def test_single_quote_doubled(self):
        assert _escape_sql_string("it's") == "it''s"

    def test_multiple_quotes_doubled(self):
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_empty_string(self):
        assert _escape_sql_string("") == ""

    def test_libpq_escaped_value_safe_for_sql(self):
        """Simulate: libpq produces password='my\\'pass', SQL must double the quotes."""
        libpq_val = "password='my\\'pass'"
        sql_safe = _escape_sql_string(libpq_val)
        assert sql_safe == "password=''my\\''pass''"


# -----------------------------------------------------------------------
# duckdb_scan_expression — SQL string safety
# -----------------------------------------------------------------------

class TestDuckdbScanExpression:
    def _make_descriptor(self, **overrides) -> SourceDescriptor:
        defaults = dict(
            scheme="postgresql",
            bucket_or_host="localhost:5432",
            path="/mydb",
            raw_uri="postgresql://localhost:5432/mydb",
            database="mydb",
            schema_name="public",
            table_name="users",
        )
        defaults.update(overrides)
        return SourceDescriptor(**defaults)

    def test_basic_scan_expression(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        creds = {"user": "admin", "password": "secret", "dbname": "mydb"}
        # Pre-set conninfo so we don't need ConnectionManager
        expr = connector.duckdb_scan_expression(desc)
        assert expr.startswith("postgres_scan('")
        assert "'public'" in expr
        assert "'users'" in expr

    def test_scan_expression_escapes_quotes_in_conninfo(self):
        """If conninfo has libpq single-quotes, they must be doubled for SQL."""
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor()
        # Build conninfo directly with a password that triggers libpq quoting
        creds = {"user": "admin", "password": "p w", "dbname": "mydb"}
        conninfo = connector._pg_conninfo(desc, creds)
        # conninfo will contain password='p w' (libpq-quoted)
        assert "'" in conninfo

        # Now verify the SQL escaping layer works on this conninfo
        from file_profiler.connectors.database import _escape_sql_string
        sql_safe = _escape_sql_string(conninfo)
        # The libpq quotes should be doubled for SQL embedding
        assert "''p w''" in sql_safe

    def test_object_uri_override(self):
        connector = DatabaseConnector("postgresql")
        desc = self._make_descriptor(table_name="original")
        creds = {"user": "admin", "password": "secret", "dbname": "mydb"}
        expr = connector.duckdb_scan_expression(desc, object_uri="override_table")
        assert "'override_table'" in expr
        assert "original" not in expr
