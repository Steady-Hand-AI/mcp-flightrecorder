"""Explicit bounded SELECT access under the caller's owned read transaction."""
from collections.abc import Mapping
import math
import re
import sqlite3

from ._inspect_common import InspectionError, display, run_read, validate_int


FUNCTIONS = frozenset("abs avg count sum total min max round coalesce ifnull nullif length lower upper substr substring trim ltrim rtrim replace instr printf format typeof hex quote unicode char like glob json json_valid json_extract json_type json_array_length json_array json_object json_group_array json_group_object -> ->>".split())
ACTIONS = frozenset((sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION,
                     getattr(sqlite3, "SQLITE_RECURSIVE", 33)))
QUERY_VALUE_BYTES = 8_388_608
STORAGE_CODES = frozenset(getattr(sqlite3, name, number) for name, number in (
    ("SQLITE_CORRUPT", 11), ("SQLITE_NOTADB", 26), ("SQLITE_IOERR", 10),
    ("SQLITE_CANTOPEN", 14), ("SQLITE_FULL", 13), ("SQLITE_READONLY", 8),
    ("SQLITE_BUSY", 5), ("SQLITE_LOCKED", 6)))


def _reject(reason, message):
    raise InspectionError("sql_rejected", reason, message)


def _validate_sql(sql):
    if type(sql) is not str:
        raise InspectionError("invalid_request", "invalid_sql", "SQL must be a UTF-8 string.")
    try:
        size = len(sql.encode("utf-8"))
    except UnicodeEncodeError:
        raise InspectionError("invalid_request", "invalid_sql", "SQL must be valid UTF-8 text.") from None
    if size > 65_536:
        raise InspectionError("invalid_request", "sql_input_limit", "SQL exceeds the documented input byte limit.")


def _leading_keyword(sql):
    remaining = sql.lstrip()
    while remaining.startswith(("--", "/*")):
        if remaining.startswith("--"):
            end = remaining.find("\n")
            remaining = "" if end < 0 else remaining[end + 1:].lstrip()
        else:
            end = remaining.find("*/", 2)
            remaining = "" if end < 0 else remaining[end + 2:].lstrip()
    match = re.match(r"[A-Za-z]+", remaining)
    return match.group(0).lower() if match else ""


def _scalar_binding(value):
    if value is None or type(value) in (str, bytes, bool):
        if type(value) is str:
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                return False
        return True
    if type(value) is int:
        return -(2 ** 63) <= value < 2 ** 63
    return type(value) is float and math.isfinite(value)


def _validate_params(params):
    if params is None:
        return
    if isinstance(params, Mapping):
        valid = all(type(key) is str and _scalar_binding(key) and _scalar_binding(value)
                    for key, value in params.items())
    elif type(params) in (list, tuple):
        valid = all(_scalar_binding(value) for value in params)
    else:
        valid = False
    if not valid:
        raise InspectionError("invalid_request", "invalid_params", "Parameters must contain only supported SQLite scalar bindings.")


def _authorize(action, first, second, database, trigger):
    if action not in ACTIONS:
        return sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_FUNCTION and str(second or first).lower() not in FUNCTIONS:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _value(value, ctx, path):
    ctx.check_deadline()
    if value is None or type(value) is int:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is str:
        try:
            return display(value, ctx, path)
        except UnicodeEncodeError:
            pass
    _reject("unsupported_sql_value", "A retained SQL value is not a supported finite JSON scalar; use an explicit SQL conversion.")


def _read_rows(cursor, ctx, limit):
    columns = [display(column[0], ctx, "/columns/" + str(index))
               for index, column in enumerate(cursor.description)]
    rows = []
    for index in range(limit):
        ctx.check_deadline()
        row = cursor.fetchone()
        if row is None:
            return columns, rows, False
        rows.append([_value(value, ctx, "/rows/" + str(index) + "/" + str(column))
                     for column, value in enumerate(row)])
    ctx.check_deadline()
    return columns, rows, cursor.fetchone() is not None


def _translate_sql_error(error, ctx):
    ctx.check_deadline()
    code = getattr(error, "sqlite_errorcode", 0) & 255
    if code == sqlite3.SQLITE_TOOBIG:
        raise InspectionError(
            "limit_exceeded", "sql_value_limit",
            "The SQL value or encoded row exceeds the 8,388,608-byte query limit."
        ) from None
    if code in STORAGE_CODES:
        raise error
    if code == getattr(sqlite3, "SQLITE_INTERRUPT", 9):
        raise InspectionError("query_timeout", "query_interrupted", "SQLite interrupted the bounded query.") from None
    if isinstance(error, sqlite3.ProgrammingError):
        _reject("statement_or_binding", "SQL must be one SELECT or WITH statement with matching bindings.")
    _reject("statement_denied", "The SQL statement, function, syntax or bindings are outside the read-only query contract.")


def _execute(ctx, sql, params, limit):
    if _leading_keyword(sql) not in ("select", "with"):
        _reject("statement_not_select", "Only one SELECT or WITH statement is accepted.")
    ctx.connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, QUERY_VALUE_BYTES)
    cursor = None
    ctx.connection.set_authorizer(_authorize)
    try:
        if params is None:
            bindings = ()
        else:
            bindings = dict(params) if isinstance(params, Mapping) else params
        cursor = ctx.connection.execute(sql, bindings)
        if cursor.description is None:
            _reject("statement_not_select", "The statement must produce a read-only result set.")
        columns, rows, has_more = _read_rows(cursor, ctx, limit)
    except sqlite3.Error as error:
        _translate_sql_error(error, ctx)
    finally:
        # Keep authorization active through all stepping and statement cleanup.
        try:
            if cursor is not None:
                cursor.close()
        finally:
            ctx.connection.set_authorizer(None)
    if has_more:
        ctx.cutoff("item_limit", "/rows", count=None)
    data = {"columns": columns, "rows": rows, "returned_rows": len(rows), "has_more": has_more}

    def shrink():
        if len(rows) <= 1:
            return False
        rows.pop()
        data["returned_rows"] = len(rows)
        data["has_more"] = True
        return True

    return data, shrink


def query(path, sql, params=None, *, limit=200, timeout=10.0,
          max_output_bytes=65_536, no_content=False):
    """Read one authorized SELECT/CTE without converting unsupported SQL values."""
    def validate():
        validate_int(limit, 1, 1000, "limit")
        _validate_sql(sql)
        _validate_params(params)

    return run_read("query", path, {}, lambda ctx: _execute(ctx, sql, params, limit),
                    max_output_bytes=max_output_bytes, no_content=no_content,
                    timeout=timeout, validate=validate, content=True)
