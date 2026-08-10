"""Restricted, in-process Python analysis sandbox for agent tools.

This is useful for query-result transformations that are awkward in SQL. It is
not a security boundary against a malicious local user: use a separately
sandboxed process/container for hostile code.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import math
import statistics
from typing import Any, Callable

from text2sql.tools import _is_read_only

MAX_CODE_CHARS = 12_000
MAX_AST_NODES = 2_000
MAX_SQL_CALLS = 10
MAX_ROWS_PER_QUERY = 500
MAX_OUTPUT_CHARS = 12_000

_SAFE_BUILTINS = {
    "abs": abs, "all": all, "any": any, "bool": bool, "dict": dict,
    "enumerate": enumerate, "float": float, "int": int, "len": len,
    "list": list, "max": max, "min": min, "range": range, "reversed": reversed,
    "print": print, "round": round, "set": set, "sorted": sorted, "str": str, "sum": sum,
    "tuple": tuple, "zip": zip,
}
_SAFE_MODULES = {"math": math, "statistics": statistics}
_SAFE_METHODS = {
    "append", "count", "get", "items", "join", "keys", "lower", "replace",
    "sort", "split", "strip", "upper", "values",
}
_ALLOWED_NODES = {
    "Module", "Expr", "Assign", "AugAssign", "Name", "Load", "Store", "Constant",
    "List", "Tuple", "Set", "Dict", "Subscript", "Slice", "Index", "BinOp", "UnaryOp",
    "BoolOp", "Compare", "IfExp", "If", "For", "Break", "Continue", "Pass",
    "ListComp", "SetComp", "DictComp", "GeneratorExp", "comprehension", "Call",
    "Attribute", "JoinedStr", "FormattedValue", "keyword", "Add", "Sub", "Mult",
    "Div", "FloorDiv", "Mod", "Pow", "USub", "UAdd", "Not", "And", "Or",
    "Eq", "NotEq", "Lt", "LtE", "Gt", "GtE", "In", "NotIn", "Is", "IsNot",
}


class _SafetyValidator(ast.NodeVisitor):
    def generic_visit(self, node: ast.AST) -> None:
        if type(node).__name__ not in _ALLOWED_NODES:
            raise ValueError(f"Unsupported Python syntax: {type(node).__name__}")
        super().generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id.startswith("_"):
            raise ValueError("Names beginning with '_' are not allowed.")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr.startswith("_"):
            raise ValueError("Private attribute access is not allowed.")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr not in _SAFE_METHODS:
            if not (isinstance(node.func.value, ast.Name) and node.func.value.id in _SAFE_MODULES):
                raise ValueError(f"Method '{node.func.attr}' is not allowed.")
        self.generic_visit(node)


def _json_safe(value: Any) -> str:
    try:
        encoded = json.dumps(value, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        encoded = str(value)
    return encoded[:MAX_OUTPUT_CHARS] + ("\n[Result truncated]" if len(encoded) > MAX_OUTPUT_CHARS else "")


def run_python(code: str, execute_sql: Callable[[str], list[dict]]) -> str:
    """Run restricted code with a read-only ``sql(query)`` helper."""
    if not isinstance(code, str) or not code.strip():
        return "Empty Python code."
    if len(code) > MAX_CODE_CHARS:
        return f"Code exceeds the {MAX_CODE_CHARS:,}-character limit."
    try:
        tree = ast.parse(code, mode="exec")
        if sum(1 for _ in ast.walk(tree)) > MAX_AST_NODES:
            return f"Code exceeds the {MAX_AST_NODES:,}-AST-node limit."
        _SafetyValidator().visit(tree)
    except (SyntaxError, ValueError) as exc:
        return f"Python blocked: {exc}"

    calls = 0
    def sql(query: str) -> list[dict]:
        nonlocal calls
        if not isinstance(query, str):
            raise TypeError("sql() requires a SQL string")
        if not _is_read_only(query):
            raise ValueError("sql() only permits read-only SQL queries")
        calls += 1
        if calls > MAX_SQL_CALLS:
            raise RuntimeError(f"sql() may be called at most {MAX_SQL_CALLS} times")
        return execute_sql(query)[:MAX_ROWS_PER_QUERY]

    output = io.StringIO()
    namespace = {"__builtins__": _SAFE_BUILTINS, "sql": sql, **_SAFE_MODULES}
    try:
        with contextlib.redirect_stdout(output):
            exec(compile(tree, "<text2sql-sandbox>", "exec"), namespace, namespace)
    except Exception as exc:
        return f"Python error: {type(exc).__name__}: {exc}"

    parts = [output.getvalue().strip()] if output.getvalue().strip() else []
    if "result" in namespace:
        parts.append(_json_safe(namespace["result"]))
    return "\n".join(parts)[:MAX_OUTPUT_CHARS] or "Python completed. Assign `result` or call print(...)."
