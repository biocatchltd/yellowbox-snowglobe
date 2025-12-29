import re
from dataclasses import dataclass
from typing import Iterable, Iterator, Pattern, Union

"""
This is a miniature transpiler that converts a snowflake-dialect query to a postgresql query.
Note that anything that can handled outside of this (like making a function called "year" that gets a date's year)
Is handled in the "schema_init" script instead.
"""


@dataclass
class TextLiteral:
    value: str


def split_literals(query: str) -> Iterator[Union[str, TextLiteral]]:
    # splits test literals out of a query
    while query:
        next_str_ind = query.find("'")
        if next_str_ind == -1:
            yield query
            return
        search_start = next_str_ind + 1
        while True:
            terminator_ind = query.find("'", search_start)
            if terminator_ind == -1:
                # unterminated string literal, we treat everything after the starter as a literal and let the caller
                # deal with it
                terminator_ind = len(query)
            if len(query) <= (terminator_ind + 1) or query[terminator_ind + 1] != "'":
                break
            search_start = terminator_ind + 2
        yield query[:next_str_ind]
        yield TextLiteral(query[next_str_ind : terminator_ind + 1])
        query = query[terminator_ind + 1 :]


def split_sql_to_statements(query: str) -> Iterator[str]:
    # splits a compound query to multiple statements, splitting on non-literal ";"
    buffer = []
    for part in split_literals(query):
        if isinstance(part, TextLiteral):
            buffer.append(part.value)
            continue
        while True:
            sep_index = part.find(";")
            if sep_index == -1:
                buffer.append(part)
                break
            buffer.append(part[:sep_index])
            yield "".join(buffer)
            buffer.clear()
            part = part[sep_index + 1 :]
    last_bit = "".join(buffer)
    if last_bit:
        yield last_bit


def find_matching_paren(text: str, start_pos: int) -> int:
    """
    Find the position of the closing parenthesis ')' corresponding to the opening parenthesis '(' at start_pos.
    Handle nested parentheses by counting open/closed parentheses.
    """
    if start_pos >= len(text) or text[start_pos] != "(":
        return -1
    depth = 1
    pos = start_pos + 1
    while pos < len(text) and depth > 0:
        if text[pos] == "(":
            depth += 1
        elif text[pos] == ")":
            depth -= 1
        pos += 1
    return pos - 1 if depth == 0 else -1


def replace_array_construct(text: str) -> str:
    """
    Replaces all occurrences of ARRAY_CONSTRUCT(...) with Array[...] while correctly handling nested parentheses.
    """
    result = []
    i = 0
    while i < len(text):
        # Search for "ARRAY_CONSTRUCT(" (case-insensitive)
        array_match = re.search(r"(?i)\bARRAY_CONSTRUCT\(", text[i:])
        if not array_match:
            result.append(text[i:])
            break

        array_start = i + array_match.start()
        paren_start = i + array_match.end() - 1  # Position of the '('

        # Find the corresponding closing parenthesis
        paren_end = find_matching_paren(text, paren_start)
        if paren_end == -1:
            # Parenthesis not closed, leave as is
            result.append(text[i:])
            break

        # Add text before ARRAY_CONSTRUCT(
        result.append(text[i:array_start])
        # Add Array[ with the content between parentheses
        content = text[paren_start + 1 : paren_end]
        result.append(f"Array[{content}]")
        i = paren_end + 1

    return "".join(result)


"""
A Rule is replacement rule that converts a snowflake-dialect query to a postgresql query.
for example there's a rule that will turn "a..b" into "a.public.b"

It's very important that rules not self-generate, as that will cause an infinite loop.
I.E don't make a rule that replaces "a b" with "a b c"
"""


@dataclass
class Rule:
    pattern: Pattern[str]
    replacement: str


OBJ_PATTERN = r"[a-z][a-z0-9._]*"
NAME_PATTERN = r"[a-z][a-z0-9_]*"

# note that all commands starting with ! are special non-postgres commands for the session to handle specially

# these are special rules that are run before the split_literals, as such they should be used sparingly (you almost
# always want to add a "^" to the beginning of the pattern)
PRE_SPLIT_RULES = [
    # retrieved stored asynchronous result
    Rule(re.compile(r"(?i)^select\s+\*\s+from\s+table\(result_scan\('([a-f0-9-]+)'\)\)"), r"!retrieve \1"),
]

RULES = [
    # commit/rollback
    Rule(re.compile(r"(?i)^(commit|rollback)"), r"!\1"),
    # use database
    Rule(re.compile(r"(?i)use(\s+database)?\s+(" + NAME_PATTERN + r")"), r"!switch_db \2"),
    # use schema
    Rule(re.compile(r"(?i)use\s+schema\s+(" + NAME_PATTERN + r")"), r"SET search_path TO \1;!set_schema \1"),
    Rule(
        re.compile(r"(?i)use(\s+schema)?\s+(" + NAME_PATTERN + r")\.(" + NAME_PATTERN + r")$"),
        r"USE DATABASE \2;use schema \3",
    ),
    # flatten(?) as ?
    Rule(
        re.compile(r"(?ix)\b" r"flatten\(" r"(" + OBJ_PATTERN + ")" r"\)\s+as\s+" r"(" + NAME_PATTERN + r")\b"),
        replacement=r"unnest(\1) as \2(value)",
    ),
    # db..table
    Rule(
        re.compile(r"(?ix)\b" r"(" + NAME_PATTERN + r")\.\.(" + NAME_PATTERN + ")" + r"\b"), replacement=r"\1.public.\2"
    ),
    # json query string
    Rule(
        re.compile(r"(?ix)\b" r"(" + NAME_PATTERN + r"):(" + NAME_PATTERN + ")" + "::string" + r"\b"),
        replacement=r"\1->>'\2'",
    ),
    # json query int
    Rule(
        re.compile(r"(?ix)\b" r"(" + NAME_PATTERN + r"):(" + NAME_PATTERN + ")" + "::number" + r"\b"),
        replacement=r"cast(\1->>'\2' as integer)",
    ),
    # show schemas
    Rule(
        re.compile(r"(?i)show\s+schemas"),
        "select null as created_on, schema_name as name, null as is_default, null as is_current, "
        "null as database_name, null as owner, null as comment, null as options, null as retention_time "
        "FROM information_schema.schemata",
    ),
    # show tables
    Rule(
        re.compile(r"(?i)show\s+tables"),
        "select null as created_on, table_name as name, table_catalog as database_name, table_schema as schema_name,"
        " 'TABLE' as kind, NULL as comment, NULL as cluster_by, NULL as rows, NULL as bytes, NULL as owner,"
        " NULL as retention_time, NULL as change_tracking, NULL as search_optimization,"
        " NULL as search_optimization_progress, NULL as search_optimization_bytes, NULL as is_external"
        " FROM information_schema.tables WHERE table_type = 'BASE TABLE'",
    ),
    # describe table
    Rule(
        re.compile(r"(?i)(describe|desc)\s+table\s+(" + NAME_PATTERN + r")"),
        "SELECT column_name as name, data_type as type, 'COLUMN' as kind, is_nullable as \"null?\","
        " column_default as default, NULL as primary_key, NULL as unique_key, NULL as check,"
        ' NULL as expression, NULL as comment, NULL as "policy name" FROM information_schema.columns'
        r" WHERE table_name = '\2'",
    ),
    # Ignore sample in queries
    Rule(re.compile(r"(?i)\bsample\s+\(([0-9\.]+)\s+rows\)"), replacement=r"order by random() limit \1"),
]


def repl_part(part: Union[str, TextLiteral], rules: Iterable[Rule]) -> str:
    if isinstance(part, TextLiteral):
        return part.value
    # Replace ARRAY_CONSTRUCT() with Array[] before applying the other rules
    part = replace_array_construct(part)
    ret_parts = []
    while part:
        best_match = None
        best_match_key = (float("inf"), 0)  # matches are ranked by position (shorter is better),
        # then by length (longer is better, stored as negative)
        for rule in rules:
            match = rule.pattern.search(part)
            if match:
                match_key = (match.start(), -len(match.group()))
                if match_key < best_match_key:
                    best_match = (rule, match)
                    best_match_key = match_key
        if best_match:
            rule, match = best_match
            ret_parts.append(part[: match.start()])
            part = match.expand(rule.replacement) + part[match.end() :]
        else:
            ret_parts.append(part)
            break
    return "".join(ret_parts)


def snow_to_post(query: str) -> str:
    query = repl_part(query, PRE_SPLIT_RULES)
    return "".join(repl_part(part, RULES) for part in split_literals(query))
