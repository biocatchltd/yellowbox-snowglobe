import re
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Iterator, Match, Pattern, Union

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
    replacement: Union[str, Callable[[Match[str]], str]]


OBJ_PATTERN = r"[a-z][a-z0-9._]*"
NAME_PATTERN = r"[a-z][a-z0-9_]*"

_QUALIFY_ROW_NUMBER_RE = re.compile(
    r"""
    \bselect\s+\*\s+from\s+(?P<source>""" + OBJ_PATTERN + r""")\s+where\s+
    (?P<where>.*?)\s+qualify\s+row_number\(\)\s+over\s*\((?P<window>.*?)\)\s*=\s*1
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)
_ALIAS_IN_WHERE_RE = re.compile(
    r"""
    \bselect\s+(?P<select>.*?\s+as\s+""" + NAME_PATTERN + r""".*?)
    (?P<rest>\s+from\s+.*?\bwhere\b)
    (?P<where>.*?)
    (?P<group>\bgroup\s+by\b.*?)
    (?=\bunion\b|\)|$)
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)
_SELECT_ALIAS_RE = re.compile(r"(?is)^(?P<expr>.*?)\s+as\s+(?P<alias>" + NAME_PATTERN + r")\s*$")
_JSON_COALESCE_STRING_RE = re.compile(
    r"(?is)coalesce\(\s*"
    r"(?P<obj1>" + OBJ_PATTERN + r"):(?P<field1>" + NAME_PATTERN + r")\s*,\s*"
    r"(?P<obj2>" + OBJ_PATTERN + r"):(?P<field2>" + NAME_PATTERN + r")\s*"
    r"\)::string"
)


def replace_qualify_row_number(match: Match[str]) -> str:
    """
    Replaces Snowflake's QUALIFY ROW_NUMBER() = 1 with a subquery filter.
    """
    source = match.group("source")
    where = match.group("where").strip()
    window = match.group("window").strip()
    return (
        "select * from ("
        f"select *, row_number() over ({window}) as __snowglobe_qualify_row_number "
        f"from {source} where {where}"
        ") __snowglobe_qualify where __snowglobe_qualify_row_number = 1"
    )


def split_top_level_commas(text: str) -> Iterator[str]:
    start = 0
    depth = 0
    pos = 0
    in_literal = False
    while pos < len(text):
        char = text[pos]
        if in_literal:
            if char == "'" and pos + 1 < len(text) and text[pos + 1] == "'":
                pos += 2
                continue
            if char == "'":
                in_literal = False
        elif char == "'":
            in_literal = True
        elif char == "(":
            depth += 1
        elif char == ")" and depth:
            depth -= 1
        elif char == "," and depth == 0:
            yield text[start:pos]
            start = pos + 1
        pos += 1
    yield text[start:]


def select_aliases(select_list: str) -> Dict[str, str]:
    aliases = {}
    for item in split_top_level_commas(select_list):
        alias_match = _SELECT_ALIAS_RE.match(item.strip())
        if alias_match:
            aliases[alias_match.group("alias").lower()] = alias_match.group("expr").strip()
    return aliases


def replace_aliases_in_where(where: str, aliases: Dict[str, str]) -> str:
    alias_pattern = re.compile(
        r"(?i)(?<![.\w])(" + "|".join(re.escape(alias) for alias in sorted(aliases, key=len, reverse=True)) + r")(?!\w)"
    )

    def repl(match: Match[str]) -> str:
        return aliases[match.group().lower()]

    return "".join(
        part.value if isinstance(part, TextLiteral) else alias_pattern.sub(repl, part) for part in split_literals(where)
    )


def replace_aliases_in_where_clause(match: Match[str]) -> str:
    """
    Replaces Snowflake's use of SELECT aliases in WHERE with the aliased expressions.
    """
    select_list = match.group("select")
    aliases = select_aliases(select_list)
    if not aliases:
        return match.group()
    where = replace_aliases_in_where(match.group("where"), aliases)
    return f"select {select_list}{match.group('rest')}{where}{match.group('group')}"


def replace_json_coalesce_string(match: Match[str]) -> str:
    """
    Replaces coalesce(json:field, json:other)::string with text JSON extraction.
    """
    return (
        f"coalesce({match.group('obj1')}->>'{match.group('field1')}', "
        f"{match.group('obj2')}->>'{match.group('field2')}')"
    )


# note that all commands starting with ! are special non-postgres commands for the session to handle specially

# these are special rules that are run before the split_literals, as such they should be used sparingly (you almost
# always want to add a "^" to the beginning of the pattern)
PRE_SPLIT_RULES = [
    # retrieved stored asynchronous result
    Rule(re.compile(r"(?i)^select\s+\*\s+from\s+table\(result_scan\('([a-f0-9-]+)'\)\)"), r"!retrieve \1"),
    Rule(
        re.compile(r"(?i)\bilike\s+any\s*\(\s*(?!array\[)([^)]+)\)"),
        replacement=r"ilike any (array[\1])",
    ),
    Rule(_JSON_COALESCE_STRING_RE, replace_json_coalesce_string),
    Rule(_QUALIFY_ROW_NUMBER_RE, replace_qualify_row_number),
    Rule(_ALIAS_IN_WHERE_RE, replace_aliases_in_where_clause),
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
        re.compile(r"(?ix)\b" r"(" + OBJ_PATTERN + r"):(" + NAME_PATTERN + ")" + "::string" + r"\b"),
        replacement=r"\1->>'\2'",
    ),
    # json query int
    Rule(
        re.compile(r"(?ix)\b" r"(" + OBJ_PATTERN + r"):(" + NAME_PATTERN + ")" + "::(number|int)" + r"\b"),
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
    # current timestamp
    Rule(re.compile(r"(?i)\bcurrent_timestamp\(\)"), replacement=r"current_timestamp"),
    # listagg
    Rule(
        re.compile(r"(?i)\blistagg\s*\(\s*distinct\s+(" + OBJ_PATTERN + r")\s*\)"),
        replacement=r"string_agg(distinct \1::text, '')",
    ),
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
            replacement = rule.replacement(match) if callable(rule.replacement) else match.expand(rule.replacement)
            if replacement == match.group():
                ret_parts.append(part[: match.start() + 1])
                part = part[match.start() + 1 :]
            else:
                ret_parts.append(part[: match.start()])
                part = replacement + part[match.end() :]
        else:
            ret_parts.append(part)
            break
    return "".join(ret_parts)


def snow_to_post(query: str) -> str:
    query = repl_part(query, PRE_SPLIT_RULES)
    return "".join(repl_part(part, RULES) for part in split_literals(query))
