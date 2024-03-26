from typing import List

import pytest

from yellowbox_snowglobe.snow_to_post import TextLiteral, snow_to_post, split_literals, split_sql_to_statements


@pytest.mark.parametrize(
    ("snow", "post"),
    [
        ("", ""),
        ("select * from foo", "select * from foo"),
        ("select * from d..foo", "select * from d.public.foo"),
        ("select * from d_1..foo", "select * from d_1.public.foo"),
        (
            "select x, root.value from bar, lateral flatten(d) as root where root.value*root.value = x;",
            "select x, root.value from bar, lateral unnest(d) as root(value) where root.value*root.value = x;",
        ),
        (
            "select * from table(result_scan('e0bd4e52-0296-4877-b392-f971c38cc82c'))",
            "!retrieve e0bd4e52-0296-4877-b392-f971c38cc82c",
        ),
        ("select * from foo where x = 'desc table foo''2'", "select * from foo where x = 'desc table foo''2'"),
        ("select * from foo where x = '''desc table foo''2'", "select * from foo where x = '''desc table foo''2'"),
        ("select * from foo sample (10 rows)", "select * from foo order by random() limit 10"),
        ("select json_column:field::number from foo", "select cast(json_column->>'field' as integer) from foo"),
        ("select json_column:field::string from foo", "select json_column->>'field' from foo"),
    ],
)
def test_snow_to_post(snow: str, post: str):
    assert snow_to_post(snow) == post


@pytest.mark.parametrize(
    ("joined", "split"),
    [
        ("", []),
        ("a", ["a"]),
        ("a;b", ["a;b"]),
        ("a;b;c", ["a;b;c"]),
        ("a;b;c;", ["a;b;c;"]),
        ("a'be'f", ["a", TextLiteral("'be'"), "f"]),
        ("a'f'", ["a", TextLiteral("'f'")]),
        ("a'f", ["a", TextLiteral("'f")]),
        ("a'b;c''d;e';f", ["a", TextLiteral("'b;c''d;e'"), ";f"]),
    ],
)
def test_split_literals(joined: str, split: List[str]):
    assert list(split_literals(joined)) == split


@pytest.mark.parametrize(
    ("joined", "split"),
    [
        ("", []),
        ("a", ["a"]),
        ("a;b", ["a", "b"]),
        ("a;b;c", ["a", "b", "c"]),
        ("a;b;c;", ["a", "b", "c"]),
        ("a'b;c''d;e';f", ["a'b;c''d;e'", "f"]),
    ],
)
def test_split_statements(joined: str, split: List[str]):
    assert list(split_sql_to_statements(joined)) == split
