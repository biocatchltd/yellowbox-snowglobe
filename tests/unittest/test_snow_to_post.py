from typing import List

from pytest import mark

from yellowbox_snowglobe.snow_to_post import TextLiteral, snow_to_post, split_literals, split_sql_to_statements


@mark.parametrize(
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
        ("select current_timestamp() from foo", "select current_timestamp from foo"),
        ("select data:a::number from foo", "select cast(data->>'a' as integer) from foo"),
        ("select data:a::int from foo", "select cast(data->>'a' as integer) from foo"),
        ("select t.data:a::int from foo", "select cast(t.data->>'a' as integer) from foo"),
        ("select data:a::string from foo", "select data->>'a' from foo"),
        (
            "select coalesce(data:a, data:b)::string from foo",
            "select coalesce(data->>'a', data->>'b') from foo",
        ),
        (
            "select * from foo where x = 1 qualify row_number() over (partition by y order by z) = 1",
            "select * from (select *, row_number() over (partition by y order by z) "
            "as __snowglobe_qualify_row_number "
            "from foo where x = 1) __snowglobe_qualify "
            "where __snowglobe_qualify_row_number = 1",
        ),
        (
            """
            select coalesce(x, y) as z, count(*) cnt
            from foo
            where z is not null
            group by 1
            """,
            """
            select coalesce(x, y) as z, count(*) cnt
            from foo
            where coalesce(x, y) is not null
            group by 1
            """,
        ),
        (
            """
            select count(*) cnt, coalesce(x, y) as z
            from foo
            where z = 'literal z'
            group by 1
            """,
            """
            select count(*) cnt, coalesce(x, y) as z
            from foo
            where coalesce(x, y) = 'literal z'
            group by 1
            """,
        ),
        (
            """
            select x as z, y as q, count(*) cnt
            from foo
            where z > 1 and q is null
            group by 1
            """,
            """
            select x as z, y as q, count(*) cnt
            from foo
            where x > 1 and y is null
            group by 1
            """,
        ),
        (
            """
            select x as z, count(*) cnt
            from foo
            where z is not null
            group by 1
            union all
            select y as z, count(*) cnt
            from bar
            where z is not null
            group by 1
            """,
            """
            select x as z, count(*) cnt
            from foo
            where x is not null
            group by 1
            union all
            select y as z, count(*) cnt
            from bar
            where y is not null
            group by 1
            """,
        ),
    ],
)
def test_snow_to_post(snow: str, post: str):
    assert snow_to_post(snow) == post


@mark.parametrize(
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


@mark.parametrize(
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
