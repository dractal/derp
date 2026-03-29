"""Static type assertions for the ORM query builder.

These tests don't execute queries — they verify that ``ty`` infers the
correct return types for ``returning().execute()`` across all query
variants.  Run with ``uv run ty check tests/orm/test_types.py``.
"""

from __future__ import annotations

import uuid
from typing import assert_type

from derp.orm import UUID, Boolean, Field, Serial, Table, Text, Varchar
from derp.orm.engine import DatabaseEngine

# -- Test table ---------------------------------------------------------------


class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field(unique=True)
    is_active: Boolean = Field(default=True)


class Org(Table, table="orgs"):
    id: UUID = Field(primary=True)
    slug: Varchar[255] = Field(unique=True)
    name: Text = Field()


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    user_id: Serial = Field()
    title: Varchar[255] = Field()


# -- INSERT returning ---------------------------------------------------------


async def insert_returning_table(db: DatabaseEngine) -> None:
    result = await db.insert(User).values(name="Alice").returning(User).execute()
    assert_type(result, User)


async def insert_returning_scalar(db: DatabaseEngine) -> None:
    result = await db.insert(User).values(name="Alice").returning(User.id).execute()
    assert_type(result, int)


async def insert_returning_scalar_str(db: DatabaseEngine) -> None:
    result = await db.insert(User).values(name="Alice").returning(User.name).execute()
    assert_type(result, str)


async def insert_returning_tuple_2(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User).values(name="Alice").returning(User.id, User.name).execute()
    )
    assert_type(result, tuple[int, str])


async def insert_returning_tuple_3(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values(name="Alice")
        .returning(User.id, User.name, User.email)
        .execute()
    )
    assert_type(result, tuple[int, str, str])


# -- INSERT ignore_conflicts returning ----------------------------------------


async def insert_conflict_returning_table(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values(name="Alice")
        .ignore_conflicts(target=User.email)
        .returning(User)
        .execute()
    )
    assert_type(result, User | None)


async def insert_conflict_returning_scalar(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values(name="Alice")
        .ignore_conflicts(target=User.email)
        .returning(User.id)
        .execute()
    )
    assert_type(result, int | None)


async def insert_conflict_returning_tuple(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values(name="Alice")
        .ignore_conflicts(target=User.email)
        .returning(User.id, User.name)
        .execute()
    )
    assert_type(result, tuple[int, str] | None)


# -- INSERT bulk (values_list) returning --------------------------------------


async def insert_bulk_returning_table(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values_list([{"name": "Alice"}, {"name": "Bob"}])
        .returning(User)
        .execute()
    )
    assert_type(result, list[User])


async def insert_bulk_returning_scalar(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values_list([{"name": "Alice"}, {"name": "Bob"}])
        .returning(User.id)
        .execute()
    )
    assert_type(result, list[int])


async def insert_bulk_returning_tuple(db: DatabaseEngine) -> None:
    result = await (
        db.insert(User)
        .values_list([{"name": "Alice"}, {"name": "Bob"}])
        .returning(User.id, User.name)
        .execute()
    )
    assert_type(result, list[tuple[int, str]])


# -- UPDATE returning ---------------------------------------------------------


async def update_returning_table(db: DatabaseEngine) -> None:
    result = await (
        db.update(User).set(name="Bob").where(User.id == 1).returning(User).execute()
    )
    assert_type(result, list[User])


async def update_returning_scalar(db: DatabaseEngine) -> None:
    result = await (
        db.update(User).set(name="Bob").where(User.id == 1).returning(User.id).execute()
    )
    assert_type(result, list[int])


async def update_returning_tuple(db: DatabaseEngine) -> None:
    result = await (
        db.update(User)
        .set(name="Bob")
        .where(User.id == 1)
        .returning(User.id, User.name)
        .execute()
    )
    assert_type(result, list[tuple[int, str]])


# -- DELETE returning ---------------------------------------------------------


async def delete_returning_table(db: DatabaseEngine) -> None:
    result = await db.delete(User).where(User.id == 1).returning(User).execute()
    assert_type(result, list[User])


async def delete_returning_scalar(db: DatabaseEngine) -> None:
    result = await db.delete(User).where(User.id == 1).returning(User.id).execute()
    assert_type(result, list[int])


async def delete_returning_tuple(db: DatabaseEngine) -> None:
    result = await (
        db.delete(User).where(User.id == 1).returning(User.id, User.email).execute()
    )
    assert_type(result, list[tuple[int, str]])


# -- Cross-table scalar type (UUID) ------------------------------------------


async def insert_returning_uuid_scalar(db: DatabaseEngine) -> None:
    result = await (
        db.insert(Org).values(slug="acme", name="Acme").returning(Org.id).execute()
    )
    assert_type(result, uuid.UUID)


async def update_returning_uuid_scalar(db: DatabaseEngine) -> None:
    result = await (
        db.update(Org)
        .set(name="New")
        .where(Org.slug == "acme")
        .returning(Org.id)
        .execute()
    )
    assert_type(result, list[uuid.UUID])


# -- SELECT -------------------------------------------------------------------


async def select_table(db: DatabaseEngine) -> None:
    result = await db.select(User).execute()
    assert_type(result, list[User])


async def select_single_column(db: DatabaseEngine) -> None:
    result = await db.select(User.id).from_(User).execute()
    assert_type(result, list[int])


async def select_single_column_str(db: DatabaseEngine) -> None:
    result = await db.select(User.name).from_(User).execute()
    assert_type(result, list[str])


async def select_two_columns(db: DatabaseEngine) -> None:
    result = await db.select(User.id, User.name).from_(User).execute()
    assert_type(result, list[tuple[int, str]])


async def select_three_columns(db: DatabaseEngine) -> None:
    result = await db.select(User.id, User.name, User.email).from_(User).execute()
    assert_type(result, list[tuple[int, str, str]])


async def select_bool_column(db: DatabaseEngine) -> None:
    result = await db.select(User.is_active).from_(User).execute()
    assert_type(result, list[bool])


async def select_uuid_column(db: DatabaseEngine) -> None:
    result = await db.select(Org.id).from_(Org).execute()
    assert_type(result, list[uuid.UUID])


async def select_cross_table_tuple(db: DatabaseEngine) -> None:
    result = await db.select(Org.id, Org.slug, Org.name).from_(Org).execute()
    assert_type(result, list[tuple[uuid.UUID, str, str]])


# -- SELECT with JOIN ---------------------------------------------------------


async def select_join_table(db: DatabaseEngine) -> None:
    result = await (
        db.select(User)
        .inner_join(Post, Post.user_id == User.id)
        .where(Post.title == "Hello")
        .execute()
    )
    assert_type(result, list[User])


async def select_join_columns_cross_table(db: DatabaseEngine) -> None:
    result = await (
        db.select(User.name, Post.title)
        .from_(User)
        .inner_join(Post, Post.user_id == User.id)
        .execute()
    )
    assert_type(result, list[tuple[str, str]])


async def select_join_scalar(db: DatabaseEngine) -> None:
    result = await (
        db.select(Post.id)
        .from_(Post)
        .inner_join(User, User.id == Post.user_id)
        .where(User.is_active)
        .execute()
    )
    assert_type(result, list[int])


async def select_left_join_mixed(db: DatabaseEngine) -> None:
    result = await (
        db.select(User.id, User.name, Post.title)
        .from_(User)
        .left_join(Post, Post.user_id == User.id)
        .execute()
    )
    assert_type(result, list[tuple[int, str, str]])
