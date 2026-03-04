"""Tests for table deduplication and discovery."""

from __future__ import annotations

import pytest

from derp.orm import Table
from derp.orm.fields import Field, Serial, Varchar
from derp.orm.loader import _deduplicate_tables

# ── Test fixtures ──────────────────────────────────────────────────────


class Product(Table, table="products"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))


class Order(Table, table="orders"):
    id: int = Field(Serial(), primary_key=True)


class Animal(Table, table="animals"):
    id: int = Field(Serial(), primary_key=True)


class Dog(Animal, table="animals"):
    breed: str = Field(Varchar(255))


class Cat(Animal, table="animals"):
    color: str = Field(Varchar(100))


class Puppy(Dog, table="animals"):
    toy: str = Field(Varchar(255))


# ── _deduplicate_tables tests ──────────────────────────────────────────


def test_dedup_no_inheritance():
    """Independent tables are unchanged."""
    tables = [Product, Order]
    result = _deduplicate_tables(tables)
    assert result == [Product, Order]


def test_dedup_empty():
    """Empty list returns empty."""
    assert _deduplicate_tables([]) == []


def test_dedup_single():
    """Single table returns as-is."""
    assert _deduplicate_tables([Product]) == [Product]


def test_dedup_parent_and_child():
    """Parent is dropped when child is present."""
    result = _deduplicate_tables([Animal, Dog])
    assert result == [Dog]


def test_dedup_child_and_parent_order():
    """Order shouldn't matter — child wins regardless."""
    result = _deduplicate_tables([Dog, Animal])
    assert result == [Dog]


def test_dedup_linear_chain():
    """Longest chain: only youngest kept."""
    result = _deduplicate_tables([Animal, Dog, Puppy])
    assert result == [Puppy]


def test_dedup_linear_chain_reversed():
    """Reversed input order still keeps youngest."""
    result = _deduplicate_tables([Puppy, Dog, Animal])
    assert result == [Puppy]


def test_dedup_branch_error():
    """Two siblings sharing an ancestor raises ValueError."""
    with pytest.raises(ValueError, match="Ambiguous table inheritance"):
        _deduplicate_tables([Dog, Cat])


def test_dedup_branch_error_with_ancestor():
    """Branch detected even when common ancestor is in the list."""
    with pytest.raises(ValueError, match="Ambiguous table inheritance"):
        _deduplicate_tables([Animal, Dog, Cat])


def test_dedup_mixed_hierarchies():
    """Independent hierarchies with one chain dedup correctly."""
    result = _deduplicate_tables([Product, Animal, Dog, Order])
    assert set(result) == {Product, Dog, Order}


def test_dedup_preserves_non_related():
    """Unrelated tables with a parent-child pair."""
    result = _deduplicate_tables([Order, Animal, Dog])
    assert Dog in result
    assert Animal not in result
    assert Order in result
