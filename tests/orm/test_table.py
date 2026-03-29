"""Tests for table definitions and DDL generation."""

import pytest

from derp.orm import (
    FK,
    Field,
    Integer,
    Nullable,
    Serial,
    Table,
    Timestamp,
    Varchar,
)


class User(Table, table="users"):
    id: Serial = Field(primary=True)
    name: Varchar[255] = Field()
    email: Varchar[255] = Field(unique=True)
    created_at: Timestamp = Field(default="now()")


class Post(Table, table="posts"):
    id: Serial = Field(primary=True)
    title: Varchar[255] = Field()
    content: Nullable[Varchar[10000]] = Field()
    author_id: Integer = Field(
        foreign_key="users.id",
        on_delete="cascade",
    )


def test_table_name():
    """Test that table names are correctly set."""
    assert User.get_table_name() == "users"
    assert Post.get_table_name() == "posts"


def test_columns():
    """Test that columns are correctly parsed."""
    user_columns = User.get_columns()
    assert "id" in user_columns
    assert "name" in user_columns
    assert "email" in user_columns
    assert "created_at" in user_columns

    # Check column properties
    assert user_columns["id"].primary_key is True
    assert user_columns["email"].unique is True
    assert user_columns["created_at"].default == "now()"


def test_primary_key():
    """Test primary key detection."""
    pk = User.get_primary_key()
    assert pk is not None
    assert pk[0] == "id"
    assert pk[1].primary_key is True


def test_foreign_key():
    """Test foreign key configuration."""
    post_columns = Post.get_columns()
    assert post_columns["author_id"].foreign_key == "users.id"
    assert post_columns["author_id"].on_delete == FK.CASCADE


def test_ddl_generation():
    """Test DDL generation for tables."""
    ddl = User.to_ddl()

    assert "CREATE TABLE users" in ddl
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "name VARCHAR(255) NOT NULL" in ddl
    assert "email VARCHAR(255) NOT NULL UNIQUE" in ddl
    assert "DEFAULT now()" in ddl


def test_ddl_foreign_key():
    """Test DDL generation with foreign keys."""
    ddl = Post.to_ddl()

    assert "CREATE TABLE posts" in ddl
    assert "FOREIGN KEY (author_id) REFERENCES users(id)" in ddl
    assert "ON DELETE CASCADE" in ddl


def test_column_metadata():
    """Test that Column has correct table/field metadata."""
    user_columns = User.get_columns()

    # Column should have table and field names
    assert user_columns["id"]._table_name == "users"
    assert user_columns["id"]._field_name == "id"
    assert user_columns["name"]._table_name == "users"
    assert user_columns["name"]._field_name == "name"


def test_column_accessor():
    """Test direct column access for query building."""
    # Access columns via class attribute
    assert User.id._field_name == "id"
    assert User.name._field_name == "name"
    assert User.email._field_name == "email"

    # Check table name is set
    assert User.id._table_name == "users"

    # Check that non-existent columns raise AttributeError
    try:
        _ = User.nonexistent  # type: ignore[unresolved-attribute]
        assert False, "Should have raised AttributeError"
    except AttributeError:
        pass


class BaseEntity(Table):
    """Abstract base table with common fields (no explicit table name)."""

    id: Serial = Field(primary=True)
    created_at: Timestamp = Field(default="now()")


class Employee(BaseEntity, table="employees"):
    """Employee table inheriting from BaseEntity."""

    name: Varchar[255] = Field()
    department: Varchar[100] = Field()


class Manager(Employee, table="employees"):
    """Manager table inheriting from Employee, same SQL table."""

    level: Integer = Field()


def test_inheritance_basic():
    """Test that child tables inherit columns from parent tables."""
    employee_columns = Employee.get_columns()

    # Should have inherited columns from BaseEntity
    assert "id" in employee_columns
    assert "created_at" in employee_columns

    # Should have its own columns
    assert "name" in employee_columns
    assert "department" in employee_columns

    # Should have correct table name
    assert Employee.get_table_name() == "employees"


def test_inheritance_column_accessor():
    """Test that column accessor works with inherited columns."""
    # Access inherited columns
    assert Employee.id._field_name == "id"
    assert Employee.created_at._field_name == "created_at"

    # Access own columns
    assert Employee.name._field_name == "name"
    assert Employee.department._field_name == "department"

    # Inherited columns should have the child's table name
    assert Employee.id._table_name == "employees"
    assert Employee.created_at._table_name == "employees"


def test_inheritance_multi_level():
    """Test multi-level inheritance."""
    manager_columns = Manager.get_columns()

    # Should have columns from BaseEntity
    assert "id" in manager_columns
    assert "created_at" in manager_columns

    # Should have columns from Employee
    assert "name" in manager_columns
    assert "department" in manager_columns

    # Should have its own columns
    assert "level" in manager_columns

    # Table name should match parent
    assert Manager.get_table_name() == "employees"

    # All columns should reference employees table
    assert Manager.id._table_name == "employees"
    assert Manager.name._table_name == "employees"
    assert Manager.level._table_name == "employees"


def test_inheritance_ddl():
    """Test DDL generation for inherited tables."""
    ddl = Employee.to_ddl()

    assert "CREATE TABLE employees" in ddl
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "created_at TIMESTAMP" in ddl
    assert "name VARCHAR(255) NOT NULL" in ddl
    assert "department VARCHAR(100) NOT NULL" in ddl


def test_inheritance_table_name_mismatch():
    """Test that mismatched table names in inheritance raise TypeError."""

    class Parent(Table, table="things"):
        id: Serial = Field(primary=True)

    with pytest.raises(TypeError, match="must use the same table name"):

        class BadChild(Parent, table="other_things"):
            extra: Varchar[100] = Field()


def test_inheritance_table_name_match():
    """Test that matching table names in inheritance are allowed."""

    class Parent2(Table, table="items"):
        id: Serial = Field(primary=True)

    class GoodChild(Parent2, table="items"):
        extra: Varchar[100] = Field()

    assert GoodChild.get_table_name() == "items"
    assert "id" in GoodChild.get_columns()
    assert "extra" in GoodChild.get_columns()


# -- Generated columns -------------------------------------------------------


class OrderLine(Table, table="order_lines"):
    id: Serial = Field(primary=True)
    price: Integer = Field()
    quantity: Integer = Field()
    amount: Integer = Field(generated="price * quantity")


def test_generated_column_metadata():
    """Test that generated column stores the expression."""
    cols = OrderLine.get_columns()
    assert cols["amount"].generated == "price * quantity"
    assert cols["amount"].default is None
    assert cols["price"].generated is None


def test_generated_column_ddl():
    """Test DDL generation for generated columns."""
    ddl = OrderLine.to_ddl()
    assert (
        "amount INTEGER NOT NULL GENERATED ALWAYS AS (price * quantity) STORED" in ddl
    )


def test_generated_and_default_mutually_exclusive():
    """Test that generated + default raises ValueError."""
    with pytest.raises(ValueError, match="cannot have both"):
        Field(default="0", generated="price * quantity")
