"""Tests for table definitions and DDL generation."""

from datetime import datetime

from derp.orm import Table
from derp.orm.fields import (
    Field,
    ForeignKey,
    ForeignKeyAction,
    Integer,
    Serial,
    Timestamp,
    Varchar,
)


class User(Table, table="users"):
    id: int = Field(Serial(), primary_key=True)
    name: str = Field(Varchar(255))
    email: str = Field(Varchar(255), unique=True)
    created_at: datetime = Field(Timestamp(), default="now()")


class Post(Table, table="posts"):
    id: int = Field(Serial(), primary_key=True)
    title: str = Field(Varchar(255))
    content: str = Field(Varchar(10000), nullable=True)
    author_id: int = Field(
        Integer(),
        foreign_key=ForeignKey("users.id", on_delete=ForeignKeyAction.CASCADE),
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
    assert post_columns["author_id"].foreign_key is not None
    assert post_columns["author_id"].foreign_key.reference == "users.id"
    assert post_columns["author_id"].foreign_key.on_delete == ForeignKeyAction.CASCADE


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


def test_field_info_metadata():
    """Test that FieldInfo has correct table/field metadata."""
    user_columns = User.get_columns()

    # FieldInfo should have table and field names
    assert user_columns["id"]._table_name == "users"
    assert user_columns["id"]._field_name == "id"
    assert user_columns["name"]._table_name == "users"
    assert user_columns["name"]._field_name == "name"


def test_column_accessor():
    """Test the .c column accessor for query building."""
    # Access columns via .c attribute
    assert User.c.id._field_name == "id"
    assert User.c.name._field_name == "name"
    assert User.c.email._field_name == "email"

    # Check table name is set
    assert User.c.id._table_name == "users"

    # Check that non-existent columns raise AttributeError
    try:
        _ = User.c.nonexistent
        assert False, "Should have raised AttributeError"
    except AttributeError:
        pass


class BaseEntity(Table, table="base_entity"):
    """Base table with common fields."""

    id: int = Field(Serial(), primary_key=True)
    created_at: datetime = Field(Timestamp(), default="now()")


class Employee(BaseEntity, table="employees"):
    """Employee table inheriting from BaseEntity."""

    name: str = Field(Varchar(255))
    department: str = Field(Varchar(100))


class Manager(Employee, table="managers"):
    """Manager table inheriting from Employee."""

    level: int = Field(Integer())


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
    # Access inherited columns via .c
    assert Employee.c.id._field_name == "id"
    assert Employee.c.created_at._field_name == "created_at"

    # Access own columns via .c
    assert Employee.c.name._field_name == "name"
    assert Employee.c.department._field_name == "department"

    # Inherited columns should have the child's table name
    assert Employee.c.id._table_name == "employees"
    assert Employee.c.created_at._table_name == "employees"


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

    # Table name should be correct
    assert Manager.get_table_name() == "managers"

    # All columns should reference managers table
    assert Manager.c.id._table_name == "managers"
    assert Manager.c.name._table_name == "managers"
    assert Manager.c.level._table_name == "managers"


def test_inheritance_ddl():
    """Test DDL generation for inherited tables."""
    ddl = Employee.to_ddl()

    assert "CREATE TABLE employees" in ddl
    assert "id SERIAL PRIMARY KEY" in ddl
    assert "created_at TIMESTAMP" in ddl
    assert "name VARCHAR(255) NOT NULL" in ddl
    assert "department VARCHAR(100) NOT NULL" in ddl
