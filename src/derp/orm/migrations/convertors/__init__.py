"""Statement to SQL convertors.

All convertors auto-register with ConvertorRegistry when imported.
"""

# Import modules to trigger registration (side effect)
from derp.orm.migrations.convertors import column as _column  # noqa: F401
from derp.orm.migrations.convertors import constraint as _constraint  # noqa: F401
from derp.orm.migrations.convertors import enum as _enum  # noqa: F401
from derp.orm.migrations.convertors import index as _index  # noqa: F401
from derp.orm.migrations.convertors import policy as _policy  # noqa: F401
from derp.orm.migrations.convertors import role as _role  # noqa: F401
from derp.orm.migrations.convertors import schema as _schema  # noqa: F401
from derp.orm.migrations.convertors import sequence as _sequence  # noqa: F401
from derp.orm.migrations.convertors import table as _table  # noqa: F401
from derp.orm.migrations.convertors.base import ConvertorRegistry, StatementConvertor

__all__ = [
    "ConvertorRegistry",
    "StatementConvertor",
]
