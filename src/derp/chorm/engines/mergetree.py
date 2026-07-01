"""MergeTree family table engines.

Every MergeTree-family engine shares the same trailing clauses
(``ORDER BY``, ``PARTITION BY``, ``PRIMARY KEY``, ``SAMPLE BY``,
``TTL``, ``SETTINGS``).  The constructor arguments differ per engine.
"""

from __future__ import annotations

from typing import Any

from derp.chorm.engines.base import TableEngine, _render_arg, quoted


def _render_keys(value: Any) -> str:
    """Render ``ORDER BY``/``PARTITION BY``/``SAMPLE BY`` keys.

    Accepts:
    - ``"tuple()"`` literal string for explicit empty ordering
    - A string (passed through verbatim)
    - A tuple/list of strings or Column objects
    - A single Column
    """
    from derp.chorm.column.base import Column

    if isinstance(value, Column):
        return value._field_name or ""
    if isinstance(value, str):
        return value
    if isinstance(value, list | tuple):
        parts = []
        for v in value:
            if isinstance(v, Column):
                parts.append(v._field_name or "")
            else:
                parts.append(str(v))
        if len(parts) == 1:
            return parts[0]
        return f"({', '.join(parts)})"
    raise TypeError(f"Cannot render key expression from {type(value).__name__}")


class _MergeTreeBase(TableEngine):
    """Shared trailing-clause logic for MergeTree-family engines."""

    _name: str = "MergeTree"
    _args: tuple[Any, ...] = ()

    def __init__(
        self,
        *,
        order_by: Any = "tuple()",
        partition_by: Any = None,
        primary_key: Any = None,
        sample_by: Any = None,
        ttl: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> None:
        self._order_by = order_by
        self._partition_by = partition_by
        self._primary_key = primary_key
        self._sample_by = sample_by
        self._ttl = ttl
        self._settings = settings or {}

    def engine_clause(self) -> str:
        if not self._args:
            return f"ENGINE = {self._name}()"
        rendered = ", ".join(_render_arg(a) for a in self._args)
        return f"ENGINE = {self._name}({rendered})"

    def order_by_clause(self) -> str | None:
        if self._order_by is None:
            return None
        return f"ORDER BY {_render_keys(self._order_by)}"

    def partition_by_clause(self) -> str | None:
        if self._partition_by is None:
            return None
        return f"PARTITION BY {_render_keys(self._partition_by)}"

    def primary_key_clause(self) -> str | None:
        if self._primary_key is None:
            return None
        return f"PRIMARY KEY {_render_keys(self._primary_key)}"

    def sample_by_clause(self) -> str | None:
        if self._sample_by is None:
            return None
        return f"SAMPLE BY {_render_keys(self._sample_by)}"

    def ttl_clause(self) -> str | None:
        if self._ttl is None:
            return None
        return f"TTL {self._ttl}"

    def settings_clause(self) -> str | None:
        if not self._settings:
            return None
        parts = []
        for k, v in self._settings.items():
            if isinstance(v, bool):
                parts.append(f"{k} = {1 if v else 0}")
            elif isinstance(v, int | float):
                parts.append(f"{k} = {v}")
            else:
                # Quote string settings values.
                escaped = str(v).replace("'", "\\'")
                parts.append(f"{k} = '{escaped}'")
        return f"SETTINGS {', '.join(parts)}"


class MergeTree(_MergeTreeBase):
    """``MergeTree`` — the default storage engine.

    Example::

        engine = MergeTree(
            order_by=("user_id", "ts"),
            partition_by="toYYYYMM(ts)",
            ttl="ts + INTERVAL 90 DAY",
            settings={"index_granularity": 8192},
        )
    """

    _name = "MergeTree"


class ReplacingMergeTree(_MergeTreeBase):
    """``ReplacingMergeTree([version[, is_deleted]])``.

    Deduplicates rows with the same sort key.
    """

    _name = "ReplacingMergeTree"

    def __init__(
        self,
        version: str | None = None,
        is_deleted: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        args: list[Any] = []
        if version is not None:
            args.append(version)
        if is_deleted is not None:
            args.append(is_deleted)
        self._args = tuple(args)


class SummingMergeTree(_MergeTreeBase):
    """``SummingMergeTree([columns])`` — sums numeric columns on merge."""

    _name = "SummingMergeTree"

    def __init__(self, columns: tuple[str, ...] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if columns:
            self._args = (f"({', '.join(columns)})",)


class AggregatingMergeTree(_MergeTreeBase):
    """``AggregatingMergeTree()`` — combines aggregate-state columns on merge."""

    _name = "AggregatingMergeTree"


class CollapsingMergeTree(_MergeTreeBase):
    """``CollapsingMergeTree(sign)``."""

    _name = "CollapsingMergeTree"

    def __init__(self, sign: str = "Sign", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._args = (sign,)


class VersionedCollapsingMergeTree(_MergeTreeBase):
    """``VersionedCollapsingMergeTree(sign, version)``."""

    _name = "VersionedCollapsingMergeTree"

    def __init__(
        self, sign: str = "Sign", version: str = "Version", **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._args = (sign, version)


class GraphiteMergeTree(_MergeTreeBase):
    """``GraphiteMergeTree(config_section)``."""

    _name = "GraphiteMergeTree"

    def __init__(self, config_section: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._args = (quoted(config_section),)


# =============================================================================
# Replicated variants
# =============================================================================


class _ReplicatedMixin:
    """Prepends ``zoo_path`` and ``replica_name`` to engine args."""

    def __init__(
        self,
        zoo_path: str | None = None,
        replica_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        prefix: tuple[Any, ...] = ()
        if zoo_path is not None and replica_name is not None:
            prefix = (quoted(zoo_path), quoted(replica_name))
        elif zoo_path is not None:
            prefix = (quoted(zoo_path),)
        # Existing args (from the concrete engine) come after the replica
        # prefix.
        existing: tuple[Any, ...] = getattr(self, "_args", ())
        self._args = prefix + existing


class ReplicatedMergeTree(_ReplicatedMixin, MergeTree):
    _name = "ReplicatedMergeTree"


class ReplicatedReplacingMergeTree(_ReplicatedMixin, ReplacingMergeTree):
    _name = "ReplicatedReplacingMergeTree"


class ReplicatedSummingMergeTree(_ReplicatedMixin, SummingMergeTree):
    _name = "ReplicatedSummingMergeTree"


class ReplicatedAggregatingMergeTree(_ReplicatedMixin, AggregatingMergeTree):
    _name = "ReplicatedAggregatingMergeTree"


class ReplicatedCollapsingMergeTree(_ReplicatedMixin, CollapsingMergeTree):
    _name = "ReplicatedCollapsingMergeTree"


class ReplicatedVersionedCollapsingMergeTree(
    _ReplicatedMixin, VersionedCollapsingMergeTree
):
    _name = "ReplicatedVersionedCollapsingMergeTree"


class ReplicatedGraphiteMergeTree(_ReplicatedMixin, GraphiteMergeTree):
    _name = "ReplicatedGraphiteMergeTree"
