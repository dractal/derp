"""Typed-tuple returning executors (2-10 columns).

Subclass the untyped ``Returning*Tuple`` classes with per-arity type
params so type checkers infer e.g. ``tuple[int, str]`` instead of
``tuple[Any, ...]``.  Runtime behavior is identical.
"""

from __future__ import annotations

from derp.orm.query.builder import (
    ReturningManyTuple,
    ReturningOneTuple,
    ReturningOneTupleOptional,
)
from derp.orm.table import Table

# ── Single-row (INSERT) ─────────────────────────────────────────


class ROT2[T: Table, A, B](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B]:
        return await super().execute()


class ROT3[T: Table, A, B, C](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C]:
        return await super().execute()


class ROT4[T: Table, A, B, C, D](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D]:
        return await super().execute()


class ROT5[T: Table, A, B, C, D, E](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D, E]:
        return await super().execute()


class ROT6[T: Table, A, B, C, D, E, F](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F]:
        return await super().execute()


class ROT7[T: Table, A, B, C, D, E, F, G](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G]:
        return await super().execute()


class ROT8[T: Table, A, B, C, D, E, F, G, H](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G, H]:
        return await super().execute()


class ROT9[T: Table, A, B, C, D, E, F, G, H, I](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G, H, I]:
        return await super().execute()


class ROT10[T: Table, A, B, C, D, E, F, G, H, I, J](ReturningOneTuple[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G, H, I, J]:
        return await super().execute()


# ── Single-row optional (INSERT … ON CONFLICT) ──────────────────


class ROTO2[T: Table, A, B](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B] | None:
        return await super().execute()


class ROTO3[T: Table, A, B, C](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C] | None:
        return await super().execute()


class ROTO4[T: Table, A, B, C, D](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D] | None:
        return await super().execute()


class ROTO5[T: Table, A, B, C, D, E](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D, E] | None:
        return await super().execute()


class ROTO6[T: Table, A, B, C, D, E, F](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F] | None:
        return await super().execute()


class ROTO7[T: Table, A, B, C, D, E, F, G](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G] | None:
        return await super().execute()


class ROTO8[T: Table, A, B, C, D, E, F, G, H](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G, H] | None:
        return await super().execute()


class ROTO9[T: Table, A, B, C, D, E, F, G, H, I](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G, H, I] | None:
        return await super().execute()


class ROTO10[T: Table, A, B, C, D, E, F, G, H, I, J](ReturningOneTupleOptional[T]):
    async def execute(self) -> tuple[A, B, C, D, E, F, G, H, I, J] | None:
        return await super().execute()


# ── Multi-row (UPDATE / DELETE / INSERT bulk) ────────────────────


class RMT2[T: Table, A, B](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B]]:
        return await super().execute()


class RMT3[T: Table, A, B, C](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C]]:
        return await super().execute()


class RMT4[T: Table, A, B, C, D](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D]]:
        return await super().execute()


class RMT5[T: Table, A, B, C, D, E](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D, E]]:
        return await super().execute()


class RMT6[T: Table, A, B, C, D, E, F](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D, E, F]]:
        return await super().execute()


class RMT7[T: Table, A, B, C, D, E, F, G](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D, E, F, G]]:
        return await super().execute()


class RMT8[T: Table, A, B, C, D, E, F, G, H](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D, E, F, G, H]]:
        return await super().execute()


class RMT9[T: Table, A, B, C, D, E, F, G, H, I](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D, E, F, G, H, I]]:
        return await super().execute()


class RMT10[T: Table, A, B, C, D, E, F, G, H, I, J](ReturningManyTuple[T]):
    async def execute(self) -> list[tuple[A, B, C, D, E, F, G, H, I, J]]:
        return await super().execute()
