"""KV store interfaces and backends."""

from derp.kv.base import KVBase, KVMeta, KVStore
from derp.kv.client import KVClient
from derp.kv.errors import KVError, NotSupportedError
from derp.kv.serializers import KVSerializer, get_serializer, register_serializer
from derp.kv.valkey import ValkeyConfig, ValkeyStore

__all__ = [
    "KVStore",
    "KVMeta",
    "KVBase",
    "KVSerializer",
    "register_serializer",
    "get_serializer",
    "NotSupportedError",
    "KVError",
    "InMemoryKV",
    "KVClient",
    "ValkeyConfig",
    "ValkeyStore",
]
