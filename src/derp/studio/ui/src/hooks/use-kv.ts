import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import {
  fetchKVKeyInfo,
  fetchKVKeys,
  type KVKeyInfo,
} from "../api";

export function useKV(enabled: boolean) {
  const [searchPrefix, setSearchPrefix] = useState("");
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  const keysQuery = useQuery({
    queryKey: ["kv", "keys", searchPrefix],
    queryFn: ({ signal }) => fetchKVKeys(searchPrefix || undefined, 100, signal),
    enabled,
  });

  const keyInfoQuery = useQuery({
    queryKey: ["kv", "keyInfo", selectedKey],
    queryFn: ({ signal }) => fetchKVKeyInfo(selectedKey!, signal),
    enabled: enabled && selectedKey != null,
  });

  const selectKey = useCallback((key: string) => {
    setSelectedKey(key);
  }, []);

  const deselectKey = useCallback(() => {
    setSelectedKey(null);
  }, []);

  return {
    keys: (keysQuery.data?.keys ?? []) as string[],
    selectedKey,
    selectKey,
    deselectKey,
    searchPrefix,
    setSearchPrefix,
    loading: keysQuery.isLoading,
    error: keysQuery.error
      ? keysQuery.error instanceof Error
        ? keysQuery.error.message
        : String(keysQuery.error)
      : null,
    keyInfo: (keyInfoQuery.data ?? null) as KVKeyInfo | null,
    keyInfoLoading: keyInfoQuery.isLoading,
  };
}
