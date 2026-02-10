import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import {
  fetchBuckets,
  fetchObjects,
} from "../api";

export function useStorage() {
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [prefix, setPrefix] = useState("");

  const bucketsQuery = useQuery({
    queryKey: ["buckets"],
    queryFn: ({ signal }) => fetchBuckets(signal).then((r) => r.buckets),
  });

  const objectsQuery = useQuery({
    queryKey: ["objects", selectedBucket, prefix],
    queryFn: ({ signal }) => fetchObjects(selectedBucket!, prefix, signal),
    enabled: selectedBucket !== null,
  });

  const loading =
    (selectedBucket === null && bucketsQuery.isLoading) ||
    (selectedBucket !== null && objectsQuery.isLoading);

  const error = bucketsQuery.error ?? objectsQuery.error;

  const selectBucket = useCallback((name: string) => {
    setSelectedBucket(name);
    setPrefix("");
  }, []);

  const navigateToPrefix = useCallback((p: string) => {
    setPrefix(p);
  }, []);

  const navigateUp = useCallback(() => {
    if (prefix === "") {
      setSelectedBucket(null);
      return;
    }
    // Strip last path segment: "a/b/c/" -> "a/b/"
    const parts = prefix.replace(/\/$/, "").split("/");
    parts.pop();
    setPrefix(parts.length > 0 ? parts.join("/") + "/" : "");
  }, [prefix]);

  return {
    buckets: bucketsQuery.data ?? [],
    selectedBucket,
    prefix,
    prefixes: objectsQuery.data?.prefixes ?? [],
    objects: objectsQuery.data?.objects ?? [],
    loading,
    error: error ? (error instanceof Error ? error.message : String(error)) : null,
    selectBucket,
    navigateToPrefix,
    navigateUp,
  };
}
