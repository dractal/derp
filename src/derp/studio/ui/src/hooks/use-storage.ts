import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import {
  fetchBuckets,
  fetchObjects,
  fetchObjectInfo,
  type ObjectInfo,
} from "../api";

export function useStorage(enabled: boolean) {
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null);
  const [prefix, setPrefix] = useState("");
  const [selectedObject, setSelectedObject] = useState<ObjectInfo | null>(null);

  const bucketsQuery = useQuery({
    queryKey: ["buckets"],
    queryFn: ({ signal }) => fetchBuckets(signal).then((r) => r.buckets),
    enabled,
  });

  const objectsQuery = useQuery({
    queryKey: ["objects", selectedBucket, prefix],
    queryFn: ({ signal }) => fetchObjects(selectedBucket!, prefix, signal),
    enabled: enabled && selectedBucket !== null,
  });

  const objectInfoQuery = useQuery({
    queryKey: ["objectInfo", selectedBucket, selectedObject?.key],
    queryFn: ({ signal }) =>
      fetchObjectInfo(selectedBucket!, selectedObject!.key, signal),
    enabled:
      enabled && selectedBucket !== null && selectedObject !== null,
  });

  const loading =
    (selectedBucket === null && bucketsQuery.isLoading) ||
    (selectedBucket !== null && objectsQuery.isLoading);

  const error = bucketsQuery.error ?? objectsQuery.error;

  const selectBucket = useCallback((name: string) => {
    setSelectedBucket(name);
    setPrefix("");
    setSelectedObject(null);
  }, []);

  const navigateToPrefix = useCallback((p: string) => {
    setPrefix(p);
    setSelectedObject(null);
  }, []);

  const navigateUp = useCallback(() => {
    if (prefix === "") {
      setSelectedBucket(null);
      setSelectedObject(null);
      return;
    }
    const parts = prefix.replace(/\/$/, "").split("/");
    parts.pop();
    setPrefix(parts.length > 0 ? parts.join("/") + "/" : "");
    setSelectedObject(null);
  }, [prefix]);

  const selectObject = useCallback((obj: ObjectInfo) => {
    setSelectedObject(obj);
  }, []);

  const deselectObject = useCallback(() => {
    setSelectedObject(null);
  }, []);

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
    selectedObject,
    objectDetail: objectInfoQuery.data ?? null,
    objectDetailLoading: objectInfoQuery.isLoading,
    selectObject,
    deselectObject,
  };
}
