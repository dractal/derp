import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import {
  fetchTableRows,
  fetchTables,
} from "../api";

const PAGE_SIZE = 50;

export function useDatabase() {
  const [selectedTableName, setSelectedTableName] = useState<string | null>(null);
  const [page, setPage] = useState(0);

  const tablesQuery = useQuery({
    queryKey: ["tables"],
    queryFn: ({ signal }) => fetchTables(signal).then((r) => r.tables),
  });

  const selectedTable = tablesQuery.data?.find((t) => t.name === selectedTableName) ?? null;

  const rowsQuery = useQuery({
    queryKey: ["tableRows", selectedTableName, page],
    queryFn: ({ signal }) =>
      fetchTableRows(selectedTableName!, PAGE_SIZE, page * PAGE_SIZE, signal),
    enabled: selectedTableName !== null,
  });

  const loading =
    (selectedTable === null && tablesQuery.isLoading) ||
    (selectedTable !== null && rowsQuery.isLoading);

  const error =
    tablesQuery.error ?? rowsQuery.error;

  const selectTable = useCallback((name: string) => {
    setSelectedTableName(name);
    setPage(0);
  }, []);

  const goToPage = useCallback((p: number) => {
    setPage(p);
  }, []);

  const goBack = useCallback(() => {
    setSelectedTableName(null);
    setPage(0);
  }, []);

  return {
    tables: tablesQuery.data ?? [],
    selectedTable,
    rows: rowsQuery.data?.rows ?? [],
    total: rowsQuery.data?.total ?? 0,
    page,
    pageSize: PAGE_SIZE,
    loading,
    error: error ? (error instanceof Error ? error.message : String(error)) : null,
    selectTable,
    goToPage,
    goBack,
  };
}
