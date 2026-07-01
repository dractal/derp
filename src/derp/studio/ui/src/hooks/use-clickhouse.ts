import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import { fetchClickHouseTableRows, fetchClickHouseTables } from "../api";

const PAGE_SIZE = 50;

export function useClickHouse() {
  const [selectedTableName, setSelectedTableName] = useState<string | null>(null);
  const [page, setPage] = useState(0);

  const tablesQuery = useQuery({
    queryKey: ["chTables"],
    queryFn: ({ signal }) => fetchClickHouseTables(signal).then((r) => r.tables),
  });

  const selectedTable =
    tablesQuery.data?.find((t) => t.name === selectedTableName) ?? null;

  const rowsQuery = useQuery({
    queryKey: ["chTableRows", selectedTableName, page],
    queryFn: ({ signal }) =>
      fetchClickHouseTableRows(selectedTableName!, PAGE_SIZE, page * PAGE_SIZE, signal),
    enabled: selectedTableName !== null,
  });

  const loading =
    (selectedTable === null && tablesQuery.isLoading) ||
    (selectedTable !== null && rowsQuery.isLoading);

  const error = tablesQuery.error ?? rowsQuery.error;

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
