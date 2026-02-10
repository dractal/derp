import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
  type VisibilityState,
} from "@tanstack/react-table";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronLeft,
  ChevronRight,
  Columns3,
  Database,
  Eye,
  EyeOff,
  Loader2,
  Table as TableIcon,
  X
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import { useDeleteRows, type ColumnInfo, type TableInfo } from "../api";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "../components/ui/alert-dialog";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Checkbox } from "../components/ui/checkbox";
import { Skeleton } from "../components/ui/skeleton";
import { useDatabase } from "../hooks/use-database";

function TableList({
  tables,
  onSelect,
}: {
  tables: TableInfo[];
  onSelect: (name: string) => void;
}) {
  if (tables.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No tables found.</p>
    );
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {tables.map((table) => (
        <Card
          key={table.name}
          className="cursor-pointer transition-colors hover:bg-muted/50"
          onClick={() => onSelect(table.name)}
        >
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-sm font-medium">
              <TableIcon className="size-4 text-muted-foreground" />
              {table.name}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <span>{table.columns.length} columns</span>
              <span>&middot;</span>
              <span>{table.row_count.toLocaleString()} rows</span>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function TableListSkeleton(): JSX.Element {
  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {Array.from({ length: 6 }).map((_, i) => (
        <Card key={i}>
          <CardHeader className="pb-2">
            <div className="flex items-center gap-2">
              <Skeleton className="size-4 rounded" />
              <Skeleton className="h-4 w-28" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex items-center gap-2">
              <Skeleton className="h-3 w-20" />
              <Skeleton className="h-3 w-16" />
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function formatCellValue(value: unknown): string {
  if (value === null) return "NULL";
  if (value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

type Row = Record<string, unknown>;

const columnHelper = createColumnHelper<Row>();

function SortIcon({ direction }: { direction: false | "asc" | "desc" }) {
  if (direction === "asc") return <ArrowUp className="size-3" />;
  if (direction === "desc") return <ArrowDown className="size-3" />;
  return <ArrowUpDown className="size-3 text-muted-foreground/50" />;
}

function RowBrowser({
  table,
  rows,
  total,
  page,
  pageSize,
  loading,
  onGoToPage,
  onGoBack,
}: {
  table: TableInfo;
  rows: Row[];
  total: number;
  page: number;
  pageSize: number;
  loading: boolean;
  onGoToPage: (page: number) => void;
  onGoBack: () => void;
}) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});
  const [columnDropdownOpen, setColumnDropdownOpen] = useState(false);
  const columnDropdownRef = useRef<HTMLDivElement>(null);
  const [focusedCell, setFocusedCell] = useState<{ rowId: string; colId: string } | null>(null);
  const [selectedRows, setSelectedRows] = useState<Set<string>>(() => new Set());
  const [hiddenRows, setHiddenRows] = useState<Set<string>>(() => new Set());
  const [showHidden, setShowHidden] = useState(false);
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false);

  const pkColumns = useMemo(
    () => table.columns.filter((c) => c.primary_key).map((c) => c.name),
    [table.columns],
  );

  const deleteRows = useDeleteRows(table.name);

  useEffect(() => {
    if (!columnDropdownOpen) return;
    const onClick = (e: MouseEvent) => {
      if (columnDropdownRef.current && !columnDropdownRef.current.contains(e.target as Node)) {
        setColumnDropdownOpen(false);
      }
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [columnDropdownOpen]);

  const toggleSelectRow = useCallback((rowId: string) => {
    setSelectedRows((prev) => {
      const next = new Set(prev);
      if (next.has(rowId)) {
        next.delete(rowId);
      } else {
        next.add(rowId);
      }
      return next;
    });
  }, []);

  const handleHideSelected = useCallback(() => {
    setHiddenRows((prev) => {
      const next = new Set(prev);
      for (const id of selectedRows) next.add(id);
      return next;
    });
    setSelectedRows(new Set());
  }, [selectedRows]);

  const confirmDeleteSelected = useCallback(() => {
    if (pkColumns.length === 0) return;
    const count = selectedRows.size;
    const rowKeys = Array.from(selectedRows).map((rowId) => {
      const row = rows[Number(rowId)];
      const key: Record<string, unknown> = {};
      for (const pk of pkColumns) key[pk] = row[pk];
      return key;
    });
    deleteRows.mutate(rowKeys, {
      onSuccess: () => {
        setSelectedRows(new Set());
        setDeleteConfirmOpen(false);
        toast.success(`Deleted ${count} ${count === 1 ? "row" : "rows"}`);
      },
      onError: (error) => {
        setDeleteConfirmOpen(false);
        toast.error(`Failed to delete ${count} ${count === 1 ? "row" : "rows"}`, {
          description: error.message,
        });
      },
    });
  }, [selectedRows, rows, pkColumns, deleteRows]);

  const handleCellClick = useCallback(
    (rowId: string, colId: string) => setFocusedCell({ rowId, colId }),
    [],
  );

  const columns = useMemo(
    () =>
      table.columns.map((col: ColumnInfo) =>
        columnHelper.accessor((row) => row[col.name], {
          id: col.name,
          size: 180,
          header: () => (
            <div className="flex flex-col gap-1">
              <span>{col.name}</span>
              <div className="flex items-center gap-1">
                <Badge variant="secondary" className="font-mono text-[10px] px-1 py-0">
                  {col.type}
                </Badge>
                {col.primary_key ? (
                  <Badge variant="default" className="text-[10px] px-1 py-0">
                    PK
                  </Badge>
                ) : null}
              </div>
            </div>
          ),
          cell: (info) => {
            const value = info.getValue();
            return (
              <span className={value === null ? "text-muted-foreground italic" : ""}>
                {formatCellValue(value)}
              </span>
            );
          },
        }),
      ),
    [table.columns],
  );

  const reactTable = useReactTable({
    data: rows,
    columns,
    state: { sorting, columnVisibility },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    columnResizeMode: "onChange",
  });

  const visibleRows = useMemo(
    () => reactTable.getRowModel().rows.filter((row) => showHidden || !hiddenRows.has(row.id)),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- rows drives reactTable.getRowModel() updates
    [reactTable, rows, sorting, showHidden, hiddenRows],
  );

  const allVisibleSelected = visibleRows.length > 0 && visibleRows.every((row) => selectedRows.has(row.id));
  const someVisibleSelected = visibleRows.some((row) => selectedRows.has(row.id));

  const toggleSelectAll = useCallback(() => {
    if (allVisibleSelected) {
      setSelectedRows(new Set());
    } else {
      setSelectedRows(new Set(visibleRows.map((row) => row.id)));
    }
  }, [allVisibleSelected, visibleRows]);

  const focusedValue = useMemo(() => {
    if (!focusedCell) return null;
    const row = rows.find((_r, i) => String(i) === focusedCell.rowId);
    if (!row) return null;
    return formatCellValue(row[focusedCell.colId]);
  }, [focusedCell, rows]);

  const start = page * pageSize + 1;
  const end = Math.min((page + 1) * pageSize, total);
  const totalPages = Math.ceil(total / pageSize);

  return (
    <div className="min-w-0 space-y-4">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" onClick={onGoBack}>
          <ChevronLeft className="size-4" />
          Back
        </Button>
        <div className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" />
          <span className="text-sm font-medium">{table.name}</span>
          <Badge variant="secondary">
            {total.toLocaleString()} rows
          </Badge>
        </div>
        {selectedRows.size > 0 ? (
          <div className="ml-auto flex items-center gap-2">
            <span className="text-xs font-medium">
              {selectedRows.size} {selectedRows.size === 1 ? "row" : "rows"} selected
            </span>
            <Button variant="outline" size="sm" onClick={handleHideSelected}>
              <EyeOff className="size-3" />
              Hide
            </Button>
            {pkColumns.length > 0 ? (
              <Button
                variant="destructive"
                size="sm"
                onClick={() => setDeleteConfirmOpen(true)}
                disabled={deleteRows.isPending}
              >
                Delete
              </Button>
            ) : null}
            <Button variant="ghost" size="sm" onClick={() => setSelectedRows(new Set())}>
              <X className="size-3" />
            </Button>
          </div>
        ) : (
          <div className="ml-auto" />
        )}
        <div className="relative" ref={columnDropdownRef}>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setColumnDropdownOpen((v) => !v)}
          >
            <Columns3 className="size-3" />
            Columns
          </Button>
          {columnDropdownOpen ? (
            <div className="absolute right-0 top-full z-20 mt-1 w-56 rounded-lg border bg-background p-2 shadow-lg">
              <div className="mb-2 flex items-center justify-between px-1">
                <span className="text-xs font-medium text-muted-foreground">Toggle columns</span>
                <button
                  type="button"
                  className="text-xs text-muted-foreground hover:text-foreground cursor-pointer"
                  onClick={() => setColumnVisibility({})}
                >
                  Show all
                </button>
              </div>
              <div className="max-h-64 overflow-y-auto">
                {reactTable.getAllLeafColumns().map((column) => (
                  <label
                    key={column.id}
                    className="flex items-center gap-2 rounded px-2 py-1.5 text-xs hover:bg-muted/50 cursor-pointer"
                  >
                    <Checkbox
                      checked={column.getIsVisible()}
                      onCheckedChange={(checked) => column.toggleVisibility(!!checked)}
                    />
                    <span className="font-mono">{column.id}</span>
                  </label>
                ))}
              </div>
            </div>
          ) : null}
        </div>
      </div>

      {hiddenRows.size > 0 ? (
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowHidden((v) => !v)}
          >
            {showHidden ? <Eye className="size-3" /> : <EyeOff className="size-3" />}
            {hiddenRows.size} hidden {hiddenRows.size === 1 ? "row" : "rows"}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setHiddenRows(new Set())}
          >
            Show all
          </Button>
        </div>
      ) : null}

      <div className="flex items-start gap-2 rounded-lg border bg-muted/30 px-3 py-2 font-mono text-xs">
        {focusedCell ? (
          <>
            <Badge variant="secondary" className="shrink-0 font-mono text-[10px]">
              {focusedCell.colId}
            </Badge>
            <span className="whitespace-pre-wrap break-all">{focusedValue}</span>
          </>
        ) : (
          <span className="text-muted-foreground">Select a cell to view its full content</span>
        )}
      </div>

      <div className="overflow-auto rounded-lg border">
        <table className="w-full text-sm">
          <thead className="border-b bg-muted/50">
            {reactTable.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                <th className="w-8 px-1 py-2 text-center">
                  <Checkbox
                    checked={someVisibleSelected && !allVisibleSelected ? "indeterminate" : allVisibleSelected}
                    onCheckedChange={toggleSelectAll}
                    className="cursor-pointer"
                  />
                </th>
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="whitespace-nowrap px-3 py-2 text-left text-xs font-medium text-muted-foreground"
                  >
                    {header.isPlaceholder ? null : (
                      <button
                        type="button"
                        className="flex items-center justify-center w-full gap-1 cursor-pointer select-none"
                        onClick={header.column.getToggleSortingHandler()}
                      >
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        <SortIcon direction={header.column.getIsSorted()} />
                      </button>
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody className="divide-y">
            {loading ? (
              Array.from({ length: 8 }).map((_, rowIndex) => (
                <tr key={`skeleton-row-${rowIndex}`}>
                  <td className="w-8 px-1 py-2">
                    <Skeleton className="mx-auto h-3 w-3 rounded-sm" />
                  </td>
                  {columns.map((column, columnIndex) => (
                    <td key={`skeleton-cell-${column.id}-${columnIndex}`} className="px-3 py-2">
                      <Skeleton className="h-3 w-full max-w-40" />
                    </td>
                  ))}
                </tr>
              ))
            ) : reactTable.getRowModel().rows.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length + 1}
                  className="px-3 py-8 text-center text-sm text-muted-foreground"
                >
                  No rows found.
                </td>
              </tr>
            ) : (
              visibleRows.map((row) => {
                const isHidden = hiddenRows.has(row.id);
                const isSelected = selectedRows.has(row.id);
                return (
                  <tr key={row.id} className={`hover:bg-muted/30 ${isHidden ? "opacity-40" : ""} ${isSelected ? "bg-muted/50" : ""}`}>
                    <td className="w-8 px-1 py-2 text-center">
                      <Checkbox
                        checked={isSelected}
                        onCheckedChange={() => toggleSelectRow(row.id)}
                        className="cursor-pointer"
                      />
                    </td>
                    {row.getVisibleCells().map((cell) => {
                      const isFocused =
                        focusedCell?.rowId === row.id &&
                        focusedCell?.colId === cell.column.id;
                      return (
                        <td
                          key={cell.id}
                          className={`max-w-[180px] truncate whitespace-nowrap px-3 py-2 font-mono text-xs cursor-pointer ${isFocused ? "bg-muted ring-1 ring-ring" : ""}`}
                          onClick={() => handleCellClick(row.id, cell.column.id)}
                        >
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </td>
                      );
                    })}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {total > pageSize ? (
        <div className="flex items-center justify-between">
          <p className="text-xs text-muted-foreground">
            Showing {start}&ndash;{end} of {total.toLocaleString()}
          </p>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon-xs"
              disabled={page === 0}
              onClick={() => onGoToPage(page - 1)}
            >
              <ChevronLeft className="size-3" />
            </Button>
            <span className="px-2 text-xs text-muted-foreground">
              {page + 1} / {totalPages}
            </span>
            <Button
              variant="outline"
              size="icon-xs"
              disabled={page >= totalPages - 1}
              onClick={() => onGoToPage(page + 1)}
            >
              <ChevronRight className="size-3" />
            </Button>
          </div>
        </div>
      ) : null}

      <AlertDialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete {selectedRows.size} {selectedRows.size === 1 ? "row" : "rows"}?</AlertDialogTitle>
            <AlertDialogDescription>
              This action cannot be undone. The selected {selectedRows.size === 1 ? "row" : "rows"} will be permanently deleted from the database.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteRows.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={confirmDeleteSelected}
              disabled={deleteRows.isPending}
            >
              {deleteRows.isPending ? <><Loader2 className="size-3 animate-spin" /> Deleting</> : "Delete"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

export function DatabasePage(): JSX.Element {
  const {
    tables,
    selectedTable,
    rows,
    total,
    page,
    pageSize,
    loading,
    error,
    selectTable,
    goToPage,
    goBack,
  } = useDatabase();

  return (
    <div className="flex min-w-0 flex-1 flex-col gap-6 p-6 md:p-10">
      {loading && selectedTable === null ? (
        <>
          <div className="flex items-center gap-2">
            <Skeleton className="h-6 w-24" />
          </div>
          <TableListSkeleton />
        </>
      ) : null}

      {error ? (
        <Card className="border-destructive/40">
          <CardContent className="pt-6 text-sm text-destructive">
            {error}
          </CardContent>
        </Card>
      ) : null}

      {!loading && !error && selectedTable === null ? (
        <>
          <div className="flex items-center gap-2">
            <Badge variant="secondary">{tables.length} tables</Badge>
          </div>
          <TableList tables={tables} onSelect={selectTable} />
        </>
      ) : null}

      {selectedTable !== null ? (
        <RowBrowser
          table={selectedTable}
          rows={rows}
          total={total}
          page={page}
          pageSize={pageSize}
          loading={loading}
          onGoToPage={goToPage}
          onGoBack={goBack}
        />
      ) : null}
    </div>
  );
}
