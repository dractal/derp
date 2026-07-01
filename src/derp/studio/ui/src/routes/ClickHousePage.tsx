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
  Boxes,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Columns3,
  Cpu,
  Info,
  Key,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

import type {
  ClickHouseColumnInfo,
  ClickHouseTableInfo,
} from "../api";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Checkbox } from "../components/ui/checkbox";
import { Skeleton } from "../components/ui/skeleton";
import { useClickHouse } from "../hooks/use-clickhouse";

function formatCellValue(value: unknown): string {
  if (value === null) return "NULL";
  if (value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function TableList({
  tables,
  onSelect,
}: {
  tables: ClickHouseTableInfo[];
  onSelect: (name: string) => void;
}) {
  if (tables.length === 0) {
    return <p className="text-sm text-muted-foreground">No tables found.</p>;
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
              <Boxes className="size-4 text-muted-foreground" />
              {table.name}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              {table.engine ? (
                <Badge variant="secondary" className="font-mono text-[10px] px-1 py-0">
                  {table.engine.name}
                </Badge>
              ) : null}
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

function Section({
  icon,
  title,
  count,
  open,
  onToggle,
  children,
}: {
  icon: React.ReactNode;
  title: string;
  count?: number;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border">
      <button
        type="button"
        className="flex w-full items-center gap-2 px-3 py-2 text-xs font-medium text-muted-foreground hover:bg-muted/50 cursor-pointer"
        onClick={onToggle}
      >
        {icon}
        <span>{title}</span>
        {count !== undefined ? (
          <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
            {count}
          </Badge>
        ) : null}
        <ChevronDown
          className={`ml-auto size-3 transition-transform ${open ? "rotate-180" : ""}`}
        />
      </button>
      {open ? <div className="border-t px-3 py-2">{children}</div> : null}
    </div>
  );
}

function EngineRow({ label, value }: { label: string; value: string | null }) {
  if (!value) return null;
  return (
    <div className="flex flex-wrap items-center gap-1.5 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono">{value}</span>
    </div>
  );
}

function TableMetadata({ table }: { table: ClickHouseTableInfo }) {
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const toggle = (k: string) => setOpen((p) => ({ ...p, [k]: !p[k] }));
  const engine = table.engine;
  const settingsEntries = engine ? Object.entries(engine.settings) : [];

  return (
    <div className="space-y-2">
      {engine ? (
        <Section
          icon={<Cpu className="size-3" />}
          title={`Engine: ${engine.name}`}
          open={open.engine ?? true}
          onToggle={() => toggle("engine")}
        >
          <div className="space-y-1">
            <EngineRow label="ORDER BY" value={engine.order_by} />
            <EngineRow label="PARTITION BY" value={engine.partition_by} />
            <EngineRow label="PRIMARY KEY" value={engine.primary_key} />
            <EngineRow label="SAMPLE BY" value={engine.sample_by} />
            <EngineRow label="TTL" value={engine.ttl} />
            {settingsEntries.map(([k, v]) => (
              <EngineRow key={k} label={k} value={v} />
            ))}
          </div>
        </Section>
      ) : null}

      {table.indexes.length > 0 ? (
        <Section
          icon={<Key className="size-3" />}
          title="Data-skipping indexes"
          count={table.indexes.length}
          open={open.indexes ?? false}
          onToggle={() => toggle("indexes")}
        >
          <div className="space-y-2">
            {table.indexes.map((idx) => (
              <div key={idx.name} className="flex flex-wrap items-center gap-1.5 text-xs">
                <span className="font-mono font-medium">{idx.name}</span>
                <Badge variant="secondary" className="font-mono text-[10px] px-1 py-0">
                  {idx.type}
                </Badge>
                <span className="font-mono">{idx.expression}</span>
                <span className="text-muted-foreground">GRANULARITY {idx.granularity}</span>
              </div>
            ))}
          </div>
        </Section>
      ) : null}
    </div>
  );
}

function ColumnInfoDropdown({ col }: { col: ClickHouseColumnInfo }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        className="inline-flex items-center gap-1 rounded border px-1 py-0 text-[10px] font-mono text-muted-foreground hover:bg-muted/50 cursor-pointer"
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
      >
        {col.type}
        <Info className="size-2.5" />
      </button>
      {open ? (
        <div
          className="absolute left-0 top-full z-30 mt-1 w-56 max-h-64 overflow-y-auto rounded-lg border bg-background shadow-lg text-xs"
          onClick={(e) => e.stopPropagation()}
        >
          <table className="w-full">
            <tbody className="divide-y">
              <tr>
                <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap">Type</td>
                <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.type}</td>
              </tr>
              <tr>
                <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap">Nullable</td>
                <td className="px-2.5 py-1.5 text-left">{col.nullable ? "Yes" : "No"}</td>
              </tr>
              {col.default ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">Default</td>
                  <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.default}</td>
                </tr>
              ) : null}
              {col.materialized ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">Materialized</td>
                  <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.materialized}</td>
                </tr>
              ) : null}
              {col.codec ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">Codec</td>
                  <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.codec}</td>
                </tr>
              ) : null}
              {col.ttl ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">TTL</td>
                  <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.ttl}</td>
                </tr>
              ) : null}
              {col.comment ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">Comment</td>
                  <td className="px-2.5 py-1.5 text-left break-all">{col.comment}</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}

function SortIcon({ direction }: { direction: false | "asc" | "desc" }) {
  if (direction === "asc") return <ArrowUp className="size-3" />;
  if (direction === "desc") return <ArrowDown className="size-3" />;
  return <ArrowUpDown className="size-3 text-muted-foreground/50" />;
}

type Row = Record<string, unknown>;
const columnHelper = createColumnHelper<Row>();

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
  table: ClickHouseTableInfo;
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

  const columns = useMemo(
    () =>
      table.columns.map((col) =>
        columnHelper.accessor((row) => row[col.name], {
          id: col.name,
          size: 180,
          header: col.name,
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
  });

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
      <div className="flex flex-wrap items-center gap-2 sm:gap-3">
        <Button variant="ghost" size="sm" onClick={onGoBack}>
          <ChevronLeft className="size-4" />
          <span className="hidden sm:inline">Back</span>
        </Button>
        <div className="flex items-center gap-2">
          <Boxes className="size-4 text-muted-foreground" />
          <span className="text-sm font-medium">{table.name}</span>
          <Badge variant="secondary">{total.toLocaleString()} rows</Badge>
        </div>
        <div className="ml-auto" />
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

      <TableMetadata table={table} />

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
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className="whitespace-nowrap px-3 py-2 text-left text-xs font-medium text-muted-foreground"
                  >
                    {header.isPlaceholder ? null : (
                      <div className="flex flex-col gap-1">
                        <button
                          type="button"
                          className="flex items-center gap-1 cursor-pointer select-none"
                          onClick={header.column.getToggleSortingHandler()}
                        >
                          <span>{header.column.id}</span>
                          <SortIcon direction={header.column.getIsSorted()} />
                        </button>
                        {(() => {
                          const col = table.columns.find((c) => c.name === header.column.id);
                          return col ? <ColumnInfoDropdown col={col} /> : null;
                        })()}
                      </div>
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
                  {columns.map((column, columnIndex) => (
                    <td key={`skeleton-cell-${column.id}-${columnIndex}`} className="px-3 py-2">
                      <Skeleton className="h-3 w-full max-w-40" />
                    </td>
                  ))}
                </tr>
              ))
            ) : reactTable.getRowModel().rows.length === 0 ? (
              <tr>
                <td colSpan={columns.length} className="px-3 py-32 text-center align-middle">
                  <div className="flex flex-col items-center gap-2">
                    <Boxes className="size-10 text-muted-foreground/30" />
                    <p className="text-sm font-medium text-muted-foreground">No rows found</p>
                    <p className="text-xs text-muted-foreground/70">This table is empty.</p>
                  </div>
                </td>
              </tr>
            ) : (
              reactTable.getRowModel().rows.map((row) => (
                <tr key={row.id} className="hover:bg-muted/30">
                  {row.getVisibleCells().map((cell) => {
                    const isFocused =
                      focusedCell?.rowId === row.id && focusedCell?.colId === cell.column.id;
                    return (
                      <td
                        key={cell.id}
                        className={`max-w-45 truncate whitespace-nowrap px-3 py-2 font-mono text-xs cursor-pointer ${isFocused ? "bg-muted ring-1 ring-ring" : ""}`}
                        onClick={() => setFocusedCell({ rowId: row.id, colId: cell.column.id })}
                      >
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    );
                  })}
                </tr>
              ))
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
    </div>
  );
}

export function ClickHousePage(): JSX.Element {
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
  } = useClickHouse();

  return (
    <div className="flex min-w-0 flex-1 flex-col gap-6 overflow-auto p-6 md:p-10">
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
          <CardContent className="pt-6 text-sm text-destructive">{error}</CardContent>
        </Card>
      ) : null}

      {!loading && !error && selectedTable === null ? (
        <>
          <div className="flex items-center justify-between">
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
