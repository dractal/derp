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
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  Columns3,
  Database,
  Eye,
  EyeOff,
  GitFork,
  Info,
  Key,
  LayoutGrid,
  Link,
  Loader2,
  Pencil,
  ShieldCheck,
  Table as TableIcon,
  Trash,
  X
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";

import { useDeleteRows, useUpdateRow, type ColumnInfo, type TableInfo } from "../api";
import { ERDiagram } from "../components/erd";
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
import { Input } from "../components/ui/input";
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
            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <span>{table.columns.length} columns</span>
              <span>&middot;</span>
              <span>{table.row_count.toLocaleString()} rows</span>
              {table.indexes.length > 0 ? (
                <>
                  <span>&middot;</span>
                  <span>{table.indexes.length} {table.indexes.length === 1 ? "index" : "indexes"}</span>
                </>
              ) : null}
              {table.foreign_keys.length > 0 ? (
                <>
                  <span>&middot;</span>
                  <span>{table.foreign_keys.length} FK{table.foreign_keys.length !== 1 ? "s" : ""}</span>
                </>
              ) : null}
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

type InputKind = "date" | "datetime-local" | "time" | "number" | "boolean" | "json" | "text";

const _INT_TYPES = new Set(["integer", "bigint", "smallint", "int2", "int4", "int8", "serial", "bigserial", "smallserial"]);
const _FLOAT_TYPES = new Set(["real", "double precision", "float4", "float8", "numeric", "decimal", "money"]);
const _BOOL_TYPES = new Set(["boolean", "bool"]);
const _JSON_TYPES = new Set(["json", "jsonb"]);

function inputKindForColumn(colType: string): InputKind {
  const t = colType.toLowerCase();
  if (t === "date") return "date";
  if (t.startsWith("timestamp")) return "datetime-local";
  if (t.startsWith("time")) return "time";
  if (_INT_TYPES.has(t) || _FLOAT_TYPES.has(t) || t.startsWith("numeric")) return "number";
  if (_BOOL_TYPES.has(t)) return "boolean";
  if (_JSON_TYPES.has(t)) return "json";
  if (t === "text") return "json";
  return "text";
}

function toInputValue(value: unknown, kind: InputKind): string {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (kind === "date") {
    const d = new Date(s);
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
    return s.slice(0, 10);
  }
  if (kind === "datetime-local") {
    const d = new Date(s);
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 23);
    return s;
  }
  if (kind === "time") {
    const match = s.match(/(\d{2}:\d{2}(?::\d{2})?)/);
    return match ? match[1] : s;
  }
  if (kind === "boolean") return value === true || s.toLowerCase() === "true" ? "true" : "false";
  if (kind === "json") {
    if (typeof value === "object") return JSON.stringify(value, null, 2);
    return s;
  }
  return formatCellValue(value);
}

function fromInputValue(raw: string, kind: InputKind): unknown {
  if (raw === "") return "";
  if (kind === "datetime-local") return new Date(raw).toISOString();
  if (kind === "boolean") return raw === "true";
  if (kind === "json") {
    try { return JSON.parse(raw); } catch { return raw; }
  }
  return raw;
}

type Row = Record<string, unknown>;

const columnHelper = createColumnHelper<Row>();

function SortIcon({ direction }: { direction: false | "asc" | "desc" }) {
  if (direction === "asc") return <ArrowUp className="size-3" />;
  if (direction === "desc") return <ArrowDown className="size-3" />;
  return <ArrowUpDown className="size-3 text-muted-foreground/50" />;
}

function ColumnInfoDropdown({ col }: { col: ColumnInfo }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
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
              {col.primary_key ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap">Primary Key</td>
                  <td className="px-2.5 py-1.5 text-left">Yes</td>
                </tr>
              ) : null}
              {col.unique ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap">Unique</td>
                  <td className="px-2.5 py-1.5 text-left">Yes</td>
                </tr>
              ) : null}
              {col.default ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">Default</td>
                  <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.default}</td>
                </tr>
              ) : null}
              {col.generated ? (
                <tr>
                  <td className="px-2.5 py-1.5 text-right text-muted-foreground whitespace-nowrap align-top">Generated</td>
                  <td className="px-2.5 py-1.5 font-mono text-left break-all">{col.generated}</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}

function MetadataSection({
  icon,
  title,
  count,
  open,
  onToggle,
  children,
}: {
  icon: React.ReactNode;
  title: string;
  count: number;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  if (count === 0) return null;
  return (
    <div className="rounded-lg border">
      <button
        type="button"
        className="flex w-full items-center gap-2 px-3 py-2 text-xs font-medium text-muted-foreground hover:bg-muted/50 cursor-pointer"
        onClick={onToggle}
      >
        {icon}
        <span>{title}</span>
        <Badge variant="secondary" className="text-[10px] px-1.5 py-0">{count}</Badge>
        <ChevronDown className={`ml-auto size-3 transition-transform ${open ? "rotate-180" : ""}`} />
      </button>
      {open ? <div className="border-t px-3 py-2">{children}</div> : null}
    </div>
  );
}

function TableMetadata({ table }: { table: TableInfo }) {
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({});
  const toggle = (key: string) =>
    setOpenSections((prev) => ({ ...prev, [key]: !prev[key] }));

  const hasMetadata =
    table.indexes.length > 0 ||
    table.foreign_keys.length > 0 ||
    table.unique_constraints.length > 0 ||
    table.check_constraints.length > 0;

  if (!hasMetadata) return null;

  return (
    <div className="space-y-2">
      <MetadataSection
        icon={<Key className="size-3" />}
        title="Indexes"
        count={table.indexes.length}
        open={openSections.indexes ?? false}
        onToggle={() => toggle("indexes")}
      >
        <div className="space-y-2">
          {table.indexes.map((idx) => (
            <div key={idx.name} className="flex flex-wrap items-center gap-1.5 text-xs">
              <span className="font-mono font-medium">{idx.name}</span>
              {idx.unique ? (
                <Badge variant="default" className="text-[10px] px-1 py-0">UNIQUE</Badge>
              ) : null}
              <Badge variant="secondary" className="font-mono text-[10px] px-1 py-0">
                {idx.method.toUpperCase()}
              </Badge>
              <span className="text-muted-foreground">on</span>
              <span className="font-mono">({idx.columns.join(", ")})</span>
              {idx.where ? (
                <span className="text-muted-foreground font-mono">WHERE {idx.where}</span>
              ) : null}
            </div>
          ))}
        </div>
      </MetadataSection>

      <MetadataSection
        icon={<Link className="size-3" />}
        title="Foreign Keys"
        count={table.foreign_keys.length}
        open={openSections.foreign_keys ?? false}
        onToggle={() => toggle("foreign_keys")}
      >
        <div className="space-y-2">
          {table.foreign_keys.map((fk) => (
            <div key={fk.name} className="flex flex-wrap items-center gap-1.5 text-xs">
              <span className="font-mono font-medium">{fk.name}</span>
              <span className="font-mono">({fk.columns.join(", ")})</span>
              <span className="text-muted-foreground">&rarr;</span>
              <span className="font-mono">
                {fk.references_schema !== "public" ? `${fk.references_schema}.` : ""}
                {fk.references_table}({fk.references_columns.join(", ")})
              </span>
              {fk.on_delete ? (
                <Badge variant="secondary" className="font-mono text-[10px] px-1 py-0">
                  ON DELETE {fk.on_delete.toUpperCase()}
                </Badge>
              ) : null}
              {fk.on_update ? (
                <Badge variant="secondary" className="font-mono text-[10px] px-1 py-0">
                  ON UPDATE {fk.on_update.toUpperCase()}
                </Badge>
              ) : null}
            </div>
          ))}
        </div>
      </MetadataSection>

      <MetadataSection
        icon={<ShieldCheck className="size-3" />}
        title="Unique Constraints"
        count={table.unique_constraints.length}
        open={openSections.unique_constraints ?? false}
        onToggle={() => toggle("unique_constraints")}
      >
        <div className="space-y-2">
          {table.unique_constraints.map((uc) => (
            <div key={uc.name} className="flex items-center gap-1.5 text-xs">
              <span className="font-mono font-medium">{uc.name}</span>
              <span className="font-mono">({uc.columns.join(", ")})</span>
            </div>
          ))}
        </div>
      </MetadataSection>

      <MetadataSection
        icon={<ShieldCheck className="size-3" />}
        title="Check Constraints"
        count={table.check_constraints.length}
        open={openSections.check_constraints ?? false}
        onToggle={() => toggle("check_constraints")}
      >
        <div className="space-y-2">
          {table.check_constraints.map((cc) => (
            <div key={cc.name} className="flex flex-wrap items-center gap-1.5 text-xs">
              <span className="font-mono font-medium">{cc.name}</span>
              <span className="font-mono text-muted-foreground">{cc.expression}</span>
            </div>
          ))}
        </div>
      </MetadataSection>
    </div>
  );
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
  const [updateDialogOpen, setUpdateDialogOpen] = useState(false);
  const [updateFormValues, setUpdateFormValues] = useState<Record<string, string>>({});

  const pkColumns = useMemo(
    () => table.columns.filter((c) => c.primary_key).map((c) => c.name),
    [table.columns],
  );

  const editableColumns = useMemo(
    () => table.columns.filter((c) => !c.primary_key),
    [table.columns],
  );

  const deleteRows = useDeleteRows(table.name);
  const updateRow = useUpdateRow(table.name);

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

  const openUpdateDialog = useCallback(() => {
    if (selectedRows.size !== 1) return;
    const rowId = Array.from(selectedRows)[0];
    const row = rows[Number(rowId)];
    const values: Record<string, string> = {};
    for (const col of editableColumns) {
      const itype = inputKindForColumn(col.type);
      values[col.name] = row[col.name] === null ? "" : toInputValue(row[col.name], itype);
    }
    setUpdateFormValues(values);
    setUpdateDialogOpen(true);
  }, [selectedRows, rows, editableColumns]);

  const getChangedValues = useCallback((): Record<string, unknown> => {
    if (selectedRows.size !== 1 || !updateDialogOpen) return {};
    const rowId = Array.from(selectedRows)[0];
    const row = rows[Number(rowId)];
    const values: Record<string, unknown> = {};
    for (const col of editableColumns) {
      const raw = updateFormValues[col.name] ?? "";
      const kind = inputKindForColumn(col.type);
      const originalRaw = row[col.name] === null ? "" : toInputValue(row[col.name], kind);
      if (raw !== originalRaw) {
        values[col.name] = raw === "" ? null : fromInputValue(raw, kind);
      }
    }
    return values;
  }, [selectedRows, rows, editableColumns, updateFormValues, updateDialogOpen]);

  const hasUpdateChanges = useMemo(
    () => Object.keys(getChangedValues()).length > 0,
    [getChangedValues],
  );

  const confirmUpdateRow = useCallback(() => {
    if (selectedRows.size !== 1 || pkColumns.length === 0) return;
    const rowId = Array.from(selectedRows)[0];
    const row = rows[Number(rowId)];
    const key: Record<string, unknown> = {};
    for (const pk of pkColumns) key[pk] = row[pk];

    const values = getChangedValues();

    if (Object.keys(values).length === 0) {
      setUpdateDialogOpen(false);
      return;
    }

    updateRow.mutate(
      { key, values },
      {
        onSuccess: () => {
          setSelectedRows(new Set());
          setUpdateDialogOpen(false);
          toast.success("Row updated");
        },
        onError: (error) => {
          toast.error("Failed to update row", { description: error.message });
        },
      },
    );
  }, [selectedRows, rows, pkColumns, getChangedValues, updateRow]);

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
      <div className="flex flex-wrap items-center gap-2 sm:gap-3">
        <Button variant="ghost" size="sm" onClick={onGoBack}>
          <ChevronLeft className="size-4" />
          <span className="hidden sm:inline">Back</span>
        </Button>
        <div className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" />
          <span className="text-sm font-medium">{table.name}</span>
          <Badge variant="secondary">
            {total.toLocaleString()} rows
          </Badge>
        </div>
        {selectedRows.size > 0 ? (
          <div className="ml-auto flex items-center gap-1.5 sm:gap-2">
            <span className="text-xs font-medium">
              {selectedRows.size} <span className="hidden sm:inline">{selectedRows.size === 1 ? "row" : "rows"} selected</span>
            </span>
            {pkColumns.length > 0 ? (
              <Button
                variant="outline"
                size="sm"
                onClick={openUpdateDialog}
                disabled={selectedRows.size !== 1}
              >
                <Pencil className="size-3" />
                <span className="hidden sm:inline">Update</span>
              </Button>
            ) : null}
            <Button variant="outline" size="sm" onClick={handleHideSelected}>
              <EyeOff className="size-3" />
              <span className="hidden sm:inline">Hide</span>
            </Button>
            {pkColumns.length > 0 ? (
              <Button
                variant="destructive"
                size="sm"
                onClick={() => setDeleteConfirmOpen(true)}
                disabled={deleteRows.isPending}
              >
                <Trash className="size-3" />
                <span className="hidden sm:inline">Delete</span>
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

      <div className={`overflow-auto rounded-lg border ${rows.length > 0 && rows.length <= 5 ? "min-h-96" : ""}`}>
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
                      <div className="flex flex-col gap-1">
                        <button
                          type="button"
                          className="flex items-center gap-1 cursor-pointer select-none"
                          onClick={header.column.getToggleSortingHandler()}
                        >
                          <span>{header.column.id}</span>
                          <SortIcon direction={header.column.getIsSorted()} />
                        </button>
                        <div className="flex items-center gap-1">
                          {(() => {
                            const col = table.columns.find((c) => c.name === header.column.id);
                            if (!col) return null;
                            return (
                              <>
                                <ColumnInfoDropdown col={col} />
                                {col.primary_key ? (
                                  <Badge variant="default" className="text-[10px] px-1 py-0">
                                    PK
                                  </Badge>
                                ) : null}
                              </>
                            );
                          })()}
                        </div>
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
                  className="px-3 py-32 text-center align-middle"
                >
                  <div className="flex flex-col items-center gap-2">
                    <Database className="size-10 text-muted-foreground/30" />
                    <p className="text-sm font-medium text-muted-foreground">No rows found</p>
                    <p className="text-xs text-muted-foreground/70">This table is empty.</p>
                  </div>
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

      <AlertDialog open={updateDialogOpen} onOpenChange={setUpdateDialogOpen}>
        <AlertDialogContent className="max-h-[80vh] overflow-y-auto">
          <AlertDialogHeader>
            <AlertDialogTitle>Update row</AlertDialogTitle>
            <AlertDialogDescription>
              Edit the fields below. Primary key columns cannot be changed.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="grid gap-3 py-4">
            {editableColumns.map((col) => {
              const kind = inputKindForColumn(col.type);
              const fieldId = `update-${col.name}`;
              const val = updateFormValues[col.name] ?? "";
              const setVal = (v: string) =>
                setUpdateFormValues((prev) => ({ ...prev, [col.name]: v }));

              return (
                <div key={col.name} className="grid gap-1.5">
                  <label className="text-sm font-medium" htmlFor={fieldId}>
                    {col.name}
                    <Badge variant="secondary" className="ml-2 font-mono text-[10px] px-1 py-0">
                      {col.type}
                    </Badge>
                  </label>
                  {kind === "boolean" ? (
                    <div className="flex items-center gap-2 h-9">
                      <Checkbox
                        id={fieldId}
                        checked={val === "true"}
                        onCheckedChange={(checked) => setVal(checked ? "true" : "false")}
                      />
                      <span className="text-sm text-muted-foreground">
                        {val === "true" ? "true" : "false"}
                      </span>
                    </div>
                  ) : kind === "json" ? (
                    <textarea
                      id={fieldId}
                      className="border-input focus-visible:border-ring focus-visible:ring-ring/50 dark:bg-input/30 min-h-20 w-full rounded-md border bg-transparent px-3 py-2 font-mono text-sm shadow-xs outline-none focus-visible:ring-[3px]"
                      value={val}
                      onChange={(e) => setVal(e.target.value)}
                      placeholder="NULL"
                      rows={4}
                    />
                  ) : (
                    <Input
                      id={fieldId}
                      type={kind}
                      step={kind === "datetime-local" ? "1" : kind === "number" && _FLOAT_TYPES.has(col.type.toLowerCase()) ? "any" : undefined}
                      value={val}
                      onChange={(e) => setVal(e.target.value)}
                      placeholder="NULL"
                    />
                  )}
                </div>
              );
            })}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={updateRow.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={confirmUpdateRow}
              disabled={updateRow.isPending || !hasUpdateChanges}
            >
              {updateRow.isPending ? <><Loader2 className="size-3 animate-spin" /> Updating</> : "Update"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

type DatabaseView = "tables" | "erd";

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

  const [view, setView] = useState<DatabaseView>("tables");

  const handleERDTableClick = useCallback(
    (name: string) => {
      setView("tables");
      selectTable(name);
    },
    [selectTable],
  );

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
          <CardContent className="pt-6 text-sm text-destructive">
            {error}
          </CardContent>
        </Card>
      ) : null}

      {!loading && !error && selectedTable === null ? (
        <>
          <div className="flex items-center justify-between">
            <Badge variant="secondary">{tables.length} tables</Badge>
            <div className="flex items-center gap-1 rounded-lg border p-0.5">
              <Button
                variant={view === "tables" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setView("tables")}
              >
                <LayoutGrid className="size-3.5" />
                Tables
              </Button>
              <Button
                variant={view === "erd" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setView("erd")}
              >
                <GitFork className="size-3.5" />
                ERD
              </Button>
            </div>
          </div>
          {view === "tables" ? (
            <TableList tables={tables} onSelect={selectTable} />
          ) : (
            <ERDiagram tables={tables} onTableClick={handleERDTableClick} />
          )}
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
