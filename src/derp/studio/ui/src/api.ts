import { useMutation, useQuery, useQueryClient, type UseMutationResult, type UseQueryResult } from "@tanstack/react-query";

export type ConfigSection = Record<string, unknown>;

export interface StudioConfig {
  database: ConfigSection;
  storage: ConfigSection | null;
  auth: ConfigSection | null;
  email: ConfigSection | null;
  kv: ConfigSection | null;
  [key: string]: unknown;
}

export function useConfig(): UseQueryResult<StudioConfig, Error> {
  return useQuery({
    queryKey: ["config"],
    queryFn: ({ signal }) => fetchConfig(signal),
  });
}

export async function fetchConfig(signal?: AbortSignal): Promise<StudioConfig> {
  const response = await fetch("/api/config", {
    headers: {
      Accept: "application/json",
    },
    signal,
  });

  if (!response.ok) {
    throw new Error(`Failed to load config: HTTP ${response.status}`);
  }

  return (await response.json()) as StudioConfig;
}

// --- Email ---

export interface EmailTemplatePreview {
  name: string;
  html: string;
}

export async function fetchEmailTemplates(
  signal?: AbortSignal,
): Promise<{ templates: EmailTemplatePreview[] }> {
  const response = await fetch("/api/email/templates", {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load email templates: HTTP ${response.status}`);
  }
  return (await response.json()) as { templates: EmailTemplatePreview[] };
}

// --- Database ---

export interface ColumnInfo {
  name: string;
  type: string;
  not_null: boolean;
  primary_key: boolean;
}

export interface TableInfo {
  name: string;
  schema: string;
  columns: ColumnInfo[];
  row_count: number;
}

export interface TableRowsResponse {
  rows: Record<string, unknown>[];
  total: number;
  limit: number;
  offset: number;
}

export async function fetchTables(
  signal?: AbortSignal,
): Promise<{ tables: TableInfo[] }> {
  const response = await fetch("/api/database/tables", {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load tables: HTTP ${response.status}`);
  }
  return (await response.json()) as { tables: TableInfo[] };
}

export async function fetchTableRows(
  table: string,
  limit: number = 50,
  offset: number = 0,
  signal?: AbortSignal,
): Promise<TableRowsResponse> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  const response = await fetch(
    `/api/database/tables/${encodeURIComponent(table)}/rows?${params}`,
    { headers: { Accept: "application/json" }, signal },
  );
  if (!response.ok) {
    throw new Error(`Failed to load rows: HTTP ${response.status}`);
  }
  return (await response.json()) as TableRowsResponse;
}

async function deleteTableRows(
  table: string,
  rows: Record<string, unknown>[],
): Promise<{ deleted: number }> {
  const response = await fetch(
    `/api/database/tables/${encodeURIComponent(table)}/delete-rows`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ rows }),
    },
  );
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to delete rows: ${text}`);
  }
  return (await response.json()) as { deleted: number };
}

export function useDeleteRows(table: string): UseMutationResult<{ deleted: number }, Error, Record<string, unknown>[]> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (rows: Record<string, unknown>[]) => deleteTableRows(table, rows),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tableRows", table] });
      queryClient.invalidateQueries({ queryKey: ["tables"] });
    },
  });
}

// --- Storage ---

export interface BucketInfo {
  name: string;
  creation_date: string;
}

export interface ObjectInfo {
  key: string;
  size: number;
  last_modified: string;
}

export interface ObjectsResponse {
  objects: ObjectInfo[];
  prefixes: string[];
}

export async function fetchBuckets(
  signal?: AbortSignal,
): Promise<{ buckets: BucketInfo[] }> {
  const response = await fetch("/api/storage/buckets", {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load buckets: HTTP ${response.status}`);
  }
  return (await response.json()) as { buckets: BucketInfo[] };
}

export async function fetchObjects(
  bucket: string,
  prefix: string = "",
  signal?: AbortSignal,
): Promise<ObjectsResponse> {
  const params = new URLSearchParams();
  if (prefix) params.set("prefix", prefix);
  const response = await fetch(
    `/api/storage/buckets/${encodeURIComponent(bucket)}/objects?${params}`,
    { headers: { Accept: "application/json" }, signal },
  );
  if (!response.ok) {
    throw new Error(`Failed to load objects: HTTP ${response.status}`);
  }
  return (await response.json()) as ObjectsResponse;
}
