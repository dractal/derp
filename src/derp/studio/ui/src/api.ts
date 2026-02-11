import { useMutation, useQuery, useQueryClient, type UseMutationResult, type UseQueryResult } from "@tanstack/react-query";

export type ConfigSection = Record<string, unknown>;

export interface StudioConfig {
  database: ConfigSection;
  storage: ConfigSection | null;
  auth: ConfigSection | null;
  email: ConfigSection | null;
  kv: ConfigSection | null;
  payments: ConfigSection | null;
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
  unique: boolean;
  default: string | null;
  generated: string | null;
  nullable: boolean;
}

export interface IndexInfo {
  name: string;
  columns: string[];
  unique: boolean;
  method: string;
  where: string | null;
}

export interface ForeignKeyInfo {
  name: string;
  columns: string[];
  references_table: string;
  references_columns: string[];
  references_schema: string;
  on_delete: string | null;
  on_update: string | null;
}

export interface UniqueConstraintInfo {
  name: string;
  columns: string[];
}

export interface CheckConstraintInfo {
  name: string;
  expression: string;
}

export interface PrimaryKeyInfo {
  name: string | null;
  columns: string[];
}

export interface TableInfo {
  name: string;
  schema: string;
  columns: ColumnInfo[];
  row_count: number;
  indexes: IndexInfo[];
  foreign_keys: ForeignKeyInfo[];
  unique_constraints: UniqueConstraintInfo[];
  check_constraints: CheckConstraintInfo[];
  primary_key: PrimaryKeyInfo | null;
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

interface UpdateRowInput {
  key: Record<string, unknown>;
  values: Record<string, unknown>;
}

async function updateTableRow(
  table: string,
  input: UpdateRowInput,
): Promise<{ updated: number }> {
  const response = await fetch(
    `/api/database/tables/${encodeURIComponent(table)}/update-row`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(input),
    },
  );
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to update row: ${text}`);
  }
  return (await response.json()) as { updated: number };
}

export function useUpdateRow(table: string): UseMutationResult<{ updated: number }, Error, UpdateRowInput> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: UpdateRowInput) => updateTableRow(table, input),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tableRows", table] });
      queryClient.invalidateQueries({ queryKey: ["tables"] });
    },
  });
}

// --- KV ---

export interface KVKeyInfo {
  key: string;
  value: string;
  ttl: number | null;
  size: number;
}

export async function fetchKVKeys(
  prefix?: string,
  limit?: number,
  signal?: AbortSignal,
): Promise<{ keys: string[] }> {
  const params = new URLSearchParams();
  if (prefix) params.set("prefix", prefix);
  if (limit != null) params.set("limit", String(limit));
  const response = await fetch(`/api/kv/keys?${params}`, {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load KV keys: HTTP ${response.status}`);
  }
  return (await response.json()) as { keys: string[] };
}

export async function fetchKVKeyInfo(
  key: string,
  signal?: AbortSignal,
): Promise<KVKeyInfo> {
  const params = new URLSearchParams({ key });
  const response = await fetch(`/api/kv/keys/info?${params}`, {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load KV key info: HTTP ${response.status}`);
  }
  return (await response.json()) as KVKeyInfo;
}

async function deleteKVKey(key: string): Promise<{ deleted: boolean }> {
  const response = await fetch("/api/kv/keys", {
    method: "DELETE",
    headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ key }),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Failed to delete KV key: ${text}`);
  }
  return (await response.json()) as { deleted: boolean };
}

export function useDeleteKVKey(): UseMutationResult<{ deleted: boolean }, Error, string> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (key: string) => deleteKVKey(key),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["kv", "keys"] });
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

export interface ObjectDetail {
  content_type: string;
  content_length: number;
  last_modified: string;
  etag: string;
  metadata: Record<string, string>;
}

export async function fetchObjectInfo(
  bucket: string,
  key: string,
  signal?: AbortSignal,
): Promise<ObjectDetail> {
  const params = new URLSearchParams({ key });
  const response = await fetch(
    `/api/storage/buckets/${encodeURIComponent(bucket)}/objects/info?${params}`,
    { headers: { Accept: "application/json" }, signal },
  );
  if (!response.ok) {
    throw new Error(`Failed to load object info: HTTP ${response.status}`);
  }
  return (await response.json()) as ObjectDetail;
}

export function objectContentUrl(bucket: string, key: string): string {
  const params = new URLSearchParams({ key });
  return `/api/storage/buckets/${encodeURIComponent(bucket)}/objects/content?${params}`;
}

// --- Auth ---

export interface AuthUser {
  id: string;
  email: string;
  provider: string;
  is_active: boolean;
  email_confirmed_at: string | null;
  last_sign_in_at: string | null;
  created_at: string;
}

export interface AuthSessionInfo {
  id: string;
  user_id: string;
  user_agent: string | null;
  ip_address: string | null;
  created_at: string;
  not_after: string;
}

export async function fetchAuthUsers(
  signal?: AbortSignal,
): Promise<{ users: AuthUser[] }> {
  const response = await fetch("/api/auth/users", {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load auth users: HTTP ${response.status}`);
  }
  return (await response.json()) as { users: AuthUser[] };
}

export async function fetchAuthSessions(
  signal?: AbortSignal,
): Promise<{ sessions: AuthSessionInfo[] }> {
  const response = await fetch("/api/auth/sessions", {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load auth sessions: HTTP ${response.status}`);
  }
  return (await response.json()) as { sessions: AuthSessionInfo[] };
}

// --- Payments ---

export interface StripeCustomer {
  id: string;
  email: string | null;
  name: string | null;
  phone: string | null;
  created: number;
  metadata: Record<string, string>;
}

export interface StripeProduct {
  id: string;
  name: string;
  description: string | null;
  active: boolean;
  created: number;
  default_price: {
    id: string;
    unit_amount: number | null;
    currency: string;
    recurring: { interval: string; interval_count: number } | null;
  } | null;
}

export interface StripeSubscription {
  id: string;
  customer: string;
  status: string;
  current_period_start: number;
  current_period_end: number;
  created: number;
  items: {
    data: {
      id: string;
      price: {
        id: string;
        unit_amount: number | null;
        currency: string;
        recurring: { interval: string; interval_count: number } | null;
        product: string;
      };
      quantity: number;
    }[];
  };
}

export interface StripeInvoice {
  id: string;
  number: string | null;
  customer: string;
  amount_due: number;
  amount_paid: number;
  currency: string;
  status: string | null;
  created: number;
  hosted_invoice_url: string | null;
}

export interface StripeCharge {
  id: string;
  amount: number;
  currency: string;
  status: string;
  customer: string | null;
  description: string | null;
  created: number;
  payment_intent: string | null;
}

export interface StripeListResponse<T> {
  data: T[];
  has_more: boolean;
}

async function fetchStripeList<T>(
  resource: string,
  limit: number = 25,
  startingAfter?: string,
  signal?: AbortSignal,
): Promise<StripeListResponse<T>> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  if (startingAfter) params.set("starting_after", startingAfter);
  const response = await fetch(`/api/payments/${resource}?${params}`, {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Failed to load ${resource}: HTTP ${response.status}`);
  }
  return (await response.json()) as StripeListResponse<T>;
}

export async function fetchCustomers(
  limit?: number,
  startingAfter?: string,
  signal?: AbortSignal,
): Promise<StripeListResponse<StripeCustomer>> {
  return fetchStripeList<StripeCustomer>("customers", limit, startingAfter, signal);
}

export async function fetchProducts(
  limit?: number,
  startingAfter?: string,
  signal?: AbortSignal,
): Promise<StripeListResponse<StripeProduct>> {
  return fetchStripeList<StripeProduct>("products", limit, startingAfter, signal);
}

export async function fetchSubscriptions(
  limit?: number,
  startingAfter?: string,
  signal?: AbortSignal,
): Promise<StripeListResponse<StripeSubscription>> {
  return fetchStripeList<StripeSubscription>("subscriptions", limit, startingAfter, signal);
}

export async function fetchInvoices(
  limit?: number,
  startingAfter?: string,
  signal?: AbortSignal,
): Promise<StripeListResponse<StripeInvoice>> {
  return fetchStripeList<StripeInvoice>("invoices", limit, startingAfter, signal);
}

export async function fetchCharges(
  limit?: number,
  startingAfter?: string,
  signal?: AbortSignal,
): Promise<StripeListResponse<StripeCharge>> {
  return fetchStripeList<StripeCharge>("charges", limit, startingAfter, signal);
}
