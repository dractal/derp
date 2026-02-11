import {
    Package,
    Receipt,
    RefreshCw,
    Users,
    Zap
} from "lucide-react";

import type {
    StripeCharge,
    StripeCustomer,
    StripeInvoice,
    StripeProduct,
    StripeSubscription,
} from "../api";
import { useConfig } from "../api";
import { Badge } from "../components/ui/badge";
import { Card, CardContent } from "../components/ui/card";
import { Skeleton } from "../components/ui/skeleton";
import {
    Tabs,
    TabsContent,
    TabsList,
    TabsTrigger,
} from "../components/ui/tabs";
import { usePayments, type PaymentsTab } from "../hooks/use-payments";

function formatCurrency(amount: number, currency: string): string {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: currency.toUpperCase(),
  }).format(amount / 100);
}

function formatDate(timestamp: number): string {
  return new Date(timestamp * 1000).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function StatusBadge({ status }: { status: string }) {
  const variant =
    status === "active" || status === "paid" || status === "succeeded"
      ? "default"
      : status === "past_due" || status === "pending" || status === "open"
        ? "secondary"
        : "destructive";

  return <Badge variant={variant}>{status}</Badge>;
}

function LoadingSkeleton() {
  return (
    <div className="rounded-lg border">
      <div className="divide-y">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="flex items-center gap-4 px-4 py-3">
            <Skeleton className="h-4 w-32" />
            <Skeleton className="h-4 w-40" />
            <Skeleton className="h-4 w-24" />
            <Skeleton className="h-4 w-20" />
          </div>
        ))}
      </div>
    </div>
  );
}

function EmptyState({ resource }: { resource: string }) {
  return (
    <p className="text-sm text-muted-foreground">No {resource} found.</p>
  );
}

function CustomersTable({ customers }: { customers: StripeCustomer[] }) {
  if (customers.length === 0) return <EmptyState resource="customers" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1fr_1.5fr_1fr_0.8fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>ID</span>
        <span>Email</span>
        <span>Name</span>
        <span>Created</span>
      </div>
      <div className="divide-y">
        {customers.map((c) => (
          <div
            key={c.id}
            className="grid grid-cols-[1fr_1.5fr_1fr_0.8fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-mono text-xs">{c.id}</span>
            <span className="truncate">{c.email ?? "—"}</span>
            <span className="truncate">{c.name ?? "—"}</span>
            <span className="text-muted-foreground">
              {formatDate(c.created)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ProductsTable({ products }: { products: StripeProduct[] }) {
  if (products.length === 0) return <EmptyState resource="products" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1.5fr_2fr_1fr_0.8fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>Name</span>
        <span>Description</span>
        <span>Price</span>
        <span>Status</span>
      </div>
      <div className="divide-y">
        {products.map((p) => (
          <div
            key={p.id}
            className="grid grid-cols-[1.5fr_2fr_1fr_0.8fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-medium">{p.name}</span>
            <span className="truncate text-muted-foreground">
              {p.description ?? "—"}
            </span>
            <span>
              {p.default_price?.unit_amount != null
                ? formatCurrency(
                    p.default_price.unit_amount,
                    p.default_price.currency,
                  )
                : "—"}
              {p.default_price?.recurring ? (
                <span className="text-xs text-muted-foreground">
                  /{p.default_price.recurring.interval}
                </span>
              ) : null}
            </span>
            <span>
              <StatusBadge status={p.active ? "active" : "inactive"} />
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function SubscriptionsTable({
  subscriptions,
}: {
  subscriptions: StripeSubscription[];
}) {
  if (subscriptions.length === 0)
    return <EmptyState resource="subscriptions" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1fr_1fr_0.8fr_1fr_0.8fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>ID</span>
        <span>Customer</span>
        <span>Status</span>
        <span>Period End</span>
        <span>Created</span>
      </div>
      <div className="divide-y">
        {subscriptions.map((s) => (
          <div
            key={s.id}
            className="grid grid-cols-[1fr_1fr_0.8fr_1fr_0.8fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-mono text-xs">{s.id}</span>
            <span className="truncate font-mono text-xs">
              {typeof s.customer === "string" ? s.customer : "—"}
            </span>
            <span>
              <StatusBadge status={s.status} />
            </span>
            <span className="text-muted-foreground">
              {formatDate(s.current_period_end)}
            </span>
            <span className="text-muted-foreground">
              {formatDate(s.created)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function InvoicesTable({ invoices }: { invoices: StripeInvoice[] }) {
  if (invoices.length === 0) return <EmptyState resource="invoices" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1fr_1fr_1fr_0.8fr_0.8fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>Number</span>
        <span>Customer</span>
        <span>Amount</span>
        <span>Status</span>
        <span>Created</span>
      </div>
      <div className="divide-y">
        {invoices.map((inv) => (
          <div
            key={inv.id}
            className="grid grid-cols-[1fr_1fr_1fr_0.8fr_0.8fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-mono text-xs">
              {inv.number ?? inv.id}
            </span>
            <span className="truncate font-mono text-xs">
              {typeof inv.customer === "string" ? inv.customer : "—"}
            </span>
            <span>
              {formatCurrency(inv.amount_due, inv.currency)}
            </span>
            <span>
              {inv.status ? <StatusBadge status={inv.status} /> : "—"}
            </span>
            <span className="text-muted-foreground">
              {formatDate(inv.created)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ChargesTable({ charges }: { charges: StripeCharge[] }) {
  if (charges.length === 0) return <EmptyState resource="charges" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1fr_1fr_0.8fr_1fr_0.8fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>ID</span>
        <span>Amount</span>
        <span>Status</span>
        <span>Description</span>
        <span>Created</span>
      </div>
      <div className="divide-y">
        {charges.map((ch) => (
          <div
            key={ch.id}
            className="grid grid-cols-[1fr_1fr_0.8fr_1fr_0.8fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-mono text-xs">{ch.id}</span>
            <span>{formatCurrency(ch.amount, ch.currency)}</span>
            <span>
              <StatusBadge status={ch.status} />
            </span>
            <span className="truncate text-muted-foreground">
              {ch.description ?? "—"}
            </span>
            <span className="text-muted-foreground">
              {formatDate(ch.created)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

const TAB_CONFIG: {
  value: PaymentsTab;
  label: string;
  icon: React.ElementType;
}[] = [
  { value: "customers", label: "Customers", icon: Users },
  { value: "products", label: "Products", icon: Package },
  { value: "subscriptions", label: "Subscriptions", icon: RefreshCw },
  { value: "invoices", label: "Invoices", icon: Receipt },
  { value: "charges", label: "Charges", icon: Zap },
];

export function PaymentsPage(): JSX.Element {
  const { data: config, isLoading: configLoading } = useConfig();
  const isConfigured = config?.payments != null;
  const {
    tab,
    selectTab,
    customers,
    products,
    subscriptions,
    invoices,
    charges,
    loading,
    error,
  } = usePayments(isConfigured);

  return (
    <div className="flex flex-1 flex-col gap-6 p-6 md:p-10">
      {!isConfigured && !configLoading ? (
        <Card>
          <CardContent className="pt-6 text-sm text-muted-foreground">
            Payments is not configured. Add a{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              [payments]
            </code>{" "}
            section to your{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              derp.toml
            </code>{" "}
            to get started.
          </CardContent>
        </Card>
      ) : null}

      {(configLoading || isConfigured) ? (
        <Tabs
          value={tab}
          onValueChange={(v) => selectTab(v as PaymentsTab)}
        >
          <TabsList>
            {TAB_CONFIG.map(({ value, label, icon: Icon }) => (
              <TabsTrigger key={value} value={value}>
                <Icon className="mr-2 size-4" />
                {label}
              </TabsTrigger>
            ))}
          </TabsList>

          {error ? (
            <Card className="mt-4 border-destructive/40">
              <CardContent className="pt-6 text-sm text-destructive">
                {error}
              </CardContent>
            </Card>
          ) : null}

          {!error ? (
            <>
              <TabsContent value="customers">
                {loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {customers.length} customers
                      </Badge>
                    </div>
                    <CustomersTable customers={customers} />
                  </>
                )}
              </TabsContent>

              <TabsContent value="products">
                {loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {products.length} products
                      </Badge>
                    </div>
                    <ProductsTable products={products} />
                  </>
                )}
              </TabsContent>

              <TabsContent value="subscriptions">
                {loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {subscriptions.length} subscriptions
                      </Badge>
                    </div>
                    <SubscriptionsTable subscriptions={subscriptions} />
                  </>
                )}
              </TabsContent>

              <TabsContent value="invoices">
                {loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {invoices.length} invoices
                      </Badge>
                    </div>
                    <InvoicesTable invoices={invoices} />
                  </>
                )}
              </TabsContent>

              <TabsContent value="charges">
                {loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {charges.length} charges
                      </Badge>
                    </div>
                    <ChargesTable charges={charges} />
                  </>
                )}
              </TabsContent>
            </>
          ) : null}
        </Tabs>
      ) : null}
    </div>
  );
}
