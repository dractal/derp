import { KeyRound, Settings, Users } from "lucide-react";

import type { AuthSessionInfo, AuthUser } from "../api";
import { useConfig } from "../api";
import { JsonViewer } from "../components/json-viewer";
import { Badge } from "../components/ui/badge";
import { Card, CardContent } from "../components/ui/card";
import { Skeleton } from "../components/ui/skeleton";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "../components/ui/tabs";
import { useAuth, type AuthTab } from "../hooks/use-auth";

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
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

function ProviderBadge({ provider }: { provider: string }) {
  const variant =
    provider === "email"
      ? "default"
      : provider === "magic_link"
        ? "secondary"
        : "outline";

  return <Badge variant={variant}>{provider}</Badge>;
}

function UsersTable({ users }: { users: AuthUser[] }) {
  if (users.length === 0) return <EmptyState resource="users" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1fr_1.5fr_0.8fr_0.6fr_0.6fr_1fr_1fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>ID</span>
        <span>Email</span>
        <span>Provider</span>
        <span>Active</span>
        <span>Confirmed</span>
        <span>Last Sign In</span>
        <span>Created</span>
      </div>
      <div className="divide-y">
        {users.map((u) => (
          <div
            key={u.id}
            className="grid grid-cols-[1fr_1.5fr_0.8fr_0.6fr_0.6fr_1fr_1fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-mono text-xs">{u.id}</span>
            <span className="truncate">{u.email}</span>
            <span>
              <ProviderBadge provider={u.provider} />
            </span>
            <span>
              <Badge variant={u.is_active ? "default" : "destructive"}>
                {u.is_active ? "yes" : "no"}
              </Badge>
            </span>
            <span className="text-muted-foreground text-xs">
              {u.email_confirmed_at ? formatDate(u.email_confirmed_at) : "—"}
            </span>
            <span className="text-muted-foreground text-xs">
              {u.last_sign_in_at ? formatDate(u.last_sign_in_at) : "—"}
            </span>
            <span className="text-muted-foreground text-xs">
              {formatDate(u.created_at)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function SessionsTable({ sessions }: { sessions: AuthSessionInfo[] }) {
  if (sessions.length === 0) return <EmptyState resource="sessions" />;

  return (
    <div className="rounded-lg border">
      <div className="grid grid-cols-[1fr_1fr_1.5fr_0.8fr_1fr_1fr] gap-4 border-b px-4 py-2.5 text-xs font-medium text-muted-foreground">
        <span>Session ID</span>
        <span>User ID</span>
        <span>User Agent</span>
        <span>IP Address</span>
        <span>Created</span>
        <span>Expires</span>
      </div>
      <div className="divide-y">
        {sessions.map((s) => (
          <div
            key={s.id}
            className="grid grid-cols-[1fr_1fr_1.5fr_0.8fr_1fr_1fr] gap-4 px-4 py-2.5 text-sm"
          >
            <span className="truncate font-mono text-xs">{s.id}</span>
            <span className="truncate font-mono text-xs">{s.user_id}</span>
            <span className="truncate text-muted-foreground text-xs">
              {s.user_agent ?? "—"}
            </span>
            <span className="truncate font-mono text-xs">
              {s.ip_address ?? "—"}
            </span>
            <span className="text-muted-foreground text-xs">
              {formatDate(s.created_at)}
            </span>
            <span className="text-muted-foreground text-xs">
              {formatDate(s.not_after)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

const TAB_CONFIG: {
  value: AuthTab;
  label: string;
  icon: React.ElementType;
}[] = [
    { value: "users", label: "Users", icon: Users },
    { value: "sessions", label: "Sessions", icon: KeyRound },
    { value: "config", label: "Config", icon: Settings },
  ];

export function AuthPage(): JSX.Element {
  const { data: config, isLoading: configLoading } = useConfig();
  const isConfigured = config?.auth != null;
  const { tab, selectTab, users, sessions, loading, error } = useAuth(isConfigured);

  return (
    <div className="flex flex-1 flex-col gap-6 overflow-y-auto p-6 md:p-10">
      {!isConfigured && !configLoading ? (
        <Card>
          <CardContent className="pt-6 text-sm text-muted-foreground">
            Authentication is not configured. Add an{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              [auth]
            </code>{" "}
            section to your{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              derp.toml
            </code>{" "}
            to get started.
          </CardContent>
        </Card>
      ) : null}

      {configLoading || isConfigured ? (
        <Tabs
          value={tab}
          onValueChange={(v) => selectTab(v as AuthTab)}
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
              <TabsContent value="users">
                {configLoading || loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {users.length} users
                      </Badge>
                    </div>
                    <UsersTable users={users} />
                  </>
                )}
              </TabsContent>

              <TabsContent value="sessions">
                {loading ? (
                  <LoadingSkeleton />
                ) : (
                  <>
                    <div className="mb-3">
                      <Badge variant="secondary">
                        {sessions.length} sessions
                      </Badge>
                    </div>
                    <SessionsTable sessions={sessions} />
                  </>
                )}
              </TabsContent>

              <TabsContent value="config">
                {config?.auth ? (
                  <div className="rounded-lg border p-4">
                    <JsonViewer
                      data={config.auth as Record<string, unknown>}
                    />
                  </div>
                ) : configLoading ? (
                  <LoadingSkeleton />
                ) : null}
              </TabsContent>
            </>
          ) : null}
        </Tabs>
      ) : null}
    </div>
  );
}
