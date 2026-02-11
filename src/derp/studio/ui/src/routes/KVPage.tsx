import { Key, Search, Trash2 } from "lucide-react";

import { useConfig, useDeleteKVKey, type KVKeyInfo } from "../api";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import { Card, CardContent } from "../components/ui/card";
import { Input } from "../components/ui/input";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "../components/ui/sheet";
import { Skeleton } from "../components/ui/skeleton";
import { useKV } from "../hooks/use-kv";

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatTTL(ttl: number | null): string {
  if (ttl == null || ttl < 0) return "No expiry";
  if (ttl < 60) return `${Math.round(ttl)}s`;
  if (ttl < 3600) return `${Math.round(ttl / 60)}m`;
  if (ttl < 86400) return `${(ttl / 3600).toFixed(1)}h`;
  return `${(ttl / 86400).toFixed(1)}d`;
}

function KeyDetailSheet({
  open,
  onOpenChange,
  keyInfo,
  loading,
  onDelete,
  deleting,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  keyInfo: KVKeyInfo | null;
  loading: boolean;
  onDelete: () => void;
  deleting: boolean;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="sm:max-w-lg overflow-y-auto">
        <SheetHeader>
          <SheetTitle className="break-all font-mono text-sm">
            {keyInfo?.key ?? ""}
          </SheetTitle>
          <SheetDescription>Key details</SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-4 px-4 pb-4">
          {loading ? (
            <div className="space-y-3">
              <Skeleton className="h-4 w-40" />
              <Skeleton className="h-4 w-32" />
              <Skeleton className="h-48 w-full" />
            </div>
          ) : keyInfo ? (
            <>
              <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5 text-sm">
                <span className="text-muted-foreground">Size</span>
                <span>{formatBytes(keyInfo.size)}</span>

                <span className="text-muted-foreground">TTL</span>
                <span>
                  <Badge variant={keyInfo.ttl != null && keyInfo.ttl >= 0 ? "secondary" : "outline"}>
                    {formatTTL(keyInfo.ttl)}
                  </Badge>
                </span>
              </div>

              <div>
                <span className="mb-1.5 block text-sm text-muted-foreground">
                  Value
                </span>
                <pre className="max-h-96 overflow-auto rounded-md border bg-muted/50 p-3 font-mono text-xs">
                  {keyInfo.value}
                </pre>
              </div>

              <Button
                variant="destructive"
                size="sm"
                className="w-fit"
                onClick={onDelete}
                disabled={deleting}
              >
                <Trash2 className="mr-2 size-4" />
                {deleting ? "Deleting..." : "Delete Key"}
              </Button>
            </>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function KeyList({
  keys,
  onSelect,
}: {
  keys: string[];
  onSelect: (key: string) => void;
}) {
  if (keys.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No keys found.</p>
    );
  }

  return (
    <div className="rounded-lg border">
      <div className="divide-y">
        {keys.map((key) => (
          <button
            key={key}
            type="button"
            className="flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm hover:bg-muted/50"
            onClick={() => onSelect(key)}
          >
            <Key className="size-4 text-muted-foreground" />
            <span className="flex-1 truncate font-mono text-xs">{key}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

export function KVPage(): JSX.Element {
  const { data: config, isLoading: configLoading } = useConfig();
  const isConfigured = config?.kv != null;
  const {
    keys,
    selectedKey,
    selectKey,
    deselectKey,
    searchPrefix,
    setSearchPrefix,
    loading,
    error,
    keyInfo,
    keyInfoLoading,
  } = useKV(isConfigured);

  const deleteMutation = useDeleteKVKey();

  const handleDelete = () => {
    if (selectedKey == null) return;
    deleteMutation.mutate(selectedKey, {
      onSuccess: () => deselectKey(),
    });
  };

  return (
    <div className="flex flex-1 flex-col gap-6 p-6 md:p-10">
      {!isConfigured && !configLoading ? (
        <Card>
          <CardContent className="pt-6 text-sm text-muted-foreground">
            KV is not configured. Add a{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              [kv]
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
        <>
          <div className="flex items-center gap-3">
            <div className="relative flex-1 max-w-sm">
              <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Filter by prefix..."
                value={searchPrefix}
                onChange={(e) => setSearchPrefix(e.target.value)}
                className="pl-9"
              />
            </div>
            <Badge variant="secondary">{keys.length} keys</Badge>
          </div>

          {error ? (
            <Card className="border-destructive/40">
              <CardContent className="pt-6 text-sm text-destructive">
                {error}
              </CardContent>
            </Card>
          ) : null}

          {!error && loading ? (
            <div className="rounded-lg border">
              <div className="divide-y">
                {Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="flex items-center gap-3 px-4 py-2.5">
                    <Skeleton className="size-4 rounded" />
                    <Skeleton className="h-4 flex-1 max-w-64" />
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          {!error && !loading ? (
            <KeyList keys={keys} onSelect={selectKey} />
          ) : null}
        </>
      ) : null}

      {selectedKey != null ? (
        <KeyDetailSheet
          open={selectedKey != null}
          onOpenChange={(open) => {
            if (!open) deselectKey();
          }}
          keyInfo={keyInfo}
          loading={keyInfoLoading}
          onDelete={handleDelete}
          deleting={deleteMutation.isPending}
        />
      ) : null}
    </div>
  );
}
