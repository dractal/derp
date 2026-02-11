import { ChevronLeft, ChevronRight, File, Folder, HardDrive } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  useConfig,
  objectContentUrl,
  type BucketInfo,
  type ObjectDetail,
  type ObjectInfo,
} from "../api";
import { Badge } from "../components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "../components/ui/sheet";
import { Skeleton } from "../components/ui/skeleton";
import { useStorage } from "../hooks/use-storage";

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function extractName(keyOrPrefix: string, currentPrefix: string): string {
  return keyOrPrefix.slice(currentPrefix.length).replace(/\/$/, "");
}

function mimeCategory(
  contentType: string,
): "image" | "video" | "audio" | "pdf" | "text" | "json" | null {
  const ct = contentType.toLowerCase();
  if (ct.startsWith("image/")) return "image";
  if (ct.startsWith("video/")) return "video";
  if (ct.startsWith("audio/")) return "audio";
  if (ct === "application/pdf") return "pdf";
  if (ct === "application/json" || ct.endsWith("+json")) return "json";
  if (
    ct.startsWith("text/") ||
    ct === "application/xml" ||
    ct.endsWith("+xml") ||
    ct === "application/javascript"
  )
    return "text";
  return null;
}

function ObjectPreview({
  bucket,
  objectKey,
  contentType,
}: {
  bucket: string;
  objectKey: string;
  contentType: string;
}) {
  const url = objectContentUrl(bucket, objectKey);
  const category = mimeCategory(contentType);

  if (category === "image") {
    return (
      <img
        src={url}
        alt={objectKey}
        className="max-h-80 w-full rounded-md border object-contain"
      />
    );
  }

  if (category === "video") {
    return (
      <video
        src={url}
        controls
        className="max-h-80 w-full rounded-md border"
      />
    );
  }

  if (category === "audio") {
    return <audio src={url} controls className="w-full" />;
  }

  if (category === "pdf") {
    return (
      <iframe
        src={url}
        title={objectKey}
        className="h-80 w-full rounded-md border"
      />
    );
  }

  if (category === "text" || category === "json") {
    return (
      <iframe
        src={url}
        title={objectKey}
        className="h-80 w-full rounded-md border bg-white font-mono text-xs"
        sandbox=""
      />
    );
  }

  return (
    <p className="text-sm text-muted-foreground">
      Preview not available for this file type.
    </p>
  );
}

function ObjectDetailSheet({
  open,
  onOpenChange,
  bucket,
  object,
  detail,
  loading,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  bucket: string;
  object: ObjectInfo;
  detail: ObjectDetail | null;
  loading: boolean;
}) {
  const fileName = object.key.split("/").pop() ?? object.key;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="sm:max-w-lg overflow-y-auto">
        <SheetHeader>
          <SheetTitle className="break-all text-sm">{fileName}</SheetTitle>
          <SheetDescription className="break-all">
            {object.key}
          </SheetDescription>
        </SheetHeader>

        <div className="flex flex-col gap-4 px-4 pb-4">
          {loading ? (
            <div className="space-y-3">
              <Skeleton className="h-4 w-40" />
              <Skeleton className="h-4 w-32" />
              <Skeleton className="h-4 w-36" />
              <Skeleton className="h-48 w-full" />
            </div>
          ) : detail ? (
            <>
              <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1.5 text-sm">
                <span className="text-muted-foreground">Content Type</span>
                <span className="font-mono text-xs">
                  {detail.content_type}
                </span>

                <span className="text-muted-foreground">Size</span>
                <span>{formatBytes(detail.content_length)}</span>

                <span className="text-muted-foreground">Last Modified</span>
                <span>{formatDate(detail.last_modified)}</span>

                <span className="text-muted-foreground">ETag</span>
                <span className="truncate font-mono text-xs">
                  {detail.etag}
                </span>

                {Object.entries(detail.metadata).map(([k, v]) => (
                  <>
                    <span key={`k-${k}`} className="text-muted-foreground">
                      {k}
                    </span>
                    <span key={`v-${k}`} className="break-all">
                      {v}
                    </span>
                  </>
                ))}
              </div>

              <ObjectPreview
                bucket={bucket}
                objectKey={object.key}
                contentType={detail.content_type}
              />
            </>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  );
}

function BucketList({
  buckets,
  onSelect,
}: {
  buckets: BucketInfo[];
  onSelect: (name: string) => void;
}) {
  if (buckets.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">No buckets found.</p>
    );
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {buckets.map((bucket) => (
        <Card
          key={bucket.name}
          className="cursor-pointer transition-colors hover:bg-muted/50"
          onClick={() => onSelect(bucket.name)}
        >
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2 text-sm font-medium">
              <HardDrive className="size-4 text-muted-foreground" />
              {bucket.name}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-xs text-muted-foreground">
              Created {formatDate(bucket.creation_date)}
            </p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function PathBreadcrumb({
  bucket,
  prefix,
  onNavigate,
}: {
  bucket: string;
  prefix: string;
  onNavigate: (prefix: string) => void;
}) {
  const segments = prefix
    ? prefix
      .replace(/\/$/, "")
      .split("/")
    : [];

  return (
    <nav className="flex items-center gap-1 text-sm">
      <Button
        variant="ghost"
        size="sm"
        onClick={() => onNavigate("")}
      >
        {bucket}
      </Button>
      {segments.map((seg, i) => {
        const path = segments.slice(0, i + 1).join("/") + "/";
        const isLast = i === segments.length - 1;
        return (
          <span key={path} className="flex items-center gap-1">
            <ChevronRight className="size-3 text-muted-foreground" />
            <Button
              type="button"
              variant="ghost"
              size="sm"
              disabled={isLast}
              onClick={() => onNavigate(path)}
            >
              {seg}
            </Button>
          </span>
        );
      })}
    </nav>
  );
}

function ObjectBrowser({
  bucket,
  prefix,
  prefixes,
  objects,
  onNavigateToPrefix,
  onNavigateUp,
  onSelectObject,
}: {
  bucket: string;
  prefix: string;
  prefixes: string[];
  objects: ObjectInfo[];
  onNavigateToPrefix: (prefix: string) => void;
  onNavigateUp: () => void;
  onSelectObject: (obj: ObjectInfo) => void;
}) {
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" onClick={onNavigateUp}>
          <ChevronLeft className="size-4" />
          Back
        </Button>
        <PathBreadcrumb
          bucket={bucket}
          prefix={prefix}
          onNavigate={onNavigateToPrefix}
        />
      </div>

      <div className="rounded-lg border">
        {prefixes.length === 0 && objects.length === 0 ? (
          <div className="p-4 text-sm text-muted-foreground">
            This location is empty.
          </div>
        ) : (
          <div className="divide-y">
            {prefixes.map((p) => (
              <button
                key={p}
                type="button"
                className="flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm hover:bg-muted/50"
                onClick={() => onNavigateToPrefix(p)}
              >
                <Folder className="size-4 text-muted-foreground" />
                <span className="flex-1 font-medium">
                  {extractName(p, prefix)}
                </span>
              </button>
            ))}
            {objects.map((obj) => (
              <button
                key={obj.key}
                type="button"
                className="flex w-full items-center gap-3 px-4 py-2.5 text-left text-sm hover:bg-muted/50"
                onClick={() => onSelectObject(obj)}
              >
                <File className="size-4 text-muted-foreground" />
                <span className="flex-1">{extractName(obj.key, prefix)}</span>
                <span className="text-xs text-muted-foreground">
                  {formatBytes(obj.size)}
                </span>
                <span className="text-xs text-muted-foreground">
                  {formatDate(obj.last_modified)}
                </span>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export function StoragePage(): JSX.Element {
  const { data: config, isLoading } = useConfig();
  const isConfigured = config?.storage != null;
  const {
    buckets,
    selectedBucket,
    prefix,
    prefixes,
    objects,
    loading,
    error,
    selectBucket,
    navigateToPrefix,
    navigateUp,
    selectedObject,
    objectDetail,
    objectDetailLoading,
    selectObject,
    deselectObject,
  } = useStorage(isConfigured);

  return (
    <div className="flex flex-1 flex-col gap-6 p-6 md:p-10">
      {!isConfigured && !isLoading ? (
        <Card>
          <CardContent className="pt-6 text-sm text-muted-foreground">
            Storage is not configured. Add a{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              [storage]
            </code>{" "}
            section to your{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              derp.toml
            </code>{" "}
            to get started.
          </CardContent>
        </Card>
      ) : null}

      {(isLoading || isConfigured) && loading && selectedBucket === null ? (
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
                <Skeleton className="h-3 w-36" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : null}

      {isConfigured && error ? (
        <Card className="border-destructive/40">
          <CardContent className="pt-6 text-sm text-destructive">
            {error}
          </CardContent>
        </Card>
      ) : null}

      {isConfigured && !loading && !error && selectedBucket === null ? (
        <>
          <div className="flex items-center gap-2">
            <Badge variant="secondary">{buckets.length} buckets</Badge>
          </div>
          <BucketList buckets={buckets} onSelect={selectBucket} />
        </>
      ) : null}

      {isConfigured && !error && selectedBucket !== null ? (
        loading ? (
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <Skeleton className="h-8 w-16" />
              <Skeleton className="h-4 w-32" />
            </div>
            <div className="rounded-lg border divide-y">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="flex items-center gap-3 px-4 py-2.5">
                  <Skeleton className="size-4 rounded" />
                  <Skeleton className="h-4 flex-1 max-w-48" />
                  <Skeleton className="h-3 w-12" />
                  <Skeleton className="h-3 w-24" />
                </div>
              ))}
            </div>
          </div>
        ) : (
          <ObjectBrowser
            bucket={selectedBucket}
            prefix={prefix}
            prefixes={prefixes}
            objects={objects}
            onNavigateToPrefix={navigateToPrefix}
            onNavigateUp={navigateUp}
            onSelectObject={selectObject}
          />
        )
      ) : null}

      {selectedBucket && selectedObject ? (
        <ObjectDetailSheet
          open={selectedObject !== null}
          onOpenChange={(open) => {
            if (!open) deselectObject();
          }}
          bucket={selectedBucket}
          object={selectedObject}
          detail={objectDetail}
          loading={objectDetailLoading}
        />
      ) : null}
    </div>
  );
}
