import { useConfig } from "../api";
import { JsonViewer } from "../components/json-viewer";
import {
  Card,
  CardContent,
} from "../components/ui/card";
import { Skeleton } from "../components/ui/skeleton";

export function LandingPage(): JSX.Element {
  const { data, isLoading, error } = useConfig();

  return (
    <div className="flex flex-1 flex-col gap-6 overflow-y-auto p-6 md:p-10">
      <header className="space-y-3">
        <h1 className="text-3xl font-semibold tracking-tight md:text-4xl">
          Derp Studio
        </h1>
        <p className="text-sm text-muted-foreground md:text-base">
          Inspect active service configuration loaded from{" "}
          <code className="rounded bg-muted px-1.5 py-0.5 text-xs md:text-sm">
            derp.toml
          </code>
          .
        </p>
      </header>

      {isLoading ? (
        <Card>
          <CardContent className="space-y-3 pt-6">
            <Skeleton className="h-4 w-44" />
            <Skeleton className="h-4 w-60" />
            <Skeleton className="h-4 w-52" />
          </CardContent>
        </Card>
      ) : null}

      {error ? (
        <Card className="border-destructive/40">
          <CardContent className="pt-6 text-sm text-destructive">
            Failed to load config: {error.message}
          </CardContent>
        </Card>
      ) : null}

      {!isLoading && !error && data ? (
        <JsonViewer data={data as Record<string, unknown>} />
      ) : null}
    </div>
  );
}
