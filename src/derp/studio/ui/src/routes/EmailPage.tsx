import { useQuery } from "@tanstack/react-query";
import { Mail } from "lucide-react";

import { fetchEmailTemplates, useConfig } from "../api";
import { Badge } from "../components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Skeleton } from "../components/ui/skeleton";

function EmailTemplatesSkeleton(): JSX.Element {
  return (
    <>
      <div className="flex items-center gap-2">
        <Skeleton className="h-6 w-28" />
      </div>
      <div className="grid gap-6">
        {Array.from({ length: 3 }).map((_, i) => (
          <Card key={i}>
            <CardHeader className="pb-3">
              <div className="flex items-center gap-2">
                <Skeleton className="size-4 rounded" />
                <Skeleton className="h-4 w-36" />
              </div>
            </CardHeader>
            <CardContent>
              <div className="overflow-hidden rounded-lg border bg-white p-3">
                <Skeleton className="h-124 w-full" />
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </>
  );
}

export function EmailPage(): JSX.Element {
  const { data: config, isLoading } = useConfig();
  const isConfigured = config?.email != null;
  const templatesQuery = useQuery({
    queryKey: ["emailTemplates"],
    queryFn: ({ signal }) => fetchEmailTemplates(signal).then((r) => r.templates),
    enabled: isConfigured,
  });

  const templates = templatesQuery.data ?? [];
  const error = templatesQuery.error
    ? templatesQuery.error instanceof Error
      ? templatesQuery.error.message
      : String(templatesQuery.error)
    : null;

  return (
    <div className="flex flex-1 flex-col gap-6 overflow-y-auto p-6 md:p-10">
      {!isConfigured && !isLoading ? (
        <Card>
          <CardContent className="pt-6 text-sm text-muted-foreground">
            Email is not configured. Add an{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              [email]
            </code>{" "}
            section to your{" "}
            <code className="rounded bg-muted px-1.5 py-0.5 text-xs">
              derp.toml
            </code>{" "}
            to preview templates.
          </CardContent>
        </Card>
      ) : null}

      {(isLoading || isConfigured) && templatesQuery.isLoading ? (
        <EmailTemplatesSkeleton />
      ) : null}

      {isConfigured && error ? (
        <Card className="border-destructive/40">
          <CardContent className="pt-6 text-sm text-destructive">
            {error}
          </CardContent>
        </Card>
      ) : null}

      {isConfigured && !templatesQuery.isLoading && !error ? (
        templates.length === 0 ? (
          <Card>
            <CardContent className="pt-6 text-sm text-muted-foreground">
              No email templates found.
            </CardContent>
          </Card>
        ) : (
          <>
            <div className="flex items-center gap-2">
              <Badge variant="secondary">
                {templates.length} templates
              </Badge>
            </div>
            <div className="grid gap-6">
              {templates.map((template) => (
                <Card key={template.name}>
                  <CardHeader className="pb-3">
                    <CardTitle className="flex items-center gap-2 text-sm font-medium">
                      <Mail className="size-4 text-muted-foreground" />
                      {template.name}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-hidden rounded-lg border bg-white">
                      <iframe
                        title={`${template.name} preview`}
                        srcDoc={template.html}
                        sandbox=""
                        className="h-130 w-full"
                      />
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </>
        )
      ) : null}
    </div>
  );
}
