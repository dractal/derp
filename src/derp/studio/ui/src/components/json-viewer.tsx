import { ChevronRight } from "lucide-react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";

type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

function Primitive({ value }: { value: string | number | boolean | null }) {
  if (value === null) {
    return <span className="text-muted-foreground italic">null</span>;
  }
  if (typeof value === "boolean") {
    return (
      <span className="text-orange-600 dark:text-orange-400">
        {String(value)}
      </span>
    );
  }
  if (typeof value === "number") {
    return (
      <span className="text-blue-600 dark:text-blue-400">{value}</span>
    );
  }
  return (
    <span className="text-green-700 dark:text-green-400">
      &quot;{value}&quot;
    </span>
  );
}

function JsonNode({
  name,
  value,
  defaultOpen = false,
}: {
  name: string;
  value: JsonValue;
  defaultOpen?: boolean;
}) {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return (
      <div className="flex items-baseline gap-2 py-0.5 pl-5">
        <span className="shrink-0 font-medium">{name}</span>
        <Primitive value={value} />
      </div>
    );
  }

  const isArray = Array.isArray(value);
  const entries = isArray
    ? value.map((v, i) => [String(i), v] as const)
    : Object.entries(value as Record<string, JsonValue>);
  const bracket = isArray ? ["[", "]"] : ["{", "}"];

  if (entries.length === 0) {
    return (
      <div className="flex items-baseline gap-2 py-0.5 pl-5">
        <span className="shrink-0 font-medium">{name}</span>
        <span className="text-muted-foreground">
          {bracket[0]}
          {bracket[1]}
        </span>
      </div>
    );
  }

  return (
    <Collapsible defaultOpen={defaultOpen}>
      <CollapsibleTrigger className="group flex w-full items-center gap-1 rounded py-0.5 text-left hover:bg-muted/60">
        <ChevronRight className="size-4 shrink-0 transition-transform group-data-[state=open]:rotate-90" />
        <span className="font-medium">{name}</span>
        <span className="text-muted-foreground text-xs">
          {bracket[0]}
          {entries.length}
          {bracket[1]}
        </span>
      </CollapsibleTrigger>
      <CollapsibleContent>
        <div className="border-l pl-3 ml-2">
          {entries.map(([key, val]) => (
            <JsonNode key={key} name={key} value={val} />
          ))}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

export function JsonViewer({
  data,
  className,
}: {
  data: Record<string, unknown>;
  className?: string;
}) {
  const entries = Object.entries(data);

  return (
    <div
      className={cn(
        "rounded-lg border bg-muted/40 p-4 font-mono text-xs md:text-sm",
        className,
      )}
    >
      {entries.map(([key, value]) => (
        <JsonNode
          key={key}
          name={key}
          value={value as JsonValue}
          defaultOpen
        />
      ))}
    </div>
  );
}
