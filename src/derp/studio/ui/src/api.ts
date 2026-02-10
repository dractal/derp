export type StudioConfig = Record<string, unknown>;

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
