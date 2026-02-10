import { useEffect, useState } from "react";

import { fetchConfig, type StudioConfig } from "../api";

export function ConfigPage(): JSX.Element {
  const [config, setConfig] = useState<StudioConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const controller = new AbortController();

    const load = async () => {
      try {
        const loadedConfig = await fetchConfig(controller.signal);
        setConfig(loadedConfig);
      } catch (err) {
        if (!(err instanceof DOMException && err.name === "AbortError")) {
          setError(err instanceof Error ? err.message : String(err));
        }
      } finally {
        setLoading(false);
      }
    };

    void load();

    return () => {
      controller.abort();
    };
  }, []);

  return (
    <main className="config-page">
      <h1>Derp Studio</h1>
      <p>
        Loaded configuration from <code>derp.toml</code>
      </p>

      {loading ? <pre>Loading...</pre> : null}
      {error ? <pre>Failed to load config: {error}</pre> : null}
      {!loading && !error && config ? <pre>{JSON.stringify(config, null, 2)}</pre> : null}
    </main>
  );
}
