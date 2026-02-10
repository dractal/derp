import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ConfigPage } from "./ConfigPage";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("ConfigPage", () => {
  it("shows a loading state before fetch resolves", () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(() => new Promise(() => { })) as unknown as typeof fetch
    );

    render(<ConfigPage />);

    expect(screen.getByText("Loading...")).toBeInTheDocument();
  });

  it("renders fetched config", async () => {
    const payload = {
      database: {
        db_url: "postgresql://example",
      },
    };

    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(JSON.stringify(payload), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
          },
        })
      ) as unknown as typeof fetch
    );

    render(<ConfigPage />);

    expect(await screen.findByText(/postgresql:\/\/example/i)).toBeInTheDocument();
  });

  it("renders an error when fetch fails", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response("boom", {
          status: 500,
        })
      ) as unknown as typeof fetch
    );

    render(<ConfigPage />);

    expect(await screen.findByText(/Failed to load config/i)).toBeInTheDocument();
  });
});
