import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import {
  fetchAuthOrganizations,
  fetchAuthSessions,
  fetchAuthUsers,
  type AuthOrganization,
  type AuthSessionInfo,
  type AuthUser,
} from "../api";

export type AuthTab = "users" | "sessions" | "organizations" | "config";

export function useAuth(enabled: boolean) {
  const [tab, setTab] = useState<AuthTab>("users");

  const usersQuery = useQuery({
    queryKey: ["auth", "users"],
    queryFn: ({ signal }) => fetchAuthUsers(signal),
    enabled: enabled && tab === "users",
  });

  const sessionsQuery = useQuery({
    queryKey: ["auth", "sessions"],
    queryFn: ({ signal }) => fetchAuthSessions(signal),
    enabled: enabled && tab === "sessions",
  });

  const orgsQuery = useQuery({
    queryKey: ["auth", "organizations"],
    queryFn: ({ signal }) => fetchAuthOrganizations(signal),
    enabled: enabled && tab === "organizations",
  });

  const queryForTab = {
    users: usersQuery,
    sessions: sessionsQuery,
    organizations: orgsQuery,
    config: null,
  }[tab];

  const selectTab = useCallback((t: AuthTab) => {
    setTab(t);
  }, []);

  return {
    tab,
    selectTab,
    users: (usersQuery.data?.users ?? []) as AuthUser[],
    sessions: (sessionsQuery.data?.sessions ?? []) as AuthSessionInfo[],
    organizations: (orgsQuery.data?.organizations ??
      []) as AuthOrganization[],
    loading: queryForTab?.isLoading ?? false,
    error: queryForTab?.error
      ? queryForTab.error instanceof Error
        ? queryForTab.error.message
        : String(queryForTab.error)
      : null,
  };
}
