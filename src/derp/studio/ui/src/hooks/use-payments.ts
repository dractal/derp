import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import {
  fetchCharges,
  fetchCustomers,
  fetchInvoices,
  fetchProducts,
  fetchSubscriptions,
  type StripeCharge,
  type StripeCustomer,
  type StripeInvoice,
  type StripeListResponse,
  type StripeProduct,
  type StripeSubscription,
} from "../api";

export type PaymentsTab =
  | "customers"
  | "products"
  | "subscriptions"
  | "invoices"
  | "charges";

export function usePayments(enabled: boolean) {
  const [tab, setTab] = useState<PaymentsTab>("customers");

  const customersQuery = useQuery({
    queryKey: ["payments", "customers"],
    queryFn: ({ signal }) => fetchCustomers(100, undefined, signal),
    enabled: enabled && tab === "customers",
  });

  const productsQuery = useQuery({
    queryKey: ["payments", "products"],
    queryFn: ({ signal }) => fetchProducts(100, undefined, signal),
    enabled: enabled && tab === "products",
  });

  const subscriptionsQuery = useQuery({
    queryKey: ["payments", "subscriptions"],
    queryFn: ({ signal }) => fetchSubscriptions(100, undefined, signal),
    enabled: enabled && tab === "subscriptions",
  });

  const invoicesQuery = useQuery({
    queryKey: ["payments", "invoices"],
    queryFn: ({ signal }) => fetchInvoices(100, undefined, signal),
    enabled: enabled && tab === "invoices",
  });

  const chargesQuery = useQuery({
    queryKey: ["payments", "charges"],
    queryFn: ({ signal }) => fetchCharges(100, undefined, signal),
    enabled: enabled && tab === "charges",
  });

  const queryForTab = {
    customers: customersQuery,
    products: productsQuery,
    subscriptions: subscriptionsQuery,
    invoices: invoicesQuery,
    charges: chargesQuery,
  }[tab];

  const selectTab = useCallback((t: PaymentsTab) => {
    setTab(t);
  }, []);

  return {
    tab,
    selectTab,
    customers: (customersQuery.data?.data ?? []) as StripeCustomer[],
    products: (productsQuery.data?.data ?? []) as StripeProduct[],
    subscriptions: (subscriptionsQuery.data?.data ?? []) as StripeSubscription[],
    invoices: (invoicesQuery.data?.data ?? []) as StripeInvoice[],
    charges: (chargesQuery.data?.data ?? []) as StripeCharge[],
    loading: queryForTab.isLoading,
    error: queryForTab.error
      ? queryForTab.error instanceof Error
        ? queryForTab.error.message
        : String(queryForTab.error)
      : null,
  };
}
