import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Route, Routes, useLocation } from "react-router-dom";

import { AppSidebar } from "./components/app-sidebar";
import { ThemeProvider } from "./components/theme-provider";
import { ThemeToggle } from "./components/theme-toggle";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbList,
  BreadcrumbPage,
} from "./components/ui/breadcrumb";
import { Separator } from "./components/ui/separator";
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "./components/ui/sidebar";
import { Toaster } from "./components/ui/sonner";
import { AuthPage } from "./routes/AuthPage";
import { DatabasePage } from "./routes/DatabasePage";
import { EmailPage } from "./routes/EmailPage";
import { KVPage } from "./routes/KVPage";
import { LandingPage } from "./routes/LandingPage";
import { PaymentsPage } from "./routes/PaymentsPage";
import { StoragePage } from "./routes/StoragePage";

const ROUTE_LABELS: Record<string, string> = {
  "/database": "Database",
  "/storage": "Storage",
  "/auth": "Authentication",
  "/email": "Email Server",
  "/kv": "KV Storage",
};

function PageBreadcrumb(): JSX.Element {
  const { pathname } = useLocation();
  const label = ROUTE_LABELS[pathname] ?? "Dashboard";

  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbPage>{label}</BreadcrumbPage>
        </BreadcrumbItem>
      </BreadcrumbList>
    </Breadcrumb>
  );
}

function AppHeader(): JSX.Element {
  return (
    <header className="flex h-12 shrink-0 items-center gap-2 border-b px-4">
      <SidebarTrigger className="-ml-1" />
      <Separator orientation="vertical" className="mr-2 h-4" />
      <PageBreadcrumb />
      <div className="ml-auto">
        <ThemeToggle />
      </div>
    </header>
  );
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5, // 5 minutes
      refetchOnWindowFocus: false,
      refetchOnMount: false,
    },
  },
});

export default function App(): JSX.Element {
  return (
    <ThemeProvider>
      <QueryClientProvider client={queryClient}>
        <SidebarProvider>
          <AppSidebar />
          <SidebarInset className="h-screen overflow-hidden">
            <AppHeader />
            <Routes>
              <Route path="/" element={<LandingPage />} />
              <Route path="/database" element={<DatabasePage />} />
              <Route path="/storage" element={<StoragePage />} />
              <Route path="/auth" element={<AuthPage />} />
              <Route path="/email" element={<EmailPage />} />
              <Route path="/kv" element={<KVPage />} />
              <Route path="/payments" element={<PaymentsPage />} />
            </Routes>
          </SidebarInset>
        </SidebarProvider>
        <Toaster />
      </QueryClientProvider>
    </ThemeProvider>
  );
}
