import { CreditCard, Database, HardDrive, KeyRound, Mail, Shield, SmileIcon } from "lucide-react";
import { Link, NavLink } from "react-router-dom";

import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail
} from "@/components/ui/sidebar";

const services = [
  { label: "Database", path: "/database", icon: Database },
  { label: "Storage", path: "/storage", icon: HardDrive },
  { label: "KV Storage", path: "/kv", icon: KeyRound },
  { label: "Authentication", path: "/auth", icon: Shield },
  { label: "Email Server", path: "/email", icon: Mail },
  { label: "Payments", path: "/payments", icon: CreditCard },
] as const;

export function AppSidebar({
  ...props
}: React.ComponentProps<typeof Sidebar>) {
  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <Link to="/">
              <SidebarMenuButton
                size="lg"
                className="data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground"
              >
                <div className="text-sidebar-primary flex aspect-square size-8 items-center justify-center rounded-lg">
                  <SmileIcon />
                </div>
                <div className="grid flex-1 text-left text-sm leading-tight">
                  <span className="truncate font-medium">Derp Studio</span>
                </div>
              </SidebarMenuButton>
            </Link>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarMenu>
            {services.map((item) => (
              <SidebarMenuItem key={item.path}>
                <SidebarMenuButton asChild tooltip={item.label}>
                  <NavLink to={item.path}>
                    <item.icon />
                    <span>{item.label}</span>
                  </NavLink>
                </SidebarMenuButton>
              </SidebarMenuItem>
            ))}
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>
      <SidebarRail />
    </Sidebar>
  );
}
