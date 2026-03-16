// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const state = {
  user: null,
  accessToken: null,
  refreshToken: null,
  workspaces: [],
  currentWorkspace: null,
  channels: [],
  currentChannel: null,
  messages: [],
};

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

async function getErrorMessage(response, fallback) {
  try {
    const data = await response.json();
    return data.detail || fallback;
  } catch {
    return fallback;
  }
}

async function api(endpoint, options = {}) {
  const headers = { "Content-Type": "application/json", ...options.headers };
  if (state.accessToken) {
    headers["Authorization"] = `Bearer ${state.accessToken}`;
  }
  const response = await fetch(endpoint, { ...options, headers });
  if (response.status === 401 && state.refreshToken) {
    const refreshed = await refreshAccessToken();
    if (refreshed) {
      headers["Authorization"] = `Bearer ${state.accessToken}`;
      return fetch(endpoint, { ...options, headers });
    }
  }
  return response;
}

async function refreshAccessToken() {
  try {
    const r = await fetch("/auth/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: state.refreshToken }),
    });
    if (r.ok) {
      const data = await r.json();
      state.accessToken = data.access_token;
      state.refreshToken = data.refresh_token;
      saveTokens();
      return true;
    }
  } catch (_) {}
  signOut();
  return false;
}

// Token persistence
function saveTokens() {
  localStorage.setItem("accessToken", state.accessToken);
  localStorage.setItem("refreshToken", state.refreshToken);
}
function loadTokens() {
  state.accessToken = localStorage.getItem("accessToken");
  state.refreshToken = localStorage.getItem("refreshToken");
}
function clearTokens() {
  localStorage.removeItem("accessToken");
  localStorage.removeItem("refreshToken");
  localStorage.removeItem("lastWorkspace");
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

async function signIn(email, password) {
  const r = await fetch("/auth/signin", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!r.ok) throw new Error(await getErrorMessage(r, "Sign in failed"));
  const data = await r.json();
  state.user = data.user;
  state.accessToken = data.access_token;
  state.refreshToken = data.refresh_token;
  saveTokens();
}

async function signUp(email, password) {
  const r = await fetch("/auth/signup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!r.ok) throw new Error(await getErrorMessage(r, "Sign up failed"));
  const data = await r.json();
  state.user = data.user;
  state.accessToken = data.access_token;
  state.refreshToken = data.refresh_token;
  saveTokens();
}

async function signOut() {
  try { await api("/auth/signout", { method: "POST" }); } catch (_) {}
  state.user = null;
  state.accessToken = null;
  state.refreshToken = null;
  state.workspaces = [];
  state.currentWorkspace = null;
  state.channels = [];
  state.currentChannel = null;
  state.messages = [];
  clearTokens();
  showView("auth");
}

async function loadCurrentUser() {
  const r = await api("/auth/user");
  if (r.ok) {
    state.user = await r.json();
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Workspaces
// ---------------------------------------------------------------------------

async function loadWorkspaces() {
  const r = await api("/workspaces");
  if (r.ok) state.workspaces = await r.json();
}

async function createWorkspace(name, slug) {
  const r = await api("/workspaces", {
    method: "POST",
    body: JSON.stringify({ name, slug }),
  });
  if (!r.ok) throw new Error(await getErrorMessage(r, "Failed"));
  return await r.json();
}

async function selectWorkspace(ws) {
  state.currentWorkspace = ws;
  localStorage.setItem("lastWorkspace", ws.id);
  await loadChannels();
  showView("app");
  renderWorkspaceRail();
  renderChannelSidebar();
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

async function loadChannels() {
  if (!state.currentWorkspace) return;
  const r = await api(`/workspaces/${state.currentWorkspace.id}/channels`);
  if (r.ok) state.channels = await r.json();
}

async function createChannel(name, topic, isPrivate) {
  const r = await api(`/workspaces/${state.currentWorkspace.id}/channels`, {
    method: "POST",
    body: JSON.stringify({ name, topic: topic || null, is_private: isPrivate }),
  });
  if (!r.ok) throw new Error(await getErrorMessage(r, "Failed"));
  return await r.json();
}

async function startDM(userId) {
  const r = await api(`/workspaces/${state.currentWorkspace.id}/dm`, {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });
  if (!r.ok) throw new Error(await getErrorMessage(r, "Failed"));
  return await r.json();
}

async function joinChannel(channelId) {
  await api(`/channels/${channelId}/join`, { method: "POST" });
}

async function selectChannel(channel) {
  state.currentChannel = channel;

  // Auto-join if not a member of public channel
  if (!channel.is_private && !channel.is_dm) {
    await joinChannel(channel.id);
  }

  await loadMessages();
  renderChannelSidebar();
  renderChat();
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

async function loadMessages() {
  if (!state.currentChannel) return;
  const r = await api(`/channels/${state.currentChannel.id}/messages?limit=50`);
  if (r.ok) {
    state.messages = (await r.json()).reverse(); // oldest first
  }
}

async function sendMessage(content) {
  if (!state.currentChannel) return;
  const r = await api(`/channels/${state.currentChannel.id}/messages`, {
    method: "POST",
    body: JSON.stringify({ content }),
  });
  if (r.ok) {
    const msg = await r.json();
    state.messages.push(msg);
    renderMessages();
    scrollToBottom();
  }
}

// ---------------------------------------------------------------------------
// User search (workspace-scoped)
// ---------------------------------------------------------------------------

async function searchUsers(query = "") {
  if (!state.currentWorkspace) return [];
  const params = query ? `?q=${encodeURIComponent(query)}` : "";
  const r = await api(
    `/workspaces/${state.currentWorkspace.id}/users/search${params}`,
  );
  return r.ok ? await r.json() : [];
}

// ---------------------------------------------------------------------------
// View management
// ---------------------------------------------------------------------------

function showView(view) {
  document.getElementById("auth-view").classList.toggle("hidden", view !== "auth");
  document.getElementById("workspace-view").classList.toggle("hidden", view !== "workspace");
  document.getElementById("app-view").classList.toggle("hidden", view !== "app");
}

// ---------------------------------------------------------------------------
// Rendering: Workspace selection
// ---------------------------------------------------------------------------

function renderWorkspaceList() {
  const list = document.getElementById("workspace-list");
  list.innerHTML = "";

  if (state.workspaces.length === 0) {
    list.innerHTML = '<p style="text-align:center; color: #868686; padding:1rem;">No workspaces yet. Create one!</p>';
    return;
  }

  for (const ws of state.workspaces) {
    const item = document.createElement("div");
    item.className = "workspace-item";
    item.onclick = () => selectWorkspace(ws);
    item.innerHTML = `
      <div class="workspace-icon">${ws.name[0].toUpperCase()}</div>
      <div class="workspace-item-info">
        <div class="workspace-item-name">${esc(ws.name)}</div>
        <div class="workspace-item-slug">${esc(ws.slug)}</div>
      </div>
    `;
    list.appendChild(item);
  }
}

// ---------------------------------------------------------------------------
// Rendering: App (Slack layout)
// ---------------------------------------------------------------------------

function renderWorkspaceRail() {
  const list = document.getElementById("ws-rail-list");
  list.innerHTML = "";
  for (const ws of state.workspaces) {
    const btn = document.createElement("div");
    btn.className = `ws-rail-item${ws.id === state.currentWorkspace?.id ? " active" : ""}`;
    btn.title = ws.name;
    btn.textContent = ws.name[0].toUpperCase();
    btn.onclick = () => selectWorkspace(ws);
    list.appendChild(btn);
  }
}

function renderChannelSidebar() {
  document.getElementById("ws-name-display").textContent =
    state.currentWorkspace?.name || "";

  const channelList = document.getElementById("channel-list");
  const dmList = document.getElementById("dm-list");
  channelList.innerHTML = "";
  dmList.innerHTML = "";

  for (const ch of state.channels) {
    const item = document.createElement("div");
    item.className = `channel-item${state.currentChannel?.id === ch.id ? " active" : ""}`;
    item.onclick = () => selectChannel(ch);

    if (ch.is_dm) {
      // Extract other user's name from DM channel name
      const displayName = getDMDisplayName(ch.name);
      item.innerHTML = `<span class="dm-avatar">${displayName[0].toUpperCase()}</span> ${esc(displayName)}`;
      dmList.appendChild(item);
    } else {
      const prefix = ch.is_private ? "🔒" : "#";
      item.innerHTML = `<span class="prefix">${prefix}</span> ${esc(ch.name)}`;
      channelList.appendChild(item);
    }
  }

  // Update user info
  if (state.user) {
    const avatarEl = document.getElementById("user-avatar");
    avatarEl.textContent = getInitials(
      state.user.email,
      state.user.display_name || state.user.username,
    );
    document.getElementById("user-display-name").textContent =
      state.user.display_name || state.user.username || state.user.email;
  }
}

function getDMDisplayName(channelName) {
  // Channel name format: dm-user1-user2
  if (!channelName.startsWith("dm-")) return channelName;
  const parts = channelName.slice(3).split("-");
  const myName = (
    state.user?.username ||
    state.user?.email?.split("@")[0] ||
    ""
  ).toLowerCase();
  // Return the other user's name
  for (const p of parts) {
    if (p !== myName) return p;
  }
  return parts[0] || channelName;
}

function renderChat() {
  const noChannel = document.getElementById("no-channel");
  const chatView = document.getElementById("chat-view");

  if (!state.currentChannel) {
    noChannel.classList.remove("hidden");
    chatView.classList.add("hidden");
    return;
  }

  noChannel.classList.add("hidden");
  chatView.classList.remove("hidden");

  const ch = state.currentChannel;
  const isChannel = !ch.is_dm;

  document.getElementById("chat-channel-name").textContent = isChannel
    ? `${ch.is_private ? "🔒 " : "# "}${ch.name}`
    : getDMDisplayName(ch.name);

  document.getElementById("chat-channel-topic").textContent = ch.topic || "";
  document.getElementById("chat-member-count").textContent = `${ch.member_count} member${ch.member_count !== 1 ? "s" : ""}`;

  document.getElementById("message-input").placeholder = isChannel
    ? `Message #${ch.name}`
    : `Message ${getDMDisplayName(ch.name)}`;

  renderMessages();
}

function renderMessages() {
  const container = document.getElementById("messages");
  container.innerHTML = "";

  let lastSender = null;
  let lastTime = null;

  for (const msg of state.messages) {
    const msgTime = new Date(msg.created_at);
    const sameGroup =
      msg.sender_id === lastSender &&
      lastTime &&
      msgTime - lastTime < 300000; // 5 min

    if (sameGroup) {
      // Continuation — just content
      const cont = document.createElement("div");
      cont.className = "message-continuation";
      cont.innerHTML = `<div class="message-content">${esc(msg.content)}</div>`;
      if (msg.edited_at) {
        cont.innerHTML += '<span class="message-edited">(edited)</span>';
      }
      container.appendChild(cont);
    } else {
      const group = document.createElement("div");
      group.className = "message-group";

      const initials = getInitials(msg.sender_name, msg.sender_name);
      group.innerHTML = `
        <div class="message-avatar">${initials}</div>
        <div class="message-body">
          <div class="message-header">
            <span class="message-sender">${esc(msg.sender_name)}</span>
            <span class="message-time">${formatTime(msg.created_at)}</span>
            ${msg.edited_at ? '<span class="message-edited">(edited)</span>' : ""}
          </div>
          <div class="message-content">${esc(msg.content)}</div>
        </div>
      `;
      container.appendChild(group);
    }

    lastSender = msg.sender_id;
    lastTime = msgTime;
  }

  scrollToBottom();
}

function scrollToBottom() {
  const container = document.getElementById("messages");
  container.scrollTop = container.scrollHeight;
}

// ---------------------------------------------------------------------------
// Modals
// ---------------------------------------------------------------------------

function openModal(id) {
  document.getElementById(id).classList.remove("hidden");
}
function closeModal(id) {
  document.getElementById(id).classList.add("hidden");
}

async function loadDMUsersList(query = "") {
  const users = await searchUsers(query);
  const list = document.getElementById("dm-users-list");
  list.innerHTML = "";

  if (users.length === 0) {
    list.innerHTML = '<p style="color:#868686; text-align:center; padding:0.5rem;">No users found</p>';
    return;
  }

  for (const u of users) {
    const item = document.createElement("div");
    item.className = "user-item";
    item.onclick = async () => {
      const ch = await startDM(u.id);
      closeModal("dm-modal");
      await loadChannels();
      renderChannelSidebar();
      await selectChannel(ch);
    };

    const avatarDiv = document.createElement("div");
    avatarDiv.className = "avatar small";
    avatarDiv.textContent = getInitials(u.email, u.display_name || u.username);
    item.appendChild(avatarDiv);

    const info = document.createElement("div");
    info.className = "user-item-info";
    info.innerHTML = `
      <span class="user-item-name">${esc(u.display_name || u.username || u.email)}</span>
      <span class="user-item-email">${esc(u.email)}</span>
    `;
    item.appendChild(info);
    list.appendChild(item);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getInitials(email, displayName) {
  if (displayName) {
    return displayName
      .split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase()
      .slice(0, 2);
  }
  return (email || "?")[0].toUpperCase();
}

function formatTime(iso) {
  const d = new Date(iso);
  const now = new Date();
  const diff = now - d;
  if (diff < 86400000 && d.getDate() === now.getDate()) {
    return d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  }
  if (diff < 604800000) {
    return d.toLocaleDateString([], { weekday: "short" }) + " " +
      d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  }
  return d.toLocaleDateString([], { month: "short", day: "numeric" });
}

function esc(text) {
  const d = document.createElement("div");
  d.textContent = text || "";
  return d.innerHTML;
}

// ---------------------------------------------------------------------------
// Event listeners
// ---------------------------------------------------------------------------

document.addEventListener("DOMContentLoaded", async () => {
  // Auth tabs
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
      tab.classList.add("active");
      const name = tab.dataset.tab;
      document.getElementById("signin-form").classList.toggle("hidden", name !== "signin");
      document.getElementById("signup-form").classList.toggle("hidden", name !== "signup");
    });
  });

  // Sign in
  document.getElementById("signin-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = e.target;
    const errEl = document.getElementById("signin-error");
    try {
      form.classList.add("loading");
      errEl.textContent = "";
      await signIn(form.email.value, form.password.value);
      await loadWorkspaces();
      await autoSelectWorkspace();
    } catch (err) {
      errEl.textContent = err.message;
    } finally {
      form.classList.remove("loading");
    }
  });

  // Sign up
  document.getElementById("signup-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = e.target;
    const errEl = document.getElementById("signup-error");
    try {
      form.classList.add("loading");
      errEl.textContent = "";
      await signUp(form.email.value, form.password.value);
      await loadWorkspaces();
      await autoSelectWorkspace();
    } catch (err) {
      errEl.textContent = err.message;
    } finally {
      form.classList.remove("loading");
    }
  });

  // Sign out buttons
  document.getElementById("signout-btn")?.addEventListener("click", signOut);
  document.getElementById("ws-signout-btn")?.addEventListener("click", signOut);

  // Workspace creation
  const createWsBtn = document.getElementById("create-workspace-btn");
  const createWsForm = document.getElementById("create-workspace-form");
  createWsBtn.addEventListener("click", () => {
    createWsForm.classList.toggle("hidden");
  });
  document.getElementById("ws-create-cancel").addEventListener("click", () => {
    createWsForm.classList.add("hidden");
  });
  document.getElementById("ws-create-submit").addEventListener("click", async () => {
    const name = document.getElementById("ws-name").value.trim();
    const slug = document.getElementById("ws-slug").value.trim();
    const errEl = document.getElementById("ws-create-error");
    if (!name || !slug) { errEl.textContent = "Name and slug required"; return; }
    try {
      errEl.textContent = "";
      const ws = await createWorkspace(name, slug);
      await loadWorkspaces();
      renderWorkspaceList();
      createWsForm.classList.add("hidden");
      await selectWorkspace(ws);
    } catch (err) {
      errEl.textContent = err.message;
    }
  });

  // Auto-slug
  document.getElementById("ws-name").addEventListener("input", (e) => {
    const slug = e.target.value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
    document.getElementById("ws-slug").value = slug;
  });

  // Workspace rail add button -> go to workspace selection
  document.getElementById("ws-rail-add")?.addEventListener("click", async () => {
    await loadWorkspaces();
    renderWorkspaceList();
    document.getElementById("ws-user-email").textContent = state.user?.email || "";
    showView("workspace");
  });

  // Channel creation
  document.getElementById("add-channel-btn").addEventListener("click", () => {
    openModal("channel-modal");
    document.getElementById("new-channel-name").value = "";
    document.getElementById("new-channel-topic").value = "";
    document.getElementById("new-channel-private").checked = false;
    document.getElementById("channel-create-error").textContent = "";
  });

  document.getElementById("channel-create-submit").addEventListener("click", async () => {
    const name = document.getElementById("new-channel-name").value.trim();
    const topic = document.getElementById("new-channel-topic").value.trim();
    const isPrivate = document.getElementById("new-channel-private").checked;
    const errEl = document.getElementById("channel-create-error");
    if (!name) { errEl.textContent = "Name is required"; return; }
    try {
      errEl.textContent = "";
      const ch = await createChannel(name, topic, isPrivate);
      closeModal("channel-modal");
      await loadChannels();
      renderChannelSidebar();
      await selectChannel(ch);
    } catch (err) {
      errEl.textContent = err.message;
    }
  });

  // New DM
  document.getElementById("add-dm-btn").addEventListener("click", () => {
    openModal("dm-modal");
    document.getElementById("dm-user-search").value = "";
    loadDMUsersList();
  });

  let dmSearchTimeout;
  document.getElementById("dm-user-search").addEventListener("input", (e) => {
    clearTimeout(dmSearchTimeout);
    dmSearchTimeout = setTimeout(() => loadDMUsersList(e.target.value), 300);
  });

  // Modal close buttons
  document.querySelectorAll(".modal-close").forEach((btn) => {
    btn.addEventListener("click", () => closeModal(btn.dataset.modal));
  });
  document.querySelectorAll(".modal").forEach((modal) => {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeModal(modal.id);
    });
  });

  // Send message
  document.getElementById("message-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("message-input");
    const content = input.value.trim();
    if (content) {
      input.value = "";
      await sendMessage(content);
    }
  });

  // Auto-refresh messages every 5s
  setInterval(async () => {
    if (state.accessToken && state.currentChannel && document.visibilityState === "visible") {
      await loadMessages();
      renderMessages();
    }
  }, 5000);

  // Auto-refresh channels every 15s
  setInterval(async () => {
    if (state.accessToken && state.currentWorkspace && document.visibilityState === "visible") {
      await loadChannels();
      renderChannelSidebar();
    }
  }, 15000);

  // Startup: check existing session
  loadTokens();
  if (state.accessToken) {
    const valid = await loadCurrentUser();
    if (valid) {
      await loadWorkspaces();
      await autoSelectWorkspace();
    } else {
      showView("auth");
    }
  }
});

async function autoSelectWorkspace() {
  if (state.workspaces.length === 0) {
    document.getElementById("ws-user-email").textContent = state.user?.email || "";
    renderWorkspaceList();
    showView("workspace");
    return;
  }

  // Try last workspace
  const lastId = localStorage.getItem("lastWorkspace");
  const last = lastId ? state.workspaces.find((w) => w.id === lastId) : null;

  if (last) {
    await selectWorkspace(last);
  } else if (state.workspaces.length === 1) {
    await selectWorkspace(state.workspaces[0]);
  } else {
    document.getElementById("ws-user-email").textContent = state.user?.email || "";
    renderWorkspaceList();
    showView("workspace");
  }
}
