// State
let state = {
  user: null,
  accessToken: null,
  refreshToken: null,
  conversations: [],
  currentConversation: null,
  messages: [],
};

// API helpers
const API_BASE = "";

async function api(endpoint, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...options.headers,
  };

  if (state.accessToken) {
    headers["Authorization"] = `Bearer ${state.accessToken}`;
  }

  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers,
  });

  if (response.status === 401 && state.refreshToken) {
    // Try to refresh token
    const refreshed = await refreshAccessToken();
    if (refreshed) {
      headers["Authorization"] = `Bearer ${state.accessToken}`;
      return fetch(`${API_BASE}${endpoint}`, { ...options, headers });
    }
  }

  return response;
}

async function refreshAccessToken() {
  try {
    const response = await fetch(`${API_BASE}/auth/refresh`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: state.refreshToken }),
    });

    if (response.ok) {
      const data = await response.json();
      state.accessToken = data.access_token;
      state.refreshToken = data.refresh_token;
      saveTokens();
      return true;
    }
  } catch (e) {
    console.error("Failed to refresh token:", e);
  }

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
}

// Avatar helper
function getInitials(email, displayName) {
  if (displayName) {
    return displayName
      .split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase()
      .slice(0, 2);
  }
  return email[0].toUpperCase();
}

function createAvatar(email, displayName, avatarUrl, small = false) {
  const div = document.createElement("div");
  div.className = `avatar${small ? " small" : ""}`;

  if (avatarUrl) {
    const img = document.createElement("img");
    img.src = avatarUrl;
    img.alt = displayName || email;
    img.style.width = "100%";
    img.style.height = "100%";
    img.style.objectFit = "cover";
    img.style.borderRadius = "50%";
    div.appendChild(img);
  } else {
    div.textContent = getInitials(email, displayName);
  }

  return div;
}

// Auth
async function signIn(email, password) {
  const response = await fetch(`${API_BASE}/auth/signin`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Sign in failed");
  }

  const data = await response.json();
  state.user = data.user;
  state.accessToken = data.access_token;
  state.refreshToken = data.refresh_token;
  saveTokens();

  return data;
}

async function signUp(email, password) {
  const response = await fetch(`${API_BASE}/auth/signup`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Sign up failed");
  }

  const data = await response.json();
  state.user = data.user;
  state.accessToken = data.access_token;
  state.refreshToken = data.refresh_token;
  saveTokens();

  return data;
}

async function signOut() {
  try {
    await api("/auth/signout", { method: "POST" });
  } catch (e) {
    // Ignore errors during sign out
  }

  state.user = null;
  state.accessToken = null;
  state.refreshToken = null;
  state.conversations = [];
  state.currentConversation = null;
  state.messages = [];
  clearTokens();

  showAuthView();
}

async function loadCurrentUser() {
  const response = await api("/auth/user");
  if (response.ok) {
    state.user = await response.json();
    return true;
  }
  return false;
}

// Conversations
async function loadConversations() {
  const response = await api("/conversations");
  if (response.ok) {
    state.conversations = await response.json();
    renderConversations();
  }
}

async function startConversation(userId) {
  const response = await api("/conversations", {
    method: "POST",
    body: JSON.stringify({ user_id: userId }),
  });

  if (response.ok) {
    const conversation = await response.json();
    await loadConversations();
    selectConversation(conversation.id);
    closeModal();
  }
}

async function selectConversation(conversationId) {
  state.currentConversation = state.conversations.find(
    (c) => c.id === conversationId,
  );

  if (state.currentConversation) {
    await loadMessages(conversationId);
    renderChat();

    // Mark as read
    if (state.currentConversation.unread_count > 0) {
      await api(`/conversations/${conversationId}/read`, { method: "PATCH" });
      state.currentConversation.unread_count = 0;
      renderConversations();
    }
  }
}

// Messages
async function loadMessages(conversationId) {
  const response = await api(`/conversations/${conversationId}`);
  if (response.ok) {
    const data = await response.json();
    state.messages = data.messages.reverse(); // Oldest first
  }
}

async function sendMessage(content) {
  if (!state.currentConversation) return;

  const response = await api(
    `/conversations/${state.currentConversation.id}/messages`,
    {
      method: "POST",
      body: JSON.stringify({ content }),
    },
  );

  if (response.ok) {
    const message = await response.json();
    state.messages.push(message);
    renderMessages();
    scrollToBottom();

    // Update conversation preview
    state.currentConversation.last_message_at = message.created_at;
    renderConversations();
  }
}

// Users
async function searchUsers(query = "") {
  const params = query ? `?search=${encodeURIComponent(query)}` : "";
  const response = await api(`/users${params}`);
  if (response.ok) {
    return await response.json();
  }
  return [];
}

// UI Rendering
function showAuthView() {
  document.getElementById("auth-view").classList.remove("hidden");
  document.getElementById("app-view").classList.add("hidden");
}

function showAppView() {
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("app-view").classList.remove("hidden");

  // Update user info
  const avatarContainer = document.getElementById("user-avatar");
  const newAvatar = createAvatar(
    state.user.email,
    state.user.username,
    state.user.avatar_url,
  );
  newAvatar.id = "user-avatar";
  avatarContainer.replaceWith(newAvatar);
  document.getElementById("user-email").textContent =
    state.user.username || state.user.email;

  loadConversations();
}

function renderConversations() {
  const list = document.getElementById("conversations-list");
  list.innerHTML = "";

  if (state.conversations.length === 0) {
    list.innerHTML =
      '<p style="padding: 1rem; color: var(--text-muted); text-align: center;">No conversations yet</p>';
    return;
  }

  for (const conv of state.conversations) {
    const item = document.createElement("div");
    item.className = `conversation-item${
      state.currentConversation?.id === conv.id ? " active" : ""
    }`;
    item.onclick = () => selectConversation(conv.id);

    const avatar = createAvatar(
      conv.other_user.email,
      conv.other_user.username,
      conv.other_user.avatar_url,
    );
    item.appendChild(avatar);

    const info = document.createElement("div");
    info.className = "conversation-info";
    info.innerHTML = `
            <div class="conversation-name">${
              conv.other_user.username || conv.other_user.email
            }</div>
            <div class="conversation-preview">${
              conv.last_message_at
                ? formatTime(conv.last_message_at)
                : "No messages yet"
            }</div>
        `;
    item.appendChild(info);

    if (conv.unread_count > 0) {
      const badge = document.createElement("span");
      badge.className = "unread-badge";
      badge.textContent = conv.unread_count;
      item.appendChild(badge);
    }

    list.appendChild(item);
  }
}

function renderChat() {
  const noChat = document.getElementById("no-chat");
  const chatView = document.getElementById("chat-view");

  if (!state.currentConversation) {
    noChat.classList.remove("hidden");
    chatView.classList.add("hidden");
    return;
  }

  noChat.classList.add("hidden");
  chatView.classList.remove("hidden");

  const other = state.currentConversation.other_user;

  // Update header
  const chatAvatarEl = document.getElementById("chat-avatar");
  const newChatAvatar = createAvatar(
    other.email,
    other.username,
    other.avatar_url,
  );
  newChatAvatar.id = "chat-avatar";
  chatAvatarEl.replaceWith(newChatAvatar);
  document.getElementById("chat-user-name").textContent =
    other.username || other.email;
  document.getElementById("chat-user-email").textContent = other.username
    ? other.email
    : "";

  renderMessages();
}

function renderMessages() {
  const container = document.getElementById("messages");
  container.innerHTML = "";

  for (const msg of state.messages) {
    const div = document.createElement("div");
    div.className = `message ${msg.is_mine ? "mine" : "other"}`;
    div.innerHTML = `
            <div>${escapeHtml(msg.content)}</div>
            <div class="message-time">${formatTime(msg.created_at)}</div>
        `;
    container.appendChild(div);
  }

  scrollToBottom();
}

function scrollToBottom() {
  const container = document.getElementById("messages");
  container.scrollTop = container.scrollHeight;
}

// Modal
function openModal() {
  document.getElementById("new-chat-modal").classList.remove("hidden");
  document.getElementById("user-search").value = "";
  loadUsersList();
}

function closeModal() {
  document.getElementById("new-chat-modal").classList.add("hidden");
}

async function loadUsersList(query = "") {
  const users = await searchUsers(query);
  const list = document.getElementById("users-list");
  list.innerHTML = "";

  if (users.length === 0) {
    list.innerHTML =
      '<p style="color: var(--text-muted); text-align: center;">No users found</p>';
    return;
  }

  for (const user of users) {
    const item = document.createElement("div");
    item.className = "user-item";
    item.onclick = () => startConversation(user.id);

    const avatar = createAvatar(
      user.email,
      user.username,
      user.avatar_url,
      true,
    );
    item.appendChild(avatar);

    const info = document.createElement("div");
    info.className = "user-item-info";
    info.innerHTML = `
            <span class="user-item-name">${
              user.username || user.email
            }</span>
            ${
              user.username
                ? `<span class="user-item-email">${user.email}</span>`
                : ""
            }
        `;
    item.appendChild(info);

    list.appendChild(item);
  }
}

// Helpers
function formatTime(isoString) {
  const date = new Date(isoString);
  const now = new Date();
  const diff = now - date;

  if (diff < 60000) return "now";
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000)
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  if (diff < 604800000)
    return date.toLocaleDateString([], { weekday: "short" });
  return date.toLocaleDateString([], { month: "short", day: "numeric" });
}

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// Event Listeners
document.addEventListener("DOMContentLoaded", async () => {
  // Tab switching
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      document
        .querySelectorAll(".tab")
        .forEach((t) => t.classList.remove("active"));
      tab.classList.add("active");

      const tabName = tab.dataset.tab;
      document
        .getElementById("signin-form")
        .classList.toggle("hidden", tabName !== "signin");
      document
        .getElementById("signup-form")
        .classList.toggle("hidden", tabName !== "signup");
    });
  });

  // Sign in form
  document
    .getElementById("signin-form")
    .addEventListener("submit", async (e) => {
      e.preventDefault();
      const form = e.target;
      const email = form.email.value;
      const password = form.password.value;
      const errorEl = document.getElementById("signin-error");

      try {
        form.classList.add("loading");
        errorEl.textContent = "";
        await signIn(email, password);
        showAppView();
      } catch (err) {
        errorEl.textContent = err.message;
      } finally {
        form.classList.remove("loading");
      }
    });

  // Sign up form
  document
    .getElementById("signup-form")
    .addEventListener("submit", async (e) => {
      e.preventDefault();
      const form = e.target;
      const email = form.email.value;
      const password = form.password.value;
      const errorEl = document.getElementById("signup-error");

      try {
        form.classList.add("loading");
        errorEl.textContent = "";
        await signUp(email, password);
        showAppView();
      } catch (err) {
        errorEl.textContent = err.message;
      } finally {
        form.classList.remove("loading");
      }
    });

  // Sign out
  document.getElementById("signout-btn").addEventListener("click", signOut);

  // New chat
  document.getElementById("new-chat-btn").addEventListener("click", openModal);
  document.querySelector(".modal-close").addEventListener("click", closeModal);
  document.getElementById("new-chat-modal").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) closeModal();
  });

  // User search
  let searchTimeout;
  document.getElementById("user-search").addEventListener("input", (e) => {
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(() => loadUsersList(e.target.value), 300);
  });

  // Send message
  document
    .getElementById("message-form")
    .addEventListener("submit", async (e) => {
      e.preventDefault();
      const input = document.getElementById("message-input");
      const content = input.value.trim();

      if (content) {
        input.value = "";
        await sendMessage(content);
      }
    });

  // Auto-refresh conversations
  setInterval(async () => {
    if (state.accessToken && document.visibilityState === "visible") {
      await loadConversations();
    }
  }, 10000);

  // Check for existing session
  loadTokens();
  if (state.accessToken) {
    const valid = await loadCurrentUser();
    if (valid) {
      showAppView();
    } else {
      showAuthView();
    }
  }
});
