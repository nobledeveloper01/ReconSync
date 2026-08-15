import "./style.css";
import { ApiError, auth, can, clearKey, storedKey, type Session } from "./api";
import { account, resetPassword, signIn, team } from "./auth-views";
import { audit, compliance, overview, rails, transactions, type View } from "./views";
import { esc } from "./format";

// Tabs a role cannot use are not rendered at all. Showing a tab that returns
// 403 teaches an operator that the dashboard is unreliable; showing only what
// they can do teaches them what their role is.
const TABS: { id: string; label: string; view: View; scope?: string; adminOnly?: boolean }[] = [
  { id: "overview", label: "Outstanding", view: overview, scope: "reports:read" },
  { id: "transactions", label: "Transactions", view: transactions, scope: "reports:read" },
  { id: "compliance", label: "Compliance", view: compliance, scope: "reports:read" },
  { id: "rails", label: "Rails", view: rails, scope: "reports:read" },
  { id: "audit", label: "Audit", view: audit, scope: "reports:read" },
  { id: "team", label: "Users", view: async () => {}, adminOnly: true },
  { id: "account", label: "Account", view: async () => {} },
];

const app = document.querySelector<HTMLDivElement>("#app")!;
let session: Session | null = null;

function visibleTabs(): typeof TABS {
  return TABS.filter((t) => {
    if (t.adminOnly) return can(session, "endpoints:write");
    return !t.scope || can(session, t.scope);
  });
}

function shell(): void {
  const tabs = visibleTabs();
  const wanted = location.hash.slice(1) || tabs[0].id;
  const tab = tabs.find((t) => t.id === wanted) ?? tabs[0];

  app.innerHTML = `
    <header>
      <h1>ReconSync</h1>
      <nav>${tabs
        .map((t) => `<a href="#${t.id}" class="${t.id === tab.id ? "on" : ""}">${esc(t.label)}</a>`)
        .join("")}</nav>
      <div class="who">
        <span class="whoami">${esc(session?.email || "API key")}${
          session?.role ? ` · ${esc(session.role)}` : ""
        }</span>
        <button id="sign-out">Sign out</button>
      </div>
    </header>
    <main id="view"><div class="loading">Loading…</div></main>`;

  app.querySelector<HTMLButtonElement>("#sign-out")!.addEventListener("click", async () => {
    clearKey();
    try {
      await auth.logout();
    } catch {
      // Already gone, which is the outcome we wanted anyway.
    }
    session = null;
    location.hash = "";
    start("You are signed out.", "banner-good");
  });

  const view = app.querySelector<HTMLElement>("#view")!;
  const render =
    tab.id === "account"
      ? account(view, session!)
      : tab.id === "team"
        ? team(view, session!)
        : tab.view(view);

  render.catch((err: unknown) => {
    if (err instanceof ApiError && err.status === 401) {
      // The session expired or the key was revoked. Sending the operator back
      // to the form with the reason beats a dashboard of empty tables.
      clearKey();
      session = null;
      start("Your session ended. Sign in again.");
      return;
    }
    if (err instanceof ApiError && err.status === 403) {
      view.innerHTML = `<div class="banner banner-warn">${esc(err.message)}</div>`;
      return;
    }
    if (err instanceof ApiError && err.status === 402) {
      // Licence expired. Say what is withheld and what is not, because the
      // difference is the whole point of how expiry works here.
      view.innerHTML = `<div class="banner banner-warn">${esc(err.message)}</div>`;
      return;
    }
    const message = err instanceof Error ? err.message : String(err);
    view.innerHTML = `<div class="banner banner-bad">${esc(message)}</div>`;
  });
}

function signedIn(s: Session): void {
  session = s;
  if (!s.totp_enabled && s.email) {
    // Nagged once per sign-in, not blocked. Locking someone out of their own
    // reconciliation data to enforce a security control is how the control
    // gets removed.
    location.hash = location.hash || "#account";
  }
  shell();
}

function start(notice = "", kind = "banner-bad"): void {
  signIn(app, signedIn, notice);
  if (notice && kind !== "banner-bad") {
    app.querySelector(".banner")?.classList.replace("banner-bad", kind);
  }
}

// Downloads need the credential on the request, which a plain link cannot
// carry. With a session that is the cookie; with a key it is the header.
document.addEventListener("click", async (e) => {
  const link = (e.target as HTMLElement).closest<HTMLAnchorElement>("[data-auth-download]");
  if (!link) return;
  e.preventDefault();

  const key = storedKey();
  const res = await fetch(link.href, {
    credentials: "same-origin",
    headers: key ? { Authorization: `Bearer ${key}` } : {},
  });
  if (!res.ok) return;

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = link.href.includes("pdf") ? "reversal-compliance.pdf" : "reversal-compliance.csv";
  a.click();
  URL.revokeObjectURL(url);
});

window.addEventListener("hashchange", () => {
  if (session) shell();
});

// A reset link lands here with the token in the fragment, which never reaches
// the server as a query string would — so it stays out of access logs.
const resetMatch = location.hash.match(/^#reset=(.+)$/);
if (resetMatch) {
  resetPassword(app, decodeURIComponent(resetMatch[1]), () => {
    location.hash = "";
    start("Password set. Sign in with it.", "banner-good");
  });
} else {
  // Ask the server who we are before drawing anything: a live session cookie
  // means the person never sees a sign-in form they do not need.
  auth
    .me()
    .then(signedIn)
    .catch(() => start());
}
