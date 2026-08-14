import "./style.css";
import { ApiError, clearKey, storedKey, storeKey } from "./api";
import { audit, compliance, overview, rails, transactions, type View } from "./views";

const TABS: { id: string; label: string; view: View }[] = [
  { id: "overview", label: "Outstanding", view: overview },
  { id: "transactions", label: "Transactions", view: transactions },
  { id: "compliance", label: "Compliance", view: compliance },
  { id: "rails", label: "Rails", view: rails },
  { id: "audit", label: "Audit", view: audit },
];

const app = document.querySelector<HTMLDivElement>("#app")!;

function signIn(message = ""): void {
  app.innerHTML = `
    <div class="signin">
      <h1>ReconSync</h1>
      <p>Paste an API key to read this tenant's data.</p>
      ${message ? `<div class="banner banner-bad">${message}</div>` : ""}
      <form id="key-form">
        <input type="password" id="key" placeholder="rs_test_…" autocomplete="off" spellcheck="false" />
        <button type="submit">Open</button>
      </form>
      <p class="note">The key is held for this browser tab only and is cleared when you
      close it. It is sent to this same server and nowhere else.</p>
    </div>`;

  app.querySelector<HTMLFormElement>("#key-form")!.addEventListener("submit", (e) => {
    e.preventDefault();
    const value = app.querySelector<HTMLInputElement>("#key")!.value.trim();
    if (!value) return;
    storeKey(value);
    shell();
  });
}

function shell(): void {
  const current = location.hash.slice(1) || TABS[0].id;
  const tab = TABS.find((t) => t.id === current) ?? TABS[0];

  app.innerHTML = `
    <header>
      <h1>ReconSync</h1>
      <nav>${TABS.map(
        (t) =>
          `<a href="#${t.id}" class="${t.id === tab.id ? "on" : ""}">${t.label}</a>`,
      ).join("")}</nav>
      <button id="sign-out">Sign out</button>
    </header>
    <main id="view"><div class="loading">Loading…</div></main>`;

  app.querySelector<HTMLButtonElement>("#sign-out")!.addEventListener("click", () => {
    clearKey();
    signIn();
  });

  const view = app.querySelector<HTMLElement>("#view")!;
  tab.view(view).catch((err: unknown) => {
    if (err instanceof ApiError && err.status === 401) {
      // The key is wrong or revoked. Sending the operator back to the form with
      // the reason beats a dashboard of empty tables.
      clearKey();
      signIn("That key was rejected.");
      return;
    }
    if (err instanceof ApiError && err.status === 402) {
      // Licence expired. Say what is withheld and what is not, because the
      // difference is the whole point of how expiry works here.
      view.innerHTML = `<div class="banner banner-warn">${err.message}</div>`;
      return;
    }
    const message = err instanceof Error ? err.message : String(err);
    view.innerHTML = `<div class="banner banner-bad">${message}</div>`;
  });
}

// Downloads need the key in a header, which a plain link cannot carry.
document.addEventListener("click", async (e) => {
  const link = (e.target as HTMLElement).closest<HTMLAnchorElement>("[data-auth-download]");
  if (!link) return;
  e.preventDefault();

  const res = await fetch(link.href, { headers: { Authorization: `Bearer ${storedKey()}` } });
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
  if (storedKey()) shell();
});

if (storedKey()) shell();
else signIn();
