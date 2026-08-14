import {
  api,
  type CurrencyExposure,
  type Transaction,
} from "./api";
import { esc, money, percent, seconds, when } from "./format";

/** A view renders itself into the given element. */
export type View = (root: HTMLElement) => Promise<void>;

function table(headers: string[], rows: string[][]): string {
  if (rows.length === 0) return `<p class="empty">Nothing to show.</p>`;
  return `<table>
    <thead><tr>${headers.map((h) => `<th>${esc(h)}</th>`).join("")}</tr></thead>
    <tbody>${rows
      .map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`)
      .join("")}</tbody>
  </table>`;
}

function card(title: string, value: string, note = ""): string {
  return `<div class="card">
    <div class="card-title">${esc(title)}</div>
    <div class="card-value">${esc(value)}</div>
    ${note ? `<div class="card-note">${esc(note)}</div>` : ""}
  </div>`;
}

function status(s: string): string {
  return `<span class="status status-${esc(s)}">${esc(s)}</span>`;
}

// --- Overview: what is outstanding right now ---

export const overview: View = async (root) => {
  const [exposure, licence] = await Promise.all([api.exposure("all"), api.licence()]);

  const banner = licence.notice
    ? `<div class="banner ${licence.expired ? "banner-bad" : "banner-warn"}">${esc(licence.notice)}</div>`
    : "";

  if (exposure.currencies.length === 0) {
    root.innerHTML = `${banner}
      <h2>Outstanding</h2>
      <p class="good">No customer money is outstanding.</p>`;
    return;
  }

  // One block per currency, never summed. Adding ₦18.2M to $4,000 would be the
  // most quotable wrong number in the product.
  const blocks = exposure.currencies
    .map((c: CurrencyExposure) => {
      const bands = table(
        ["Age", "Transactions", "Amount"],
        c.by_age.map((b) => [esc(b.band.replaceAll("_", " ")), String(b.transactions), money(b.amount_minor, c.currency)]),
      );
      const unresolvedNote =
        c.unresolved.transactions > 0
          ? `<p class="note">${c.unresolved.transactions} of these are unresolved
             (${money(c.unresolved.amount_minor, c.currency)}) — we could not establish
             what happened, so they may be perfectly fine.</p>`
          : "";
      return `<section>
        <h3>${esc(c.currency)}</h3>
        <div class="cards">
          ${card("Outstanding", money(c.amount_minor, c.currency))}
          ${card("Customers affected", String(c.customers_affected))}
          ${card("Transactions", String(c.transactions))}
          ${card("Oldest", `${c.oldest_age_days} days`, when(c.oldest_debit_at))}
        </div>
        ${unresolvedNote}
        ${bands}
      </section>`;
    })
    .join("");

  root.innerHTML = `${banner}
    <h2>Outstanding</h2>
    <p class="note">${esc(exposure.notice)}</p>
    ${blocks}`;
};

// --- Transactions ---

const OPEN_STATES = [
  "orphaned",
  "reversal_pending",
  "suspect",
  "reversal_failed",
  "pending_debit",
  "completed",
];

export const transactions: View = async (root) => {
  const chosen = (root.dataset.status ??= "orphaned");

  const tabs = OPEN_STATES.map(
    (s) =>
      `<button class="tab ${s === chosen ? "tab-on" : ""}" data-status="${esc(s)}">${esc(
        s.replaceAll("_", " "),
      )}</button>`,
  ).join("");

  root.innerHTML = `<h2>Transactions</h2><div class="tabs">${tabs}</div><div class="loading">Loading…</div>`;

  root.querySelectorAll<HTMLButtonElement>(".tab").forEach((b) =>
    b.addEventListener("click", () => {
      root.dataset.status = b.dataset.status!;
      void transactions(root);
    }),
  );

  const { transactions: rows } = await api.transactions(chosen);
  const body = table(
    ["Transaction", "Status", "Amount", "Debited", "Detected"],
    rows.map((t: Transaction) => [
      `<code>${esc(t.transaction_id)}</code>`,
      status(t.status),
      money(t.amount_minor, t.currency),
      esc(when(t.debit_at)),
      esc(when(t.detected_at)),
    ]),
  );
  root.querySelector(".loading")!.outerHTML = body;
};

// --- Compliance ---

export const compliance: View = async (root) => {
  const r = await api.compliance();

  // The caveats go above the numbers, not below them: a reader who stops after
  // the headline must not have missed that it is a lower bound.
  const caveats = [
    r.incomplete ? `<div class="banner banner-bad">${esc(r.notice ?? "This report is incomplete.")}</div>` : "",
    r.truncated
      ? `<div class="banner banner-warn">The itemised list is capped. The counts above it are exact.</div>`
      : "",
  ].join("");

  root.innerHTML = `<h2>Reversal compliance</h2>
    ${caveats}
    <p class="note">${esc(when(r.from))} to ${esc(when(r.to))},
       against a ${r.reversal_deadline_seconds}s deadline.</p>
    <div class="cards">
      ${card("Within deadline", String(r.compliance.within_deadline))}
      ${card("Breached", String(r.compliance.breached))}
      ${card("Outstanding", String(r.compliance.outstanding), "neither yet")}
      ${card("Rate", percent(r.compliance.rate), "of concluded reversals")}
    </div>
    <div class="cards">
      ${card("Detection p50", seconds(r.detection_latency.p50_seconds))}
      ${card("Detection p95", seconds(r.detection_latency.p95_seconds))}
      ${card("Detection max", seconds(r.detection_latency.max_seconds))}
      ${card("Samples", String(r.detection_latency.samples))}
    </div>
    <h3>Breaches (${r.breaches.length})</h3>
    ${table(
      ["Transaction", "Status", "Amount", "Elapsed", "Why"],
      r.breaches.map((b) => [
        `<code>${esc(b.transaction_id)}</code>`,
        status(b.status),
        money(b.amount_minor, b.currency),
        esc(seconds(b.elapsed_seconds)),
        esc(b.reason),
      ]),
    )}
    <p class="downloads">
      <a href="/v1/reports/reversal-compliance?format=csv" data-auth-download>Download CSV</a>
      <a href="/v1/reports/reversal-compliance?format=pdf" data-auth-download>Download PDF</a>
    </p>`;
};

// --- Rails ---

export const rails: View = async (root) => {
  const [score, fit] = await Promise.all([api.scorecard(), api.windowFit()]);

  const scoreRows = score.providers.map((p) => [
    esc(p.provider),
    String(p.transactions),
    percent(p.failure_rate) + (p.low_sample ? ` <span class="thin">thin sample</span>` : ""),
    esc(seconds(p.settlement_latency.p95_seconds)),
    esc(p.verdict),
  ]);

  const fitRows = fit.rails.map((f) => [
    esc(f.provider),
    `${f.window_seconds}s`,
    esc(seconds(f.observed_p95_seconds)),
    f.recommended_window_seconds ? `${f.recommended_window_seconds}s` : "—",
    f.too_tight ? `<span class="bad">${esc(f.verdict)}</span>` : esc(f.verdict),
  ]);

  root.innerHTML = `<h2>Rails</h2>
    <p class="note">${esc(score.scope)}</p>
    ${table(["Rail", "Transactions", "Failure rate", "Settlement p95", "Verdict"], scoreRows)}
    <h3>Window fit</h3>
    <p class="note">${esc(fit.notice)}</p>
    ${table(["Rail", "Window", "Observed p95", "Recommended", "Verdict"], fitRows)}`;
};

// --- Audit ---

export const audit: View = async (root) => {
  const [v, endpoints] = await Promise.all([api.audit(), api.endpoints()]);

  // An empty chain verifies trivially, and calling that a pass would claim an
  // audit trail is intact when there is no audit trail. A fresh deployment sees
  // this, and it is the moment it most matters to say so plainly.
  const chain =
    v.records === 0
      ? `<div class="banner banner-warn">No audit records yet. Nothing has been
         verified because nothing has been written.</div>`
      : v.verified
        ? `<div class="banner banner-good">The chain verifies. ${v.records} records.</div>`
        : `<div class="banner banner-bad">The chain does not verify${
            v.broken_at ? ` — broken at record ${v.broken_at}` : ""
          }. ${esc(v.reason ?? "")}</div>`;

  // An unsigned chain is not a pass. It means the strongest guarantee is simply
  // absent, and saying nothing would read as everything being fine.
  const checkpoint = v.checkpoint.checked
    ? v.checkpoint.matches
      ? `<div class="banner banner-good">Signed at record ${v.checkpoint.seq},
         ${esc(when(v.checkpoint.taken_at))}. A rewrite of the whole chain would be detected.</div>`
      : `<div class="banner banner-bad">${esc(v.checkpoint.reason ?? "The signature does not match.")}</div>`
    : `<div class="banner banner-warn">${esc(
        v.checkpoint.reason ?? "No checkpoint has been signed.",
      )}</div>`;

  root.innerHTML = `<h2>Audit</h2>
    ${chain}
    ${checkpoint}
    <h3>Delivery endpoints</h3>
    ${table(
      ["Endpoint", "URL", "Events", "Enabled"],
      endpoints.endpoints.map((e) => [
        `<code>${esc(e.id)}</code>`,
        `<code>${esc(e.url)}</code>`,
        esc(e.events.length ? e.events.join(", ") : "all"),
        e.enabled ? "yes" : `<span class="bad">no</span>`,
      ]),
    )}
    ${
      endpoints.endpoints.filter((e) => e.enabled).length === 0
        ? `<div class="banner banner-warn">No enabled endpoint. Reversals are detected
           and recorded, but nobody is told.</div>`
        : ""
    }`;
};
