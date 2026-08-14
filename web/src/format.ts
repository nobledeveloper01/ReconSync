// Formatting, with the decisions that matter for money and time in one place.

/** Minor units, grouped, with the currency named. */
export function money(minor: number, currency: string): string {
  // Never converted to major units. The number of minor units per unit differs
  // by currency — JPY has none — and this system does not track the exponent,
  // so a naive divide-by-100 would misstate some currencies by a hundredfold.
  // Grouping makes it readable without claiming to know the scale.
  return `${currency} ${minor.toLocaleString("en-US")}`;
}

export function seconds(v: number): string {
  if (v < 60) return `${v.toFixed(1)}s`;
  if (v < 3600) return `${(v / 60).toFixed(1)}m`;
  if (v < 86400) return `${(v / 3600).toFixed(1)}h`;
  return `${(v / 86400).toFixed(1)}d`;
}

export function when(iso?: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toISOString().replace("T", " ").slice(0, 19) + "Z";
}

export function percent(rate?: number): string {
  // Absent is not zero: "no data" and "0%" are different claims, and the API
  // omits the field rather than sending zero for exactly that reason.
  return rate === undefined || rate === null ? "—" : `${(rate * 100).toFixed(1)}%`;
}

/** Escapes a value for insertion into HTML. */
export function esc(v: unknown): string {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
