// The API client.
//
// The dashboard is served by the ReconSync binary itself, from the same origin,
// so every request here is a relative path. That is deliberate: a dashboard on
// another origin would need CORS opened on an API that advises money movement,
// and would put the credential through a cross-origin request. Same origin
// means neither.

export class ApiError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

/**
 * Where an API key lives while the tab is open.
 *
 * A signed-in person does not use this at all — their session is an HttpOnly
 * cookie this code cannot read, which is the point. The key path remains for a
 * deployment that has no user accounts, and for reading a tenant with a
 * service credential.
 */
const KEY_STORAGE = "reconsync.key";

export function storedKey(): string | null {
  return sessionStorage.getItem(KEY_STORAGE);
}

export function storeKey(key: string): void {
  // sessionStorage, not localStorage: the key is cleared when the tab closes.
  // A key that outlives the session on a shared machine is a credential left
  // lying around, and this one can read every transaction a tenant has.
  sessionStorage.setItem(KEY_STORAGE, key);
}

export function clearKey(): void {
  sessionStorage.removeItem(KEY_STORAGE);
}

/** Reads the CSRF cookie the server set alongside the session. */
function csrfToken(): string {
  const match = document.cookie.match(/(?:^|;\s*)reconsync_csrf=([^;]*)/);
  return match ? decodeURIComponent(match[1]) : "";
}

function headers(withBody: boolean): Record<string, string> {
  const out: Record<string, string> = {};
  if (withBody) out["Content-Type"] = "application/json";

  const csrf = csrfToken();
  if (csrf) out["X-ReconSync-CSRF"] = csrf;

  // Only when there is no session. A stale key alongside a live session would
  // otherwise be ambiguous, and the server prefers the cookie anyway.
  const key = storedKey();
  if (key && !csrf) out["Authorization"] = `Bearer ${key}`;
  return out;
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: headers(body !== undefined),
    // The session cookie rides along; without this fetch would omit it.
    credentials: "same-origin",
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (!res.ok) {
    // The API returns a structured error; fall back to the status when it does
    // not, so a proxy returning HTML does not surface as "unexpected token <".
    let code = "error";
    let message = `${res.status} ${res.statusText}`;
    try {
      const parsed = await res.json();
      if (parsed?.error) {
        code = parsed.error.code ?? code;
        message = parsed.error.message ?? message;
      }
    } catch {
      /* not JSON */
    }
    throw new ApiError(res.status, code, message);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

function request<T>(path: string): Promise<T> {
  return call<T>("GET", path);
}

// --- shapes, mirroring the Go types the API actually returns ---

export interface Transaction {
  transaction_id: string;
  status: string;
  transaction_type: string;
  provider?: string;
  amount_minor: number;
  currency: string;
  debit_at: string;
  credit_at?: string;
  expected_completion_at: string;
  detected_at?: string;
  reversal_completed_at?: string;
}

export interface CurrencyExposure {
  currency: string;
  transactions: number;
  customers_affected: number;
  amount_minor: number;
  oldest_debit_at: string;
  oldest_age_days: number;
  unresolved: { transactions: number; amount_minor: number };
  by_age: { band: string; transactions: number; amount_minor: number }[];
}

export interface Exposure {
  tenant_id: string;
  as_of: string;
  scope: string;
  currencies: CurrencyExposure[];
  notice: string;
}

export interface Compliance {
  tenant_id: string;
  from: string;
  to: string;
  reversal_deadline_seconds: number;
  totals: Record<string, number>;
  detection_latency: { samples: number; p50_seconds: number; p95_seconds: number; max_seconds: number };
  compliance: { within_deadline: number; breached: number; outstanding: number; rate?: number };
  breaches: {
    transaction_id: string;
    amount_minor: number;
    currency: string;
    status: string;
    elapsed_seconds: number;
    reason: string;
  }[];
  truncated?: boolean;
  incomplete?: boolean;
  notice?: string;
}

export interface Scorecard {
  tenant_id: string;
  scope: string;
  providers: {
    provider: string;
    transactions: number;
    settled: number;
    failed: number;
    unresolved: number;
    failure_rate?: number;
    low_sample?: boolean;
    settlement_latency: { samples: number; p50_seconds: number; p95_seconds: number; max_seconds: number };
    verdict: string;
  }[];
}

export interface WindowFit {
  rails: {
    provider: string;
    window_seconds: number;
    observed_p95_seconds: number;
    settled_samples: number;
    recommended_window_seconds?: number;
    too_tight: boolean;
    verdict: string;
  }[];
  notice: string;
}

export interface AuditVerification {
  tenant_id: string;
  records: number;
  verified: boolean;
  broken_at?: number;
  reason?: string;
  last_hash?: string;
  checkpoint: {
    checked: boolean;
    seq?: number;
    taken_at?: string;
    matches: boolean;
    public_key?: string;
    reason?: string;
  };
}

export interface Licence {
  licensed: boolean;
  customer?: string;
  plan?: string;
  expires_at?: string;
  expired?: boolean;
  days_remaining?: number;
  notice?: string;
}

export interface Endpoint {
  id: string;
  url: string;
  events: string[];
  enabled: boolean;
}

export interface Session {
  email: string;
  role: string;
  tenant_id: string;
  scopes: string[];
  totp_enabled: boolean;
  csrf_token: string;
}

export interface UserRow {
  id: string;
  email: string;
  role: string;
  totp_enabled: boolean;
  disabled: boolean;
  last_login_at?: string;
  created_at: string;
}

export interface BrowserSession {
  user_agent: string;
  ip: string;
  created_at: string;
  last_seen_at: string;
  current: boolean;
}

/** True when the signed-in role holds a scope. Mirrors the server exactly. */
export function can(session: Session | null, scope: string): boolean {
  if (!session) return false;
  // An empty list means an unscoped API key, which has full access.
  return session.scopes.length === 0 || session.scopes.includes(scope);
}

export const auth = {
  login: (email: string, password: string, code?: string) =>
    call<Session | { totp_required: true; user_id: string }>("POST", "/v1/auth/login", {
      email,
      password,
      ...(code ? { code } : {}),
    }),
  me: () => request<Session>("/v1/auth/me"),
  logout: () => call<unknown>("POST", "/v1/auth/logout", {}),
  changePassword: (current_password: string, new_password: string) =>
    call<unknown>("POST", "/v1/auth/password", { current_password, new_password }),
  beginTOTP: () =>
    call<{ secret: string; uri: string; qr: string; notice: string }>("POST", "/v1/auth/totp/begin", {}),
  confirmTOTP: (code: string) =>
    call<{ recovery_codes: string[]; notice: string }>("POST", "/v1/auth/totp/confirm", { code }),
  disableTOTP: (password: string) => call<unknown>("POST", "/v1/auth/totp/disable", { password }),
  sessions: () => request<{ sessions: BrowserSession[] }>("/v1/auth/sessions"),
  revokeSessions: () => call<unknown>("DELETE", "/v1/auth/sessions"),
  completeReset: (token: string, password: string) =>
    call<unknown>("POST", "/v1/auth/reset", { token, password }),
};

export const users = {
  list: () => request<{ users: UserRow[] }>("/v1/users"),
  create: (email: string, password: string, role: string) =>
    call<UserRow>("POST", "/v1/users", { email, password, role }),
  update: (id: string, patch: { role?: string; disabled?: boolean }) =>
    call<unknown>("PATCH", `/v1/users/${encodeURIComponent(id)}`, patch),
  issueReset: (id: string) =>
    call<{ reset_token: string; expires_at: string; notice: string }>(
      "POST",
      `/v1/users/${encodeURIComponent(id)}/reset`,
      {},
    ),
};

export const api = {
  transactions: (status: string, limit = 100) =>
    request<{ transactions: Transaction[] }>(
      `/v1/transactions?status=${encodeURIComponent(status)}&limit=${limit}`,
    ),
  exposure: (scope: string) => request<Exposure>(`/v1/reports/exposure?scope=${scope}`),
  compliance: () => request<Compliance>("/v1/reports/reversal-compliance"),
  scorecard: () => request<Scorecard>("/v1/reports/providers"),
  windowFit: () => request<WindowFit>("/v1/reports/window-fit"),
  audit: () => request<AuditVerification>("/v1/audit/verify"),
  licence: () => request<Licence>("/v1/licence"),
  endpoints: () => request<{ endpoints: Endpoint[] }>("/v1/webhooks"),
};
