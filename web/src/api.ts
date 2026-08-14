// The API client.
//
// The dashboard is served by the ReconSync binary itself, from the same origin,
// so every request here is a relative path. That is deliberate: a dashboard on
// another origin would need CORS opened on an API that advises money movement,
// and would put the key through a cross-origin request. Same origin means
// neither.

export class ApiError extends Error {
  status: number;
  code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

/** Where the key lives while the tab is open. */
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

async function request<T>(path: string): Promise<T> {
  const key = storedKey();
  if (!key) throw new ApiError(401, "unauthenticated", "no API key");

  const res = await fetch(path, {
    headers: { Authorization: `Bearer ${key}` },
  });

  if (!res.ok) {
    // The API returns a structured error; fall back to the status when it does
    // not, so a proxy returning HTML does not surface as "unexpected token <".
    let code = "error";
    let message = `${res.status} ${res.statusText}`;
    try {
      const body = await res.json();
      if (body?.error) {
        code = body.error.code ?? code;
        message = body.error.message ?? message;
      }
    } catch {
      /* not JSON */
    }
    throw new ApiError(res.status, code, message);
  }
  return (await res.json()) as T;
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
