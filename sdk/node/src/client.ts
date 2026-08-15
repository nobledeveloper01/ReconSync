// The client, and the reporter that keeps it off your payment path.

export type CreditStatus = "success" | "failed" | "unknown";

export interface Debit {
  transaction_id: string;
  idempotency_key?: string;
  transaction_type: string;
  provider?: string;
  amount_minor: number;
  currency: string;
  debit_at?: string;
  customer_ref: string;
  metadata?: Record<string, unknown>;
  expected_credit_minor?: number;
  backfill?: boolean;
}

export interface Credit {
  transaction_id: string;
  idempotency_key?: string;
  credit_at?: string;
  provider_reference?: string;
  status: CreditStatus;
  amount_minor?: number;
  currency?: string;
}

export interface Accepted {
  status: string;
  transaction_id: string;
  expected_completion_at: string;
  window_seconds: number;
}

export class ApiError extends Error {
  readonly statusCode: number;
  readonly code: string;
  readonly field?: string;
  readonly requestId?: string;

  constructor(statusCode: number, code: string, message: string, field?: string, requestId?: string) {
    super(message);
    this.name = "ApiError";
    this.statusCode = statusCode;
    this.code = code;
    this.field = field;
    this.requestId = requestId;
  }

  /** Whether sending the same request again could succeed. */
  get retryable(): boolean {
    return this.statusCode === 429 || this.statusCode >= 500;
  }
}

export interface ClientOptions {
  /** Per-attempt timeout. Short on purpose: this call sits beside a payment. */
  timeoutMs?: number;
  maxAttempts?: number;
  userAgent?: string;
  fetch?: typeof globalThis.fetch;
}

export class ReconSyncClient {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly timeoutMs: number;
  private readonly maxAttempts: number;
  private readonly userAgent: string;
  private readonly doFetch: typeof globalThis.fetch;

  constructor(baseUrl: string, apiKey: string, options: ClientOptions = {}) {
    const trimmed = baseUrl.trim().replace(/\/+$/, "");
    if (!trimmed) throw new Error("reconsync: base URL is required");
    if (!/^https?:\/\//.test(trimmed)) {
      throw new Error(`reconsync: base URL must start with http:// or https://, got ${baseUrl}`);
    }
    if (!apiKey) throw new Error("reconsync: api key is required");

    this.baseUrl = trimmed;
    this.apiKey = apiKey;
    this.timeoutMs = options.timeoutMs ?? 5000;
    this.maxAttempts = options.maxAttempts ?? 3;
    this.userAgent = options.userAgent ?? "reconsync-node";
    this.doFetch = options.fetch ?? globalThis.fetch;

    if (typeof this.doFetch !== "function") {
      throw new Error("reconsync: no fetch available; use Node 18+ or pass options.fetch");
    }
  }

  async reportDebit(debit: Debit): Promise<Accepted> {
    if (!debit.transaction_id) throw new Error("reconsync: transaction_id is required");
    return this.post<Accepted>("/v1/events/debit", {
      ...debit,
      // Derived rather than random, so a retry after a network timeout cannot
      // register the same debit twice.
      idempotency_key: debit.idempotency_key ?? `debit-${debit.transaction_id}`,
      debit_at: debit.debit_at ?? new Date().toISOString(),
    });
  }

  async reportCredit(credit: Credit): Promise<void> {
    if (!credit.transaction_id) throw new Error("reconsync: transaction_id is required");
    if (!credit.status) throw new Error("reconsync: status is required: success, failed or unknown");
    await this.post<unknown>("/v1/events/credit", {
      ...credit,
      // Keyed on the verdict too: a transaction can legitimately go unknown and
      // then succeed, and those are two distinct events.
      idempotency_key: credit.idempotency_key ?? `credit-${credit.transaction_id}-${credit.status}`,
      credit_at: credit.credit_at ?? new Date().toISOString(),
    });
  }

  /**
   * Close the loop after you have reversed. Without it the transaction stays
   * outstanding on the compliance report forever — ReconSync advised the
   * reversal but never saw it happen.
   */
  async reportReversalCompleted(transactionId: string, completedAt?: string): Promise<void> {
    await this.post<unknown>("/v1/events/reversal-completed", {
      transaction_id: transactionId,
      completed_at: completedAt ?? new Date().toISOString(),
    });
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      if (attempt > 1) {
        // Exponential with jitter: without jitter a fleet that all timed out on
        // the same server retries in lockstep and keeps it down.
        const base = 100 * 2 ** (attempt - 2);
        await sleep(base + Math.random() * base * 0.25);
      }

      try {
        return await this.attempt<T>(path, body);
      } catch (err) {
        lastError = err;
        if (err instanceof ApiError && !err.retryable) throw err;
      }
    }
    throw lastError;
  }

  private async attempt<T>(path: string, body: unknown): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const res = await this.doFetch(`${this.baseUrl}${path}`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.apiKey}`,
          "Content-Type": "application/json",
          "User-Agent": this.userAgent,
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      if (!res.ok) throw await toApiError(res);
      if (res.status === 204) return undefined as T;

      const text = await res.text();
      return (text ? JSON.parse(text) : undefined) as T;
    } finally {
      clearTimeout(timer);
    }
  }
}

async function toApiError(res: Response): Promise<ApiError> {
  let code = `http_${res.status}`;
  let message = res.statusText || `HTTP ${res.status}`;
  let field: string | undefined;
  let requestId = res.headers.get("x-request-id") ?? undefined;

  try {
    // A proxy returning HTML is a real case, so a body that will not parse
    // leaves the status-derived message rather than masking what happened.
    const parsed = (await res.json()) as { error?: Record<string, string> };
    if (parsed?.error?.code) {
      code = parsed.error.code;
      message = parsed.error.message ?? message;
      field = parsed.error.field;
      requestId = parsed.error.request_id ?? requestId;
    }
  } catch {
    /* not JSON */
  }
  return new ApiError(res.status, code, message, field, requestId);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- the reporter ---

export interface ReporterOptions {
  bufferSize?: number;
  /** Called when the buffer is full and a report is discarded. Alert on this. */
  onDrop?: (kind: string, transactionId: string) => void;
  /** Called when a report was sent and refused, after retries. */
  onError?: (kind: string, transactionId: string, error: unknown) => void;
}

export interface ReporterStats {
  sent: number;
  failed: number;
  dropped: number;
  queued: number;
}

/**
 * Reports transactions without ever standing in the way of one.
 *
 * The naive integration — `await client.reportDebit(d)` inside the transfer —
 * makes a reconciliation service having a bad afternoon into a reason the
 * customer's payment fails. That is strictly worse than not reconciling it. So
 * these methods return `void` immediately and a full buffer drops the report.
 *
 * It does not drop silently. A dropped debit is a transaction ReconSync will
 * never see and so can never detect the failure of. Wire `onDrop` to an alert.
 */
export class Reporter {
  private readonly client: ReconSyncClient;
  private readonly queue: Array<() => Promise<void>> = [];
  private readonly bufferSize: number;
  private readonly onDrop?: (kind: string, transactionId: string) => void;
  private readonly onError?: (kind: string, transactionId: string, error: unknown) => void;

  private draining = false;
  private closed = false;
  private stats: ReporterStats = { sent: 0, failed: 0, dropped: 0, queued: 0 };
  private idle: Promise<void> = Promise.resolve();
  private resolveIdle: (() => void) | null = null;

  constructor(client: ReconSyncClient, options: ReporterOptions = {}) {
    this.client = client;
    this.bufferSize = options.bufferSize ?? 1024;
    this.onDrop = options.onDrop;
    this.onError = options.onError;
  }

  reportDebit(debit: Debit): boolean {
    return this.enqueue("debit", debit.transaction_id, () => this.client.reportDebit(debit).then(() => undefined));
  }

  reportCredit(credit: Credit): boolean {
    return this.enqueue("credit", credit.transaction_id, () => this.client.reportCredit(credit));
  }

  reportReversalCompleted(transactionId: string, completedAt?: string): boolean {
    return this.enqueue("reversal", transactionId, () =>
      this.client.reportReversalCompleted(transactionId, completedAt),
    );
  }

  getStats(): ReporterStats {
    return { ...this.stats, queued: this.queue.length };
  }

  /** Stop accepting reports and wait for the queue to drain. */
  async close(timeoutMs = 5000): Promise<void> {
    this.closed = true;
    const deadline = Date.now() + timeoutMs;

    while (this.queue.length > 0 || this.draining) {
      if (Date.now() > deadline) {
        // Said out loud: a queue silently lost at every rolling restart is a
        // gap in the record that nobody would ever notice.
        throw new Error(`reconsync: closed with ${this.queue.length} reports still queued`);
      }
      await sleep(10);
    }
  }

  private enqueue(kind: string, transactionId: string, task: () => Promise<void>): boolean {
    if (this.closed || this.queue.length >= this.bufferSize) {
      this.stats.dropped++;
      this.onDrop?.(kind, transactionId);
      return false;
    }

    this.queue.push(async () => {
      try {
        await task();
        this.stats.sent++;
      } catch (err) {
        this.stats.failed++;
        this.onError?.(kind, transactionId, err);
      }
    });

    void this.drain();
    return true;
  }

  private async drain(): Promise<void> {
    if (this.draining) return;
    this.draining = true;

    try {
      while (this.queue.length > 0) {
        const task = this.queue.shift();
        if (task) await task();
      }
    } finally {
      this.draining = false;
      this.resolveIdle?.();
    }
  }
}
