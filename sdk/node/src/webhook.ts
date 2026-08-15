import { createHmac, timingSafeEqual } from "node:crypto";

// Verifying the signature is the one thing every receiver must get right.
//
// The payload advises reversing a customer's money. A handler that acts on an
// unverified body will act on anything anyone posts to that URL, and the whole
// design — advisory payloads, no credentials in the webhook — rests on the
// receiver checking who sent it.

export const SIGNATURE_HEADER = "x-reconsync-signature";
export const EVENT_HEADER = "x-reconsync-event";
export const DELIVERY_HEADER = "x-reconsync-delivery";
export const DRILL_HEADER = "x-reconsync-drill";

/** How far a signature's timestamp may be from now. */
export const DEFAULT_TOLERANCE_SECONDS = 300;

export class SignatureError extends Error {
  readonly reason: "malformed" | "mismatch" | "expired";

  constructor(reason: "malformed" | "mismatch" | "expired", message: string) {
    super(message);
    this.name = "SignatureError";
    this.reason = reason;
  }
}

/**
 * Verify a webhook signature.
 *
 * `body` must be the **raw bytes as received**. Passing a re-serialised object
 * is the most common way this fails in production: `JSON.parse` then
 * `JSON.stringify` can reorder keys and change whitespace, and the signature
 * covers the exact bytes that were sent. In Express that means
 * `express.raw({ type: "application/json" })` on this route, not `express.json()`.
 */
export function verifySignature(
  secret: string | string[],
  header: string | undefined | null,
  body: Buffer | string,
  options: { nowSeconds?: number; toleranceSeconds?: number } = {},
): void {
  const now = options.nowSeconds ?? Math.floor(Date.now() / 1000);
  const tolerance = options.toleranceSeconds ?? DEFAULT_TOLERANCE_SECONDS;

  const { timestamp, signatures } = parseHeader(header);

  if (tolerance > 0 && Math.abs(now - timestamp) > tolerance) {
    // Bounded replay: a request captured off the wire stops working shortly
    // after it was made, so a recorded reversal cannot be replayed next week.
    throw new SignatureError(
      "expired",
      `signature timestamp is ${Math.abs(now - timestamp)}s away, outside the ${tolerance}s tolerance`,
    );
  }

  const raw = typeof body === "string" ? Buffer.from(body, "utf8") : body;
  // An array accepts a rotation from your side: hold the old and the new while
  // the sender changes over, then drop the old.
  const secrets = Array.isArray(secret) ? secret : [secret];

  let matched = false;
  for (const candidate of secrets) {
    const expected = Buffer.from(
      createHmac("sha256", candidate).update(`${timestamp}.`).update(raw).digest("hex"),
      "utf8",
    );

    for (const provided of signatures) {
      const given = Buffer.from(provided, "utf8");
      // Length first because timingSafeEqual throws on a mismatch; the
      // comparison itself is constant time, so a wrong signature does not
      // reveal how much of it was right. Every pair is checked even after one
      // matches, so the time taken does not depend on which one did.
      if (expected.length === given.length && timingSafeEqual(expected, given)) {
        matched = true;
      }
    }
  }

  if (!matched) {
    throw new SignatureError("mismatch", "signature does not match");
  }
}

/**
 * Reads the timestamp and **every** signature the header carries.
 *
 * Several v1 entries is the normal state while the sender rotates its secret,
 * so they are all collected. Keeping only the last would make the sender's
 * ordering decide whether your receiver worked.
 */
function parseHeader(header: string | undefined | null): {
  timestamp: number;
  signatures: string[];
} {
  if (!header) {
    throw new SignatureError("malformed", `missing ${SIGNATURE_HEADER} header`);
  }

  let timestamp = 0;
  const signatures: string[] = [];
  for (const part of header.split(",")) {
    const [key, value] = part.trim().split("=", 2);
    if (value === undefined) {
      throw new SignatureError("malformed", `cannot parse ${SIGNATURE_HEADER}`);
    }
    if (key === "t") {
      timestamp = Number.parseInt(value, 10);
      if (!Number.isFinite(timestamp)) {
        throw new SignatureError("malformed", "timestamp is not a number");
      }
    } else if (key === "v1" && value) {
      signatures.push(value);
    }
  }

  if (!timestamp || signatures.length === 0) {
    throw new SignatureError("malformed", `${SIGNATURE_HEADER} is missing t or v1`);
  }
  return { timestamp, signatures };
}

// --- payload shapes ---

export type EventType =
  | "reversal.triggered"
  | "reversal.completed"
  | "reversal.failed"
  | "transaction.suspect"
  | "transaction.reconciled"
  | "sla.at_risk"
  | "integration.silent"
  | "integration.recovered";

export interface EvidenceSignal {
  source: string;
  detail?: string;
  weight?: number;
}

export interface WebhookData {
  transaction_id: string;
  amount_minor: number;
  currency: string;
  reason?: string;

  /**
   * Present only when part of the money arrived. Reverse `outstanding_minor`,
   * never `amount_minor` — refunding the full amount when a fifth of it already
   * reached the destination pays the customer twice for that fifth.
   */
  credited_minor?: number;
  outstanding_minor?: number;

  debit_at: string;
  window_seconds: number;
  detected_at?: string;
  regulatory_deadline: string;

  /** Always true. Check your own ledger before moving money. */
  advisory: boolean;

  /** 0 to 1. Set your own bar rather than treating every verdict alike. */
  confidence: number;
  evidence?: EvidenceSignal[];

  seconds_until_breach?: number;

  /** Present only on a fire drill. Acknowledge it and do nothing else. */
  drill?: boolean;
}

export interface WebhookEnvelope {
  event: EventType;
  occurred_at: string;
  data: WebhookData;
}

/** Verify and parse in one step, in the order that keeps you safe. */
export function parseWebhook(
  secret: string | string[],
  header: string | undefined | null,
  body: Buffer | string,
  options?: { nowSeconds?: number; toleranceSeconds?: number },
): WebhookEnvelope {
  verifySignature(secret, header, body, options);
  return JSON.parse(typeof body === "string" ? body : body.toString("utf8")) as WebhookEnvelope;
}
