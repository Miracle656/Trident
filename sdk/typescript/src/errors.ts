import { z } from "zod";

/**
 * Machine-readable codes the Trident API can return in an error envelope's
 * `error.code` field, kept in sync with the catalogue documented in
 * docs/errors.md and services/api/internal/httputil/registry.go. Branch on
 * `TridentApiError.code` using this type rather than matching on `message`,
 * which is not stable across releases.
 */
export type ApiErrorCode =
  | "INVALID_ARGUMENT"
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "PAYLOAD_TOO_LARGE"
  | "RATE_LIMITED"
  | "INTERNAL"
  | "UNAVAILABLE";

/**
 * Client-side error codes raised by the SDK itself (config problems,
 * iteration limits, retry exhaustion) rather than parsed from a server
 * response. Distinct from {@link ApiErrorCode}, which is the server-defined
 * catalogue.
 */
export type TridentErrorCode =
  | "CONFIG"
  | "NOT_FOUND"
  | "UNAUTHORIZED"
  | "RATE_LIMITED"
  | "INVALID_ARGUMENT"
  | "TIMEOUT"
  | "INTERNAL"
  | "ITERATION_LIMIT"
  | "RETRY_EXHAUSTED";

export class TridentError extends Error {
  readonly code: TridentErrorCode;
  readonly cause?: unknown;
  /** Number of attempts made before this error was thrown (>1 if retried). */
  attempts: number;

  constructor(code: TridentErrorCode, message: string, cause?: unknown) {
    super(message);
    this.name = "TridentError";
    this.code = code;
    this.cause = cause;
    this.attempts = 1;
  }
}

/**
 * Structured error thrown by SDK methods on all non-2xx API responses.
 * Carries the HTTP status code, machine-readable error code, human-readable
 * message, and the optional field that caused a validation failure.
 */
export class TridentApiError extends Error {
  readonly status: number;
  /**
   * Typed as `ApiErrorCode | (string & {})`: editors autocomplete and
   * type-check against the documented catalogue, but a server response
   * carrying a code newer than this SDK build still deserializes instead of
   * failing, so a client can safely fall through to a default case on an
   * unrecognized value rather than crash.
   */
  readonly code: ApiErrorCode | (string & {});
  readonly field?: string;
  /** Structured, code-specific context from the envelope's `error.details`. */
  readonly details?: Record<string, unknown>;
  /** Number of attempts made before this error was thrown (>1 if retried). */
  attempts: number;

  constructor(
    status: number,
    code: ApiErrorCode | (string & {}),
    message: string,
    field?: string,
    details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = "TridentApiError";
    this.status = status;
    this.code = code;
    this.field = field;
    this.details = details;
    this.attempts = 1;
  }
}

const ApiErrorEnvelopeSchema = z.object({
  error: z.object({
    code: z.string(),
    message: z.string(),
    field: z.string().optional(),
    details: z.record(z.string(), z.unknown()).optional(),
  }),
});

/**
 * Parse a non-2xx response body into a TridentApiError.
 * Falls back to code="INTERNAL" when the body is not a valid error envelope.
 */
export function parseApiError(status: number, body: string): TridentApiError {
  try {
    const parsed = ApiErrorEnvelopeSchema.parse(JSON.parse(body));
    const { code, message, field, details } = parsed.error;
    return new TridentApiError(status, code, message, field, details);
  } catch {
    return new TridentApiError(status, "INTERNAL", body || `HTTP ${status}`);
  }
}

/** @deprecated Use parseApiError for structured errors from the API. */
export function httpStatusToError(
  status: number,
  body: string,
): TridentError {
  switch (status) {
    case 401:
      return new TridentError("UNAUTHORIZED", body || "Unauthorized");
    case 404:
      return new TridentError("NOT_FOUND", body || "Not found");
    case 429:
      return new TridentError("RATE_LIMITED", body || "Rate limit exceeded");
    default:
      return new TridentError("INTERNAL", body || `HTTP ${status}`);
  }
}
