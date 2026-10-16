export const policies = ["never", "always", "pre-visible", "adaptive", "resume"] as const;
export type Policy = (typeof policies)[number];
export type Scenario = "before" | "after" | "late" | "healthy";
export type Source = "scripted" | "ollama";
export type Frame = { type: "generated"; units?: number } | { type: "done" } | { type: "delta"; text: string }
  | { type: "error"; message: string; retryable?: boolean; retryAfterSeconds?: number }
  | { type: "work-limit"; limit: number } | { type: "usage"; output: number };

export type Attempt = {
  chunks: string[]; interrupted: boolean; cursor?: number;
  outputLimit?: number; observedWork?: number; failure?: string;
};
export type Phase = "ready" | "streaming" | "retrying" | "complete" | "failed" | "stopped" | "deadline";
export type Lane = { phase: Phase; attempts: Attempt[]; generated: number; error?: string; decision?: string; firstTextMs?: number; outcomeMs?: number };
type Read = (attempt: number, emit: (frame: Frame) => void, signal: AbortSignal, cursor?: number) => Promise<void>;
type RetryWindow = { deadlineMs: number; outputBudget?: number; now?: () => number };
const MIN_RESTART_RESERVE_MS = 4_000;

export const emptyLane = (): Lane => ({ phase: "ready", attempts: [], generated: 0 });

export function hasVisibleText(attempt: Attempt | undefined): boolean {
  return attempt?.chunks.some((chunk) => /\S/.test(chunk)) ?? false;
}

export function repeatedPrefix(lane: Pick<Lane, "attempts">): boolean {
  if (lane.attempts[1]?.cursor !== undefined) return false;
  const first = lane.attempts[0]?.chunks.join("");
  const second = lane.attempts[1]?.chunks.join("");
  return !!first?.trim() && !!second?.startsWith(first);
}

function pause(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    signal.addEventListener("abort", abort, { once: true });
  });
}

export async function comparePolicy(
  policy: Policy,
  read: Read,
  update: (lane: Lane) => void,
  signal: AbortSignal,
  retryDelay = 650,
  window: RetryWindow = { deadlineMs: 9_000 },
): Promise<void> {
  const lane = emptyLane();
  const now = window.now ?? (() => performance.now());
  const started = now();
  const phase = (next: Phase) => {
    lane.phase = next;
    if (next === "complete" || next === "failed" || next === "stopped" || next === "deadline") {
      lane.outcomeMs = now() - started;
    }
    update(lane);
  };
  const abortPhase = () => phase(signal.reason === "deadline" ? "deadline" : "stopped");

  for (let number = 1; number <= 2; number++) {
    if (signal.aborted) return abortPhase();
    const attempt: Attempt = { chunks: [], interrupted: false };
    if (number === 2 && policy === "resume") attempt.cursor = lane.attempts[0]!.chunks.length;
    lane.attempts.push(attempt);
    phase("streaming");

    try {
      let complete = false;
      await read(number, (frame) => {
        if (frame.type === "error") throw new SourceFailure(frame.message, frame.retryable, frame.retryAfterSeconds);
        if (frame.type === "work-limit") attempt.outputLimit = frame.limit;
        if (frame.type === "usage") attempt.observedWork = frame.output;
        if (frame.type === "generated") {
          lane.generated++;
          if (frame.units !== undefined) attempt.observedWork = (attempt.observedWork ?? 0) + frame.units;
        }
        if (frame.type === "delta") {
          attempt.chunks.push(frame.text);
          if (lane.firstTextMs === undefined && /\S/.test(frame.text)) lane.firstTextMs = now() - started;
        }
        if (frame.type === "done") complete = true;
        update(lane);
      }, signal, attempt.cursor);
      if (signal.aborted) return abortPhase();
      if (!complete) throw new Error("Stream ended without completion");
      return phase("complete");
    } catch (error) {
      if (signal.aborted) return abortPhase();
      attempt.interrupted = true;
      const source = error instanceof SourceFailure ? error : undefined;
      if (source) attempt.failure = source.message;
      const fail = () => {
        lane.error = source?.message;
        return phase("failed");
      };
      if ((source && !source.retryable) || number !== 1 || policy === "never" ||
          (policy === "pre-visible" && hasVisibleText(attempt))) return fail();
      const elapsed = now() - started;
      const remaining = window.deadlineMs - elapsed;
      const wait = Math.max(retryDelay, (source?.retryAfterSeconds ?? 0) * 1_000);
      if (policy === "adaptive") {
        const limit = attempt.outputLimit;
        const budget = window.outputBudget;
        if (limit === undefined || budget === undefined) {
          lane.decision = "Restart skipped: work limit unavailable.";
        } else if ((attempt.observedWork ?? limit) + limit > budget) {
          const required = (attempt.observedWork ?? limit) + limit;
          const unreported = attempt.observedWork === undefined ? " (first attempt unreported)" : "";
          lane.decision = `Restart skipped: needs ${required}, budget ${budget}${unreported}.`;
        } else if (remaining < wait + Math.max(elapsed, MIN_RESTART_RESERVE_MS)) {
          lane.decision = "Restart skipped: not enough time left.";
        }
        if (lane.decision) return fail();
      }
      if (source?.retryAfterSeconds !== undefined && remaining <= wait) {
        lane.decision = "Restart skipped: retry delay exceeds the deadline.";
        return fail();
      }
      phase("retrying");
      try {
        await pause(wait, signal);
      } catch {
        return abortPhase();
      }
    }
  }
}

class SourceFailure extends Error {
  constructor(message: string, readonly retryable = false, readonly retryAfterSeconds?: number) {
    super(message);
  }
}

export async function readStream(
  scenario: Scenario,
  attempt: number,
  emit: (frame: Frame) => void,
  signal: AbortSignal,
  source: Source = "scripted",
  cursor?: number,
): Promise<void> {
  const query = new URLSearchParams({ scenario, attempt: String(attempt), source });
  if (cursor !== undefined) query.set("cursor", String(cursor));
  return readNdjson<Frame>(`/v1/chat/completions/stream?${query}`, emit, signal, (frame) => frame.type === "done");
}

export async function readNdjson<Event>(
  url: string,
  emit: (event: Event) => void,
  signal: AbortSignal,
  isTerminal?: (event: Event) => boolean,
  method: "GET" | "POST" = "POST",
): Promise<void> {
  const response = await fetch(url, { method, signal });
  if (!response.ok || !response.body) {
    const delay = Number(response.headers.get("Retry-After"));
    throw new SourceFailure(`HTTP ${response.status}`,
      response.status === 408 || response.status === 429 || response.status >= 500,
      Number.isFinite(delay) && delay > 0 ? delay : undefined);
  }

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += value;
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line) continue;
        let frame: Event;
        try {
          frame = JSON.parse(line) as Event;
        } catch {
          throw new SourceFailure("Invalid stream data.");
        }
        emit(frame);
        if (isTerminal?.(frame)) return;
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}
