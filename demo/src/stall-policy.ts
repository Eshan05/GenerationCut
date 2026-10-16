import type { Attempt, Frame } from "./experiment.js";

export const stallPolicies = ["wait", "keep", "retry"] as const;
export const stallIdleMs = 1_200;
export const stallRestartReserveMs = 2_200;
export type StallPolicy = (typeof stallPolicies)[number];
type Phase = "ready" | "streaming" | "waiting" | "complete" | "kept" | "deadline" | "stopped" | "failed";
export type StallLane = {
  phase: Phase;
  attempts: Attempt[];
  outcomeMs?: number;
  resumed?: boolean;
  restarted?: boolean;
  skipped?: boolean;
  error?: string;
};
type Read = (attempt: number, emit: (event: Frame) => void, signal: AbortSignal) => Promise<void>;
type Timing = { deadlineMs: number; idleMs?: number; reserveMs?: number; now?: () => number };

export const emptyStallLane = (): StallLane => ({ phase: "ready", attempts: [] });

export async function compareStall(
  policy: StallPolicy,
  read: Read,
  update: (lane: StallLane) => void,
  signal: AbortSignal,
  { deadlineMs, idleMs = stallIdleMs, reserveMs = stallRestartReserveMs, now = () => performance.now() }: Timing,
) {
  const lane = emptyStallLane();
  const start = now();
  const setPhase = (phase: Phase) => {
    lane.phase = phase;
    update(lane);
  };
  const finish = (phase: Phase) => {
    lane.outcomeMs = now() - start;
    setPhase(phase);
  };
  const abortPhase = () => signal.reason === "deadline" ? "deadline" : "stopped";

  for (let number = 1; number <= 2; number++) {
    if (signal.aborted) return finish(abortPhase());
    const attempt: Attempt = { chunks: [], interrupted: false };
    const request = new AbortController();
    const abort = () => request.abort(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    let idle: ReturnType<typeof setTimeout> | undefined;
    let done = false;
    let error: unknown;
    lane.attempts.push(attempt);
    setPhase("streaming");

    try {
      await read(number, (event) => {
        if (event.type === "error") throw Error(event.message);
        if (event.type === "done") {
          done = true;
          clearTimeout(idle);
        }
        if (event.type !== "delta") return;
        attempt.chunks.push(event.text);
        if (event.text.trim()) {
          if (number === 1 && lane.phase === "waiting") lane.resumed = true;
          lane.phase = "streaming";
          clearTimeout(idle);
          idle = setTimeout(() => {
            if (policy === "wait") setPhase("waiting");
            else request.abort("idle");
          }, idleMs);
        }
        update(lane);
      }, request.signal);
    } catch (caught) {
      error = caught;
    } finally {
      clearTimeout(idle);
      signal.removeEventListener("abort", abort);
    }

    if (done && !signal.aborted && !request.signal.aborted) return finish("complete");
    attempt.interrupted = true;
    if (signal.aborted) return finish(abortPhase());
    if (request.signal.reason !== "idle") {
      lane.error = error instanceof Error ? error.message : "The stream ended without completing.";
      return finish("failed");
    }
    if (policy !== "retry" || number > 1) return finish("kept");
    if (deadlineMs - (now() - start) < reserveMs) {
      lane.skipped = true;
      return finish("kept");
    }
    lane.restarted = true;
  }
}
