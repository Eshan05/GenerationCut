import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout } from "node:timers/promises";
import { repeatedPrefix } from "../dist/experiment.js";
import { compareStall } from "../dist/stall-policy.js";
import type { Frame } from "../src/experiment.ts";
import type { StallLane, StallPolicy } from "../src/stall-policy.ts";

type Emit = (event: Frame) => void;
type Read = (attempt: number, emit: Emit, signal: AbortSignal) => Promise<void>;
const first = (emit: Emit) => emit({ type: "delta", text: "A cache " });
const waitForAbort = (signal: AbortSignal) => new Promise<never>((_, reject) => {
  if (signal.aborted) reject(signal.reason);
  else signal.addEventListener("abort", () => reject(signal.reason), { once: true });
});

async function run(policy: StallPolicy, read: Read, reserveMs = 50) {
  let lane!: StallLane;
  const phases: string[] = [];
  await compareStall(policy, read, (next) => {
    lane = next;
    phases.push(next.phase);
  }, new AbortController().signal, { idleMs: 2, deadlineMs: 100, reserveMs, now: () => 0 });
  return { lane, phases };
}

test("waiting keeps the connection open through a recoverable pause", async () => {
  const { lane, phases } = await run("wait", async (_, emit, signal) => {
    first(emit);
    await setTimeout(10);
    assert.equal(signal.aborted, false);
    emit({ type: "delta", text: "stores copies." });
    emit({ type: "done" });
  });
  assert.ok(phases.includes("waiting"));
  assert.equal(lane.phase, "complete");
  assert.equal(lane.resumed, true);
  assert.equal(lane.attempts.length, 1);
});

test("the idle policy aborts and keeps the visible answer", async () => {
  const { lane } = await run("keep", async (_, emit, signal) => {
    first(emit);
    await waitForAbort(signal);
  });
  assert.equal(lane.phase, "kept");
  assert.deepEqual(lane.attempts, [{ chunks: ["A cache "], interrupted: true }]);
});

test("retry after silence starts one fresh request and exposes the repeated opening", async () => {
  const { lane } = await run("retry", async (attempt, emit, signal) => {
    first(emit);
    if (attempt === 1) await waitForAbort(signal);
    emit({ type: "delta", text: "stores copies." });
    emit({ type: "done" });
  });
  assert.equal(lane.phase, "complete");
  assert.equal(lane.attempts.length, 2);
  assert.equal(repeatedPrefix(lane), true);
});

test("retry is skipped when the remaining deadline cannot fit another attempt", async () => {
  const { lane } = await run("retry", async (_, emit, signal) => {
    first(emit);
    await waitForAbort(signal);
  }, 101);
  assert.equal(lane.phase, "kept");
  assert.equal(lane.attempts.length, 1);
  assert.equal(lane.skipped, true);
});

test("the overall deadline still bounds the wait policy", async () => {
  const controller = new AbortController();
  let lane!: StallLane;
  await compareStall("wait", async (_, emit, signal) => {
    first(emit);
    await waitForAbort(signal);
  }, (next) => {
    lane = next;
    if (next.phase === "waiting") controller.abort("deadline");
  }, controller.signal, { idleMs: 2, deadlineMs: 100 });
  assert.equal(lane.phase, "deadline");
  assert.equal(lane.attempts[0]!.interrupted, true);
  assert.equal(lane.attempts[0]!.chunks[0], "A cache ");
});

test("a broken transport is not a stall and does not trigger the idle retry", async () => {
  const { lane } = await run("retry", async (_, emit) => {
    first(emit);
    throw Error("Connection dropped");
  });
  assert.equal(lane.phase, "failed");
  assert.equal(lane.attempts.length, 1);
});
