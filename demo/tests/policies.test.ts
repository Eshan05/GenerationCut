import assert from "node:assert/strict";
import test from "node:test";
import { comparePolicy, emptyLane, repeatedPrefix } from "../dist/experiment.js";
import type { Frame, Lane, Policy } from "../src/experiment.ts";

async function run(policy: Policy, visible: boolean, recover = true): Promise<Lane> {
  let lane = emptyLane();
  await comparePolicy(policy, async (attempt, emit) => {
    emit({ type: "generated" });
    if (visible || attempt === 2) emit({ type: "delta", text: "A cache " });
    if (attempt === 1 || !recover) throw new Error("Disconnected");
    for (const frame of [
      { type: "generated" },
      { type: "delta", text: "avoids repeated work." },
      { type: "done" },
    ] satisfies Frame[]) emit(frame);
  }, (next) => { lane = next; }, new AbortController().signal, 0);
  return lane;
}

test("retries invisible generation but preserves visible partial text", async () => {
  const before = await run("pre-visible", false);
  assert.equal(before.phase, "complete");
  assert.equal(before.generated, 3);
  assert.deepEqual(before.attempts.map((attempt) => attempt.chunks.length), [0, 2]);
  assert.equal(repeatedPrefix(before), false);

  const after = await run("pre-visible", true);
  assert.equal(after.phase, "failed");
  assert.deepEqual(after.attempts, [{ chunks: ["A cache "], interrupted: true }]);
});

test("unconditional retry repeats the prefix but never makes a third attempt", async () => {
  const recovered = await run("always", true);
  assert.equal(recovered.phase, "complete");
  assert.equal(repeatedPrefix(recovered), true);

  const failed = await run("always", true, false);
  assert.equal(failed.phase, "failed");
  assert.equal(failed.attempts.length, 2);
});

test("an ended response without a done frame is not a completed answer", async () => {
  let lane = emptyLane();
  let now = 0;
  await comparePolicy("never", async (_, emit) => {
    emit({ type: "generated" });
    emit({ type: "delta", text: "No terminal frame" });
    now = 420;
  }, (next) => { lane = next; }, new AbortController().signal, 0, { deadlineMs: 9_000, now: () => now });
  assert.equal(lane.phase, "failed");
  assert.equal(lane.outcomeMs, 420);
  assert.equal(lane.attempts[0].chunks[0], "No terminal frame");
});

test("time to outcome includes the retry and stays unset until completion", async () => {
  let now = 5_000;
  const updates: [Lane["phase"], number | undefined][] = [];
  await comparePolicy("always", async (attempt, emit) => {
    if (attempt === 1) {
      now += 100;
      throw Error("Disconnected");
    }
    now += 250;
    emit({ type: "done" });
  }, (lane) => {
    updates.push([lane.phase, lane.outcomeMs]);
    if (lane.phase === "retrying") now += 650;
  }, new AbortController().signal, 0, { deadlineMs: 9_000, now: () => now });
  assert.deepEqual(updates.at(-1), ["complete", 1_000]);
  assert.ok(updates.slice(0, -1).every(([, time]) => time === undefined));
});

test("a deadline during the retry delay does not start another attempt", async () => {
  const controller = new AbortController();
  let lane = emptyLane();
  let now = 0;
  await comparePolicy("always", async () => { throw new Error("Disconnected"); }, (next) => {
    lane = next;
    if (next.phase === "retrying") {
      now = 9_000;
      controller.abort("deadline");
    }
  }, controller.signal, 650, { deadlineMs: 9_000, now: () => now });
  assert.equal(lane.phase, "deadline");
  assert.equal(lane.outcomeMs, 9_000);
  assert.equal(lane.attempts.length, 1);
});

test("a reported source error preserves the partial answer without retrying", async () => {
  let lane = emptyLane();
  await comparePolicy("always", async (_, emit) => {
    emit({ type: "generated" });
    emit({ type: "delta", text: "Partial" });
    emit({ type: "error", message: "Permanent source failure" });
  }, (next) => { lane = next; }, new AbortController().signal, 0);
  assert.equal(lane.phase, "failed");
  assert.equal(lane.error, "Permanent source failure");
  assert.deepEqual(lane.attempts, [{ chunks: ["Partial"], interrupted: true, failure: "Permanent source failure" }]);
});

test("a transient upstream error retries only when the chosen policy permits it", async () => {
  for (const visible of [false, true]) {
    for (const policy of ["always", "pre-visible"] as const) {
      let lane = emptyLane();
      await comparePolicy(policy, async (attempt, emit) => {
        if (attempt === 1) {
          if (visible) emit({ type: "delta", text: "Partial" });
          emit({ type: "error", message: "Upstream closed without completion", retryable: true });
        }
        emit({ type: "delta", text: "Answer" });
        emit({ type: "done" });
      }, (next) => { lane = next; }, new AbortController().signal, 0);
      const retried = policy === "always" || !visible;
      assert.equal(lane.phase, retried ? "complete" : "failed");
      assert.equal(lane.attempts.length, retried ? 2 : 1);
      if (visible) assert.deepEqual(lane.attempts[0].chunks, ["Partial"]);
    }
  }
});

test("does not ignore Retry-After or schedule a retry after the deadline", async () => {
  let lane = emptyLane();
  await comparePolicy("always", async (_, emit) => {
    emit({ type: "error", message: "Rate limited", retryable: true, retryAfterSeconds: 100 });
  }, (next) => { lane = next; }, new AbortController().signal, 0, { deadlineMs: 5_000 });
  assert.equal(lane.attempts.length, 1);
  assert.match(lane.decision ?? "", /delay exceeds the deadline/);
});

test("whitespace alone does not block the visibility-aware retry", async () => {
  let lane = emptyLane();
  await comparePolicy("pre-visible", async (attempt, emit) => {
    emit({ type: "generated" });
    emit({ type: "delta", text: attempt === 1 ? " \n" : "Answer" });
    if (attempt === 1) throw new Error("Disconnected");
    emit({ type: "done" });
  }, (next) => { lane = next; }, new AbortController().signal, 0);
  assert.equal(lane.phase, "complete");
  assert.equal(lane.attempts.length, 2);
  assert.equal(repeatedPrefix(lane), false);
});

test("cursor follows the last displayed delta, not the generated chunk count", async () => {
  const cursors: (number | undefined)[] = [];
  let lane = emptyLane();
  await comparePolicy("resume", async (attempt, emit, _, cursor) => {
    cursors.push(cursor);
    emit({ type: "generated" });
    emit({ type: "delta", text: attempt === 1 ? "First " : "second." });
    if (attempt === 1) {
      emit({ type: "generated" });
      throw Error("Disconnected");
    }
    emit({ type: "done" });
  }, (next) => { lane = next; }, new AbortController().signal, 0);
  assert.deepEqual(cursors, [undefined, 1]);
  assert.equal(lane.attempts.flatMap((attempt) => attempt.chunks).join(""), "First second.");
  assert.equal(lane.phase, "complete");
});

test("work and time budgets decide independently whether to restart", async () => {
  for (const [units, deadlineMs, attempts, note] of [
    [25, 9_000, 2, undefined],
    [41, 9_000, 1, /needs 86, budget 75/],
    [25, 5_000, 1, /not enough time/],
  ] as const) {
    let now = 0;
    let lane = emptyLane();
    await comparePolicy("adaptive", async (attempt, emit) => {
      if (attempt === 1) {
        emit({ type: "work-limit", limit: 45 });
        emit({ type: "generated", units });
        emit({ type: "delta", text: "Partial" });
        now = 2_000;
        throw Error("Disconnected");
      }
      emit({ type: "done" });
    }, (next) => { lane = next; }, new AbortController().signal, 0,
    { deadlineMs, outputBudget: 75, now: () => now });
    assert.equal(lane.attempts.length, attempts);
    if (note) assert.match(lane.decision ?? "", note);
    else assert.equal(lane.phase, "complete");
  }
});

test("an incomplete live attempt reserves its full token limit, not its visible chunks", async () => {
  for (const budget of [191, 192]) {
    let lane = emptyLane();
    await comparePolicy("adaptive", async (attempt, emit) => {
      emit({ type: "work-limit", limit: 96 });
      emit({ type: "generated" });
      if (attempt === 1) emit({ type: "error", message: "Upstream disconnected", retryable: true });
      emit({ type: "usage", output: 12 });
      emit({ type: "done" });
    }, (next) => { lane = next; }, new AbortController().signal, 0, { deadlineMs: 90_000, outputBudget: budget });
    assert.equal(lane.attempts[0].observedWork, undefined);
    assert.equal(lane.attempts.length, budget === 192 ? 2 : 1);
    if (budget === 192) assert.equal(lane.attempts[1].observedWork, 12);
    else assert.match(lane.decision ?? "", /first attempt unreported/);
  }
});
