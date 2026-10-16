import { readNdjson } from "./experiment.js";

const policies = {
  queued: "Queue short request", immediate: "Dispatch short request immediately",
  keep: "Keep long stream open", cancel: "Close long stream",
  "keep-both": "Let both streams finish", "stop-a": "A stops their stream",
} as const;
type Policy = keyof typeof policies;
const pairs = {
  dispatch: ["queued", "immediate"], cancellation: ["keep", "cancel"], saturation: ["keep-both", "stop-a"],
} as const satisfies Record<string, readonly Policy[]>;
type Comparison = keyof typeof pairs;
type User = "long" | "peer" | "short";
type Targets = Partial<Record<User, { first?: number; completion: number }>>;
type LaneEvent = { policy: Policy; lane: User; ms: number } & (
  { type: "arrival" | "dispatch" | "done" | "cancelled" } | { type: "delta"; text: string } |
  { type: "error" | "skipped"; message: string }
);
type Event = LaneEvent | { type: "setup"; targets: Targets; prompts: Partial<Record<User, string>> }
  | { type: "warming" | "comparison-end" } | { type: "failed"; message: string }
  | { type: "trial-invalid"; policy: Policy; message: string }
  | { type: "trial-end" | "timeout"; policy: Policy };

type Lane = {
  node: HTMLElement; text: HTMLElement; status: HTMLElement;
  arrival?: number; sent?: number; first?: number; last?: number; gap?: number; end?: number; cancelled?: boolean;
};
type Trial = {
  policy: Policy; overlap: HTMLElement; long: Lane; short: Lane; peer?: Lane;
  running?: boolean; invalid?: boolean;
};

const state = document.querySelector<HTMLElement>("#shared-state")!;
const sources = document.querySelector<HTMLFieldSetElement>("#sources")!;
const comparisons = document.querySelector<HTMLFieldSetElement>("#comparison")!;
const trialOrder = document.querySelector<HTMLLabelElement>("#trial-order")!;
const reverse = trialOrder.querySelector<HTMLInputElement>("input")!;
const button = document.querySelector<HTMLButtonElement>("#run")!;
const template = document.querySelector<HTMLTemplateElement>("#trial-template")!;
const trialsNode = document.querySelector<HTMLElement>("#trials")!;
let active: AbortController | undefined;
let targets: Targets | undefined;

function selectedComparison(): Comparison {
  return comparisons.querySelector<HTMLInputElement>("input:checked")!.value as Comparison;
}

function selectedSource() {
  return sources.querySelector<HTMLInputElement>("input:checked")!.value;
}

function makeTrials(comparison: Comparison): Map<Policy, Trial> {
  trialsNode.replaceChildren();
  trialsNode.dataset.comparison = comparison;
  function build(policy: Policy): Trial {
    const view = template.content.cloneNode(true) as DocumentFragment;
    view.querySelector("h2")!.textContent = policies[policy];
    function user(lane: User): Lane {
      const node = view.querySelector<HTMLElement>(`[data-lane="${lane}"]`)!;
      return { node, text: node.querySelector(".transcript")!, status: node.querySelector(".status")! };
    }
    const trial: Trial = { policy, overlap: view.querySelector<HTMLElement>(".overlap")!, long: user("long"), short: user("short") };
    const peer = user("peer");
    if (comparison === "saturation") {
      trial.peer = peer;
      trial.short.node.querySelector(".user-name")!.textContent = "User C";
      trial.short.text.setAttribute("aria-label", "User C output");
    } else {
      peer.node.remove();
    }
    trialsNode.append(view);
    return trial;
  }
  return new Map(pairs[comparison].map((policy) => [policy, build(policy)]));
}

let trials = makeTrials(selectedComparison());
const duration = (ms: number) => ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`;

function metric(lane: Lane, name: string, ms: number, target?: number) {
  const value = lane.node.querySelector<HTMLElement>(`[data-metric="${name}"]`)!;
  value.textContent = duration(ms);
  if (target === undefined) return;
  const outcome = document.createElement("small");
  outcome.textContent = `${ms <= target * 1000 ? "On time" : "Late"} · ${target} s target`;
  outcome.dataset.late = String(ms > target * 1000);
  value.append(outcome);
}

function phase(lane: Lane, key: string, label: string) {
  lane.node.dataset.phase = key;
  lane.status.textContent = label;
}

function receive(event: LaneEvent, source: string) {
  const trial = trials.get(event.policy)!;
  const lane = trial[event.lane]!;
  switch (event.type) {
    case "arrival":
      lane.arrival = event.ms;
      trial.running = true;
      trial.overlap.textContent = "Running";
      phase(lane, "waiting", "Waiting to dispatch");
      break;
    case "dispatch": {
      lane.sent = event.ms;
      const newcomer = trial.peer && event.lane === "short";
      const waiting = newcomer && source === "scripted" ? "Waiting for a slot" : "Waiting for first text";
      phase(lane, "waiting", waiting);
      if (newcomer) trial.overlap.textContent = `C: ${waiting}`;
      break;
    }
    case "delta": {
      const pinned = lane.text.scrollHeight - lane.text.scrollTop <= lane.text.clientHeight + 2;
      lane.text.append(event.text);
      if (pinned) lane.text.scrollTop = lane.text.scrollHeight;
      if (!event.text.trim()) return;
      if (lane.first === undefined) {
        lane.first = event.ms;
        phase(lane, "streaming", "Streaming");
        if (event.lane === "short") {
          const firstText = event.ms - lane.arrival!;
          metric(lane, "first", firstText, targets?.short?.first);
          if (trial.peer) trial.overlap.textContent = `C first text: ${duration(firstText)}`;
        }
      }
      if (event.lane !== "short" && lane.last !== undefined) {
        lane.gap = Math.max(lane.gap ?? 0, event.ms - lane.last);
        metric(lane, "gap", lane.gap);
      }
      lane.last = event.ms;
      break;
    }
    case "done":
      if (lane.first === undefined) return phase(lane, "failed", "No visible output");
      lane.end = event.ms;
      phase(lane, "complete", "Complete");
      metric(lane, "completion", event.ms - lane.arrival!, targets?.[event.lane]?.completion);
      break;
    case "cancelled":
      lane.cancelled = true;
      phase(lane, "cancelled", trial.peer ? "Stopped by A" : "Connection closed");
      break;
    case "error":
    case "skipped":
      phase(lane, "failed", event.message);
  }
}

function closeTrial(trial: Trial, label?: string) {
  trial.running = false;
  for (const lane of [trial.long, trial.peer, trial.short]) {
    if (lane && ["waiting", "streaming"].includes(lane.node.dataset.phase ?? "")) {
      phase(lane, "failed", label ? "Not completed" : "Incomplete");
    }
  }
  if (label) {
    trial.invalid = true;
    trial.overlap.textContent = label;
  } else if (trial.peer) {
    const { long, peer, short } = trial;
    if (peer.end === undefined || short.end === undefined ||
        (trial.policy === "stop-a" ? !long.cancelled : long.end === undefined)) {
      trial.invalid = true;
      trial.overlap.textContent = "Incomplete trial";
    } else {
      const first = short.first!;
      const outcome = trial.policy === "stop-a"
        ? first < peer.last! ? "C and B streamed together" : "No C/B output overlap"
        : first >= Math.min(long.end!, peer.end!) ? "C waited for a completion" : "C began before a completion was observed";
      trial.overlap.textContent = `C first text: ${duration(first - short.arrival!)} · ${outcome}`;
    }
  } else if (trial.policy === "cancel") {
    trial.overlap.textContent = trial.long.cancelled && trial.short.end !== undefined
      ? "Generation Interrupted closed A's connection" : "Incomplete trial";
  } else if (trial.long.end === undefined || trial.short.end === undefined) {
    trial.overlap.textContent = "Incomplete trial";
  } else if (trial.policy === "keep") {
    trial.overlap.textContent = "A's stream stayed open";
  } else if (trial.short.first! < trial.long.last!) {
    trial.overlap.textContent = "Output overlap observed";
  } else if (trial.short.sent! < trial.long.end) {
    trial.overlap.textContent = "Dispatched early · no output overlap";
  } else {
    trial.overlap.textContent = "No output overlap";
  }
}

function running(value: boolean) {
  sources.disabled = value;
  comparisons.disabled = value;
  reverse.disabled = value;
  button.querySelector("span")!.textContent = value ? "Stop comparison" : "Run comparison";
  button.querySelector("img")!.src = `/demo/icons/${value ? "square" : "play"}.svg`;
}

async function run() {
  if (active) return active.abort();
  const controller = new AbortController();
  active = controller;
  const comparison = selectedComparison();
  const order = comparison !== "dispatch" && reverse.checked ? "reverse" : "baseline";
  trials = makeTrials(comparison);
  targets = undefined;
  running(true);
  const source = selectedSource();
  state.textContent = source === "ollama" ? "Starting local comparison" : "Running scripted comparison";
  let ended = false;
  try {
    await readNdjson<Event>(`/v1/chat/completions/shared-backend?source=${source}&comparison=${comparison}&order=${order}`, (event) => {
      if ("lane" in event) {
        if (event.type === "arrival" && event.lane === "long") {
          state.textContent = source === "ollama" ? "Running local comparison" : "Running scripted comparison";
        }
        return receive(event, source);
      }
      switch (event.type) {
        case "setup":
          targets = event.targets;
          for (const trial of trials.values()) {
            for (const user of ["long", "peer", "short"] as const) {
              const prompt = trial[user]?.node.querySelector(".shared-prompt");
              if (prompt) prompt.textContent = event.prompts[user] ?? "";
            }
          }
          break;
        case "warming": state.textContent = "Warming local model"; break;
        case "trial-end": {
          const trial = trials.get(event.policy)!;
          if (!trial.invalid) closeTrial(trial);
          break;
        }
        case "trial-invalid": closeTrial(trials.get(event.policy)!, event.message); break;
        case "timeout": closeTrial(trials.get(event.policy)!, "Trial timed out"); break;
        case "failed": state.textContent = event.message; ended = true; break;
        case "comparison-end":
          state.textContent = [...trials.values()].some((trial) => trial.invalid) ? "Comparison incomplete" : "Comparison finished";
          ended = true;
          break;
      }
    }, controller.signal);
    if (!ended) throw new Error("Missing completion event");
  } catch {
    const label = controller.signal.aborted ? "Comparison stopped" : "Comparison interrupted";
    for (const trial of trials.values()) {
      if (trial.running) closeTrial(trial, label);
    }
    state.textContent = label;
  } finally {
    active = undefined;
    running(false);
  }
}

function resetComparison() {
  if (active) return;
  const comparison = selectedComparison();
  trialOrder.hidden = comparison === "dispatch";
  trialOrder.querySelector("span")!.textContent = comparison === "saturation" ? "Run stop trial first" : "Run cancel trial first";
  trials = makeTrials(comparison);
  state.textContent = comparison === "saturation" && selectedSource() === "ollama"
    ? "Live saturation requires two Ollama slots" : "Ready";
}

comparisons.addEventListener("change", resetComparison);
sources.addEventListener("change", resetComparison);
reverse.addEventListener("change", resetComparison);
button.addEventListener("click", () => void run());
window.addEventListener("pagehide", () => active?.abort());
