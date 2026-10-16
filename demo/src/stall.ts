import { readNdjson, hasVisibleText, repeatedPrefix } from "./experiment.js";
import type { Frame } from "./experiment.js";
import { compareStall, emptyStallLane, stallIdleMs, stallPolicies, stallRestartReserveMs } from "./stall-policy.js";
import type { StallLane, StallPolicy } from "./stall-policy.js";

const form = document.querySelector<HTMLFormElement>("#stall-form")!;
const run = document.querySelector<HTMLButtonElement>("#run")!;
const deadline = document.querySelector<HTMLInputElement>("#stall-deadline")!;
const parent = document.querySelector("#stall-policies")!;
const template = document.querySelector<HTMLTemplateElement>("#stall-template")!;
const runState = document.querySelector<HTMLElement>("#run-state")!;
document.querySelector("#idle-threshold")!.textContent = `${stallIdleMs / 1000} s`;
let active: AbortController | undefined;

const cards = {
  wait: { title: "Wait through silence", icon: "audio-lines", mark: "#78b3f6" },
  keep: { title: "Stop and keep partial", icon: "square", mark: "#70cabd" },
  retry: { title: "Retry if time allows", icon: "rotate-ccw", mark: "#e6ac76" },
};

function makeView(policy: StallPolicy) {
  const card = template.content.firstElementChild!.cloneNode(true) as HTMLElement;
  const { title, icon, mark } = cards[policy];
  card.dataset.policy = policy;
  card.style.setProperty("--policy-icon", `url("/demo/icons/${icon}.svg")`);
  card.style.setProperty("--mark", mark);
  const heading = card.querySelector("h2")!;
  heading.textContent = title;
  heading.id = `stall-${policy}`;
  card.setAttribute("aria-labelledby", heading.id);
  const status = card.querySelector(".status")!;
  const note = card.querySelector(".note")!;
  const output = card.querySelector(".transcript")!;
  const requests = card.querySelector('[data-metric="requests"]')!;
  const outcome = card.querySelector('[data-metric="outcome"]')!;
  const repeat = card.querySelector('[data-metric="repeat"]')!;
  parent.append(card);

  return (lane: StallLane) => {
    const repeated = repeatedPrefix(lane);
    card.dataset.phase = lane.phase;
    card.dataset.repeated = String(repeated);
    status.textContent = {
      ready: "Ready", streaming: lane.restarted ? "Streaming retry" : "Streaming",
      waiting: "Waiting through silence", complete: "Complete", kept: "Partial kept",
      deadline: "Deadline reached", stopped: "Stopped", failed: "Stream failed",
    }[lane.phase];
    requests.textContent = String(lane.attempts.length);
    outcome.textContent = lane.outcomeMs === undefined ? "-" : `${(lane.outcomeMs / 1000).toFixed(2)} s`;
    repeat.textContent = repeated ? "Yes" : lane.outcomeMs === undefined ? "-" : "No";
    note.textContent = lane.skipped ? `Retry skipped: less than ${stallRestartReserveMs / 1000} s remained.`
      : lane.error ? lane.error
      : repeated ? "The retry repeated text already on screen."
      : lane.resumed ? "The original stream resumed on the same connection."
      : lane.phase === "waiting" ? "No text arrived, but the connection is still open."
      : lane.phase === "kept" ? "Stopped at the idle threshold."
      : lane.phase === "deadline" ? "Stopped at the overall deadline."
      : lane.restarted ? "Started a new request after the idle threshold." : "";

    const parts = lane.attempts.map((attempt, index) => {
      if (!hasVisibleText(attempt)) return null;
      const segment = document.createElement("div");
      segment.className = "segment";
      segment.dataset.interrupted = String(attempt.interrupted);
      if (index === 1) {
        const marker = document.createElement("span");
        marker.className = "attempt-marker";
        marker.textContent = "Retry output";
        segment.append(marker);
      }
      const text = document.createElement("p");
      text.textContent = attempt.chunks.join("");
      segment.append(text);
      return segment;
    }).filter((segment) => segment !== null);
    if (!parts.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "placeholder";
      placeholder.textContent = lane.phase === "ready" || lane.phase === "streaming"
        ? "Waiting for text" : "No text shown";
      parts.push(placeholder);
    }
    output.replaceChildren(...parts);
  };
}

const views = stallPolicies.map(makeView);
const reset = () => {
  for (const render of views) render(emptyStallLane());
  runState.textContent = "Not run";
  runState.removeAttribute("data-state");
};
reset();
form.addEventListener("change", () => { if (!active) reset(); });

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (active) return active.abort("stopped");

  const scenario = new FormData(form).get("scenario");
  const deadlineMs = deadline.valueAsNumber * 1000;
  const controller = new AbortController();
  active = controller;
  const inputs = form.querySelectorAll<HTMLInputElement>("input");
  const icon = run.querySelector("img")!;
  const label = run.querySelector("span")!;
  inputs.forEach((input) => input.disabled = true);
  runState.textContent = "Running";
  runState.dataset.state = "running";
  icon.src = "/demo/icons/square.svg";
  label.textContent = "Stop run";
  const timer = window.setTimeout(() => controller.abort("deadline"), deadlineMs);
  try {
    await Promise.all(stallPolicies.map((policy, index) => compareStall(
      policy,
      (attempt, emit, signal) => readNdjson<Frame>(
        `/v1/chat/completions/stall?scenario=${scenario}&attempt=${attempt}`,
        emit, signal, (frame) => frame.type === "done",
      ),
      views[index]!, controller.signal, { deadlineMs },
    )));
  } finally {
    window.clearTimeout(timer);
    runState.textContent = controller.signal.reason === "stopped" ? "Run stopped" : "Comparison finished";
    runState.dataset.state = "finished";
    icon.src = "/demo/icons/play.svg";
    label.textContent = "Run comparison";
    inputs.forEach((input) => input.disabled = false);
    active = undefined;
  }
});
