import { comparePolicy, emptyLane, hasVisibleText, policies, readStream, repeatedPrefix } from "./experiment.js";
import type { Lane, Policy, Scenario, Source } from "./experiment.js";

const deadlines: Record<Source, number> = { scripted: 9_000, ollama: 90_000 };
const budgets: Record<Source, number> = { scripted: 75, ollama: 192 };
const form = document.querySelector<HTMLFormElement>("#run-form")!;
const run = document.querySelector<HTMLButtonElement>("#run")!;
const runIcon = run.querySelector<HTMLImageElement>("img")!;
const runLabel = run.querySelector("span")!;
const clock = document.querySelector("#clock")!;
const runState = document.querySelector("#run-state")!;
const progress = document.querySelector<HTMLProgressElement>("#progress")!;
const panels = document.querySelector("#policies")!;
const template = document.querySelector<HTMLTemplateElement>("#policy-template")!;
const policyPicker = form.querySelector<HTMLSelectElement>("#live-policy")!;
const policyField = document.querySelector<HTMLElement>("#live-policy-field")!;
const deadlineLabel = document.querySelector("#deadline")!;
const sourceLabel = document.querySelector("#source-label")!;
const scriptDeadline = document.querySelector<HTMLInputElement>("#script-deadline")!;
const scriptDeadlineField = document.querySelector<HTMLElement>("#script-deadline-field")!;
const workBudget = document.querySelector<HTMLInputElement>("#work-budget")!;
const workBudgetField = document.querySelector<HTMLElement>("#work-budget-field")!;
const workBudgetLabel = document.querySelector("#work-budget-label")!;

const labels: Record<Policy, string> = {
  never: "No retry",
  always: "Always retry",
  "pre-visible": "Retry before visible text",
  adaptive: "Budgeted restart",
  resume: "Scripted cursor replay",
};

const icons: Record<Policy, string> = {
  never: "square",
  always: "rotate-ccw",
  "pre-visible": "audio-lines",
  adaptive: "list-filter",
  resume: "network",
};

function text(element: Element, value: string | number) {
  const content = String(value);
  if (element.textContent !== content) element.textContent = content;
}

function makePanel(policy: Policy) {
  const panel = template.content.firstElementChild!.cloneNode(true) as HTMLElement;
  const select = (name: string) => panel.querySelector<HTMLElement>(`[data-part=${name}]`)!;
  const title = select("title");
  const status = select("status");
  const transcript = select("transcript");
  const attempts = select("attempts");
  const generated = select("generated");
  const generatedLabel = select("generated-label");
  const unshown = select("unshown");
  const outcomeTime = select("outcome-time");
  const firstText = select("first-text");
  const note = select("note");
  const work = select("work");
  title.id = `policy-${policy}`;
  title.textContent = labels[policy];
  panel.setAttribute("aria-labelledby", title.id);
  panel.dataset.policy = policy;
  panel.style.setProperty("--policy-icon", `url("/demo/icons/${icons[policy]}.svg")`);
  panels.append(panel);

  function render(lane: Lane) {
    const shown = lane.attempts.reduce((sum, attempt) => sum + attempt.chunks.length, 0);
    const resumedAt = lane.attempts[1]?.cursor;
    const presented = resumedAt ? [{ chunks: lane.attempts.flatMap((item) => item.chunks), interrupted: lane.phase === "failed" }] : lane.attempts;
    const segments = presented.filter(hasVisibleText);
    const repeated = repeatedPrefix(lane);
    panel.dataset.phase = lane.phase;
    panel.dataset.repeated = String(repeated);
    const phases = {
      ready: "Ready",
      streaming: panels.getAttribute("data-source") === "ollama" && !hasVisibleText(lane.attempts.at(-1))
        ? "Waiting for Ollama" : "Streaming",
      retrying: "Retrying",
      complete: repeated ? "Repeated text" : "Complete",
      failed: lane.error ? "Source error" : "Interrupted",
      stopped: "Stopped",
      deadline: "Deadline reached",
    };
    text(status, phases[lane.phase]);
    text(attempts, lane.attempts.length);
    text(generated, lane.generated);
    text(unshown, lane.generated - shown);
    text(outcomeTime, lane.outcomeMs === undefined ? "-" : `${(lane.outcomeMs / 1000).toFixed(2)} s`);
    text(firstText, lane.firstTextMs === undefined ? "" : `First text ${(lane.firstTextMs / 1000).toFixed(2)} s`);

    const parts: HTMLElement[] = [];
    if (!segments.length) {
      const placeholder = document.createElement("p");
      placeholder.className = "placeholder";
      placeholder.textContent = lane.phase === "ready" || lane.phase === "streaming"
        ? "Waiting for output"
        : "No text shown";
      parts.push(placeholder);
    }
    for (const [index, attempt] of segments.entries()) {
      const segment = document.createElement("div");
      segment.className = "segment";
      segment.dataset.interrupted = String(attempt.interrupted);
      const copy = document.createElement("p");
      copy.textContent = attempt.chunks.join("");
      if (index > 0) {
        const marker = document.createElement("span");
        marker.className = "attempt-marker";
        marker.textContent = "Retry output";
        segment.append(marker);
      }
      if (attempt === presented.at(-1) && lane.phase === "streaming") {
        const cursor = document.createElement("span");
        cursor.className = "cursor";
        cursor.setAttribute("aria-hidden", "true");
        copy.append(cursor);
      }
      segment.append(copy);
      parts.push(segment);
    }
    transcript.replaceChildren(...parts);

    let outcome = "";
    if (lane.phase === "retrying") {
      outcome = `${lane.attempts.at(-1)?.failure ?? "Connection lost."} Waiting to retry.`;
    }
    if (lane.phase === "failed") outcome = segments.length ? "Partial answer kept. Not complete." : "Connection lost before any text.";
    if (lane.phase === "complete" && repeated) outcome = "Restart repeated the visible opening.";
    if (lane.phase === "complete" && lane.attempts.length > 1 && !repeated) {
      outcome = hasVisibleText(lane.attempts[0]) ? "Retried from the beginning." : "Retried before any text appeared.";
    }
    if (lane.phase === "complete" && resumedAt) outcome = `Continued after chunk ${resumedAt}.`;
    if (lane.phase === "deadline") outcome = "The run hit its time limit.";
    if (lane.error) outcome = segments.length ? `${lane.error} Partial answer kept.` : lane.error;
    if (lane.decision) outcome = lane.decision;
    text(note, outcome);

    const live = panels.getAttribute("data-source") === "ollama";
    const observed = lane.attempts.filter((attempt) => attempt.observedWork !== undefined);
    const unknown = lane.attempts.length - observed.length;
    const total = observed.reduce((sum, attempt) => sum + attempt.observedWork!, 0);
    work.hidden = lane.attempts.length === 0 || (!live && policy !== "adaptive");
    const units = live ? "Output tokens" : "Scripted work";
    const reported = observed.length ? `${total} reported` : "not reported";
    text(work, `${units}: ${reported}${unknown && observed.length ? ` · ${unknown} attempt unreported` : ""}`);
  }

  return { panel, generatedLabel, render };
}

const views = policies.map(makePanel);
let active: AbortController | undefined;

function selectedSource(): Source {
  return form.querySelector<HTMLInputElement>('input[name="source"]:checked')!.value as Source;
}

function deadlineFor(source: Source) {
  return source === "scripted" && scriptDeadline.validity.valid && scriptDeadline.value
    ? scriptDeadline.valueAsNumber * 1000 : deadlines[source];
}

function usesBudget(source: Source) {
  return source === "scripted" || policyPicker.value === "adaptive";
}

function reset() {
  const source = selectedSource();
  const deadlineMs = deadlineFor(source);
  policyField.hidden = source !== "ollama";
  scriptDeadlineField.hidden = source !== "scripted";
  scriptDeadline.disabled = source !== "scripted";
  workBudgetField.hidden = !usesBudget(source);
  workBudget.disabled = !usesBudget(source);
  workBudget.valueAsNumber = budgets[source];
  text(workBudgetLabel, source === "ollama" ? "Restart budget (tokens)" : "Restart budget (units)");
  panels.setAttribute("data-source", source);
  panels.setAttribute("aria-label", source === "ollama" ? "Live policy result" : "Policy comparison");
  views.forEach(({ panel, generatedLabel, render }) => {
    panel.hidden = source === "ollama" && panel.dataset.policy !== policyPicker.value;
    text(generatedLabel, source === "ollama" ? "Received chunks" : "Generated chunks");
    render(emptyLane());
  });
  text(runState, "Not run");
  runState.removeAttribute("data-state");
  text(clock, "0.0 s");
  text(deadlineLabel, `${deadlineMs / 1000} s`);
  text(sourceLabel, source === "ollama" ? "Local Ollama generation" : "Scripted generation");
  progress.max = deadlineMs;
  progress.value = 0;
  runIcon.src = "/demo/icons/play.svg";
  text(runLabel, source === "ollama" ? "Run live" : "Run comparison");
}

reset();
form.addEventListener("change", (event) => {
  if (event.target === workBudget && workBudget.validity.valid) budgets[selectedSource()] = workBudget.valueAsNumber;
  if (!active) reset();
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (active) {
    active.abort("stopped");
    return;
  }
  const scenario = new FormData(form).get("scenario") as Scenario;
  const source = selectedSource();
  const deadlineMs = deadlineFor(source);
  const outputBudget = workBudget.validity.valid && workBudget.value ? workBudget.valueAsNumber : budgets[source];
  const selected = source === "ollama" ? [policyPicker.value as Policy] : policies;
  const controller = new AbortController();
  active = controller;
  const inputs = form.querySelectorAll<HTMLInputElement | HTMLSelectElement>("input, select");
  inputs.forEach((input) => input.disabled = true);
  runIcon.src = "/demo/icons/square.svg";
  text(runLabel, "Stop run");
  text(runState, "Running");
  runState.setAttribute("data-state", "running");
  const start = performance.now();
  const tick = () => {
    const elapsed = performance.now() - start;
    text(clock, `${(elapsed / 1000).toFixed(1)} s`);
    progress.value = elapsed;
  };
  progress.value = 0;
  tick();
  const timer = window.setInterval(tick, 100);
  const deadline = window.setTimeout(() => controller.abort("deadline"), deadlineMs);
  try {
    await Promise.all(selected.map((policy) => comparePolicy(
      policy,
      (attempt, emit, signal, cursor) => readStream(scenario, attempt, emit, signal, source, cursor),
      views[policies.indexOf(policy)]!.render,
      controller.signal,
      undefined,
      { deadlineMs, outputBudget },
    )));
  } finally {
    window.clearInterval(timer);
    window.clearTimeout(deadline);
    tick();
    const result = controller.signal.reason === "deadline" ? "Deadline reached"
      : controller.signal.aborted ? "Run stopped" : "Run finished";
    text(runState, result);
    runState.setAttribute("data-state", "finished");
    runIcon.src = "/demo/icons/rotate-ccw.svg";
    text(runLabel, "Run again");
    inputs.forEach((input) => input.disabled = false);
    scriptDeadline.disabled = source !== "scripted";
    workBudget.disabled = !usesBudget(source);
    active = undefined;
  }
});
