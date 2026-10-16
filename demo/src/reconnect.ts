import { readNdjson } from "./experiment.js";

type Terminal = "done" | "upstream-cut" | "deadline" | "expired" | "error" | "stopped";
type Frame = { seq: number; replayed?: boolean; message?: string } & (
  { type: "delta"; text: string } | { type: "started" | Terminal }
);
type Run = { id: string; cut_after: number };
type Active = { id?: string; stopping: boolean };
type Action = "continue" | "regenerate";

const api = "/v1/stream-runs";
const faults = document.querySelector<HTMLFieldSetElement>("#faults")!;
const run = document.querySelector<HTMLButtonElement>("#run")!;
const attempts = document.querySelector<HTMLElement>("#attempts")!;
const template = document.querySelector<HTMLTemplateElement>("#attempt-template")!;
const total = document.querySelector<HTMLElement>("#total-calls")!;
const actions = document.querySelector<HTMLElement>("#recovery-actions")!;
const errorText = document.querySelector<HTMLElement>("#run-error")!;
let active: Active | undefined;
let previousCut: string | undefined;
let calls = 0;

function showCalls() {
  total.textContent = `${calls} ${calls === 1 ? "request" : "requests"} attempted`;
}

function makeAttempt(title: string) {
  const card = template.content.firstElementChild!.cloneNode(true) as HTMLElement;
  card.querySelector("h2")!.textContent = title;
  const output = card.querySelector(".transcript")!;
  const status = card.querySelector(".status")!;
  const note = card.querySelector(".note")!;
  attempts.append(card);
  return {
    output,
    note,
    phase(key: string, label: string) {
      card.dataset.phase = key;
      status.textContent = label;
    },
    metric(key: string, value: number | string) {
      card.querySelector(`[data-metric="${key}"]`)!.textContent = String(value);
    },
  };
}

async function call(url: string, method: "POST" | "DELETE") {
  const response = await fetch(url, { method });
  if (!response.ok) {
    const body = await response.json().catch(() => ({})) as { detail?: unknown };
    throw Error(typeof body.detail === "string" ? body.detail : `Request failed (HTTP ${response.status}).`);
  }
  return response;
}

async function attach(info: Run, view: ReturnType<typeof makeAttempt>, cutBrowser: boolean) {
  let cursor = 0;
  let visible = 0;
  let replayed = 0;
  let terminal: Terminal | undefined;
  let resumeFrom: number | undefined;
  do {
    const browser = new AbortController();
    let drop = false;
    try {
      await readNdjson<Frame>(`${api}/${info.id}/events?after=${cursor}`, (frame) => {
        if (frame.seq !== cursor + 1) throw Error("Delivery sequence was not continuous.");
        cursor = frame.seq;
        if (frame.type === "started") {
          view.metric("calls", 1);
          calls++;
          showCalls();
        } else if (frame.type === "delta") {
          view.phase("streaming", "Streaming");
          view.output.append(frame.text);
          if (frame.replayed) view.metric("replayed", ++replayed);
          if (frame.text.trim()) visible++;
          if (cutBrowser && resumeFrom === undefined && visible === info.cut_after) {
            resumeFrom = cursor;
            drop = true;
            browser.abort();
            throw Error("Injected browser disconnect");
          }
        } else {
          terminal = frame.type;
          const labels: Record<Terminal, string> = {
            done: "Complete", "upstream-cut": "Ollama interrupted", deadline: "Deadline reached",
            expired: "Reconnect window expired", error: "Source error", stopped: "Stopped",
          };
          const phase = frame.type === "done" ? "complete"
            : frame.type === "upstream-cut" || frame.type === "error" ? "failed"
            : frame.type;
          view.phase(phase, labels[frame.type]);
          if (frame.message) view.note.textContent = frame.message;
          if (frame.type === "done" && resumeFrom !== undefined) {
            view.note.textContent = `Reattached after event ${resumeFrom}. No gaps or duplicates.`;
          } else if (frame.type === "done" && cutBrowser) {
            view.note.textContent = "The answer ended before the browser cut.";
          }
        }
      }, browser.signal, (frame) => frame.type !== "started" && frame.type !== "delta", "GET");
    } catch (error) {
      if (!drop) throw error;
    }
    if (!drop) break;
    view.phase("waiting", "Browser disconnected");
    view.note.textContent = `Reconnecting after event ${resumeFrom}.`;
    await new Promise((resolve) => window.setTimeout(resolve, 650));
    view.metric("reconnects", 1);
  } while (!terminal);
  if (!terminal) throw Error("The connection ended without an outcome.");
  return terminal;
}

function showError(error: unknown) {
  errorText.textContent = error instanceof Error ? error.message : "The run failed.";
  errorText.hidden = false;
}

function updateButton(busy: boolean) {
  run.disabled = false;
  run.querySelector("span")!.textContent = busy ? "Stop run" : "Run experiment";
  run.querySelector("img")!.src = `/demo/icons/${busy ? "square" : "play"}.svg`;
  faults.disabled = busy;
}

async function start(action?: Action) {
  if (active) return;
  const parent = previousCut;
  if (action && !parent) return;
  previousCut = undefined;
  actions.hidden = true;
  errorText.hidden = true;
  if (!action) {
    attempts.replaceChildren();
    calls = 0;
    showCalls();
  }
  const job: Active = { stopping: false };
  active = job;
  updateButton(true);
  const view = makeAttempt(action === "continue" ? "New generation: continue from partial"
    : action === "regenerate" ? "New generation: regenerate" : "Original generation");
  view.phase("waiting", "Waiting for Ollama");
  const fault = faults.querySelector<HTMLInputElement>("input:checked")!.value;
  const started = performance.now();
  try {
    const endpoint = action ? `${api}/${parent}/retry?action=${action}` : `${api}?fault=${fault}`;
    const response = await call(endpoint, "POST");
    const info = await response.json() as Run;
    job.id = info.id;
    if (job.stopping) await call(`${api}/${job.id}`, "DELETE");
    const outcome = await attach(info, view, !action && fault === "browser");
    if (outcome === "upstream-cut") {
      previousCut = info.id;
      actions.hidden = false;
    }
  } catch (error) {
    view.phase("failed", "Interrupted");
    showError(error);
  } finally {
    view.metric("time", `${((performance.now() - started) / 1000).toFixed(2)} s`);
    active = undefined;
    updateButton(false);
  }
}

run.addEventListener("click", () => {
  const job = active;
  if (!job) return void start();
  job.stopping = true;
  run.disabled = true;
  if (job.id) void call(`${api}/${job.id}`, "DELETE").catch((error) => {
    if (active === job) {
      showError(error);
      job.stopping = false;
      run.disabled = false;
    }
  });
});

for (const button of actions.querySelectorAll<HTMLButtonElement>("button")) {
  button.addEventListener("click", () => void start(button.dataset.action as Action));
}
