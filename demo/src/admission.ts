export {};

type Sample = { status: string; first_s: number | null; completion_s: number | null };
type Run = { first_on_time: number; completion_on_time: number; p95_gap_ms: number | null; gap_samples: number; requests: Sample[] };
type Report = {
  source: "scripted" | "ollama"; model: string | null; queue_limit: number;
  arrivals: { offset_s: number; first_s: number; completion_s: number; kind: string }[];
  runs: { bounded: Run; deadline: Run };
};

const detail = document.querySelector("#admission-detail")!;
const results = document.querySelector<HTMLElement>("#admission-results")!;
const chart = document.querySelector("#admission-chart")!;
const rows = document.querySelector("#admission-rows")!;
const seconds = (value: number) => `${value.toFixed(2)} s`;

const labels: Record<string, string> = {
  complete: "Completed",
  completion_timeout: "Completion deadline",
  deadline_rejected: "Rejected at admission",
  expired_in_queue: "Expired in queue",
  queue_full: "Queue full",
  source_error: "Source error",
  empty_stream: "No text received",
};

function panel(label: string, run: Run, total: number) {
  const section = document.createElement("section");
  const title = document.createElement("h3");
  title.textContent = label;
  section.append(title);
  for (const [metric, amount] of [["First text", run.first_on_time], ["Completion", run.completion_on_time]] as const) {
    const row = document.createElement("div");
    row.className = "admission-bar";
    const name = document.createElement("span");
    name.textContent = metric;
    const value = document.createElement("strong");
    value.textContent = `${amount}/${total}`;
    const bar = document.createElement("progress");
    bar.max = total;
    bar.value = amount;
    bar.setAttribute("aria-label", `${label}: ${metric} on time, ${amount} of ${total} requests`);
    row.append(name, value, bar);
    section.append(row);
  }
  const gap = document.createElement("p");
  gap.className = "run-detail";
  gap.textContent = run.p95_gap_ms === null ? "p95 text gap: n/a" : `p95 text gap: ${run.p95_gap_ms} ms (${run.gap_samples} gaps)`;
  section.append(gap);
  return section;
}

function outcome(sample: Sample, firstTarget: number) {
  const pieces = [labels[sample.status] ?? "Unknown"];
  if (sample.first_s !== null) pieces.push(`text ${seconds(sample.first_s)}${sample.first_s > firstTarget ? " (late)" : ""}`);
  if (sample.completion_s !== null) pieces.push(`done ${seconds(sample.completion_s)}`);
  return pieces.join(" / ");
}

async function show() {
  try {
    const response = await fetch("/demo/admission-results.json");
    if (response.status === 404) { detail.textContent = "No admission run yet."; return; }
    if (!response.ok) throw Error();
    const report = await response.json() as Report;
    const { arrivals, runs } = report;
    if (!arrivals.length || runs.bounded.requests.length !== arrivals.length || runs.deadline.requests.length !== arrivals.length) throw Error();
    const source = report.source === "ollama" ? `Ollama: ${report.model}` : "Scripted stream";
    detail.textContent = `${source} | ${arrivals.length} arrivals per policy | 1 worker, ${report.queue_limit} waiting slot`;
    chart.replaceChildren(panel("Bounded queue", runs.bounded, arrivals.length), panel("Deadline-aware", runs.deadline, arrivals.length));
    rows.replaceChildren(...arrivals.map((job, index) => {
      const row = document.createElement("tr");
      const name = document.createElement("th");
      name.scope = "row";
      name.textContent = `${seconds(job.offset_s)} | ${job.kind}`;
      row.append(name);
      for (const value of [seconds(job.first_s), seconds(job.completion_s),
        outcome(runs.bounded.requests[index]!, job.first_s), outcome(runs.deadline.requests[index]!, job.first_s)]) {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      }
      return row;
    }));
    results.hidden = false;
  } catch {
    detail.textContent = "Could not read the admission results.";
  }
}

void show();
