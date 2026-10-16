export {};

const faults = { http429: "HTTP 429", http500: "HTTP 500", hang: "Hang", truncate: "Truncation", garbage: "Invalid output" } as const;
type Fault = keyof typeof faults;
type Metrics = { success: number; attempts: number; p99_s: number };
type Metric = keyof Metrics;
type Pair = { fault: Fault; seed: number; immediate: Metrics; backoff: Metrics };
type Report = { version: number; config: { seeds: number; requests: number; fault_rate: number }; runs: Pair[] };

const detail = document.querySelector("#run-detail")!;
const results = document.querySelector<HTMLElement>("#fault-results")!;
const metricPicker = document.querySelector<HTMLSelectElement>("#fault-metric")!;
const faultPicker = document.querySelector<HTMLSelectElement>("#fault-picker")!;
const summary = document.querySelector("#fault-summary")!;
const pairs = document.querySelector("#fault-pairs")!;

function format(number: number, metric: Metric, delta = false) {
  const success = metric === "success";
  const digits = success ? 1 : 3;
  const amount = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: digits, maximumFractionDigits: digits, signDisplay: delta ? "exceptZero" : "never",
  }).format(success ? number * 100 : number);
  return amount + (success ? (delta ? " pp" : "%") : metric === "p99_s" ? " s" : "");
}

function row(label: string, values: string[]) {
  const tr = document.createElement("tr");
  const th = document.createElement("th");
  th.scope = "row";
  th.textContent = label;
  tr.append(th);
  for (const value of values) tr.insertCell().textContent = value;
  return tr;
}

function show(report: Report) {
  const grouped = Map.groupBy(report.runs, (pair) => pair.fault);
  faultPicker.replaceChildren(...Array.from(grouped.keys(), (fault) => new Option(faults[fault], fault)));

  function render() {
    const metric = metricPicker.value as Metric;
    const diff = (pair: Pair) => pair.backoff[metric] - pair.immediate[metric];
    const signed = (number: number) => format(number, metric, true);
    summary.replaceChildren(...Array.from(grouped, ([fault, group]) => {
      const deltas = group.map(diff).sort((a, b) => a - b);
      const mid = Math.floor(deltas.length / 2);
      const median = deltas.length % 2 ? deltas[mid]! : (deltas[mid - 1]! + deltas[mid]!) / 2;
      return row(faults[fault], [signed(median), `${signed(deltas[0]!)} to ${signed(deltas.at(-1)!)}`]);
    }));
    pairs.replaceChildren(...(grouped.get(faultPicker.value as Fault) ?? []).map((pair) => row(String(pair.seed), [
      format(pair.immediate[metric], metric), format(pair.backoff[metric], metric), signed(diff(pair)),
    ])));
  }
  faultPicker.addEventListener("change", render);
  metricPicker.addEventListener("change", render);
  render();
  results.hidden = false;
}

fetch("/demo/single-fault-results.json").then(async (response) => {
  if (response.status === 404) {
    detail.textContent = "No single-fault run yet.";
    return;
  }
  if (!response.ok) throw new Error("Report unavailable");
  const report: Report = await response.json();
  if (report.version !== 1 || !report.runs.length) throw new Error("Invalid report");
  const { seeds, requests, fault_rate: faultRate } = report.config;
  detail.textContent = `${seeds} paired seeds · ${requests} requests per policy and seed · ${format(faultRate, "success")} fault probability per attempt`;
  show(report);
}).catch(() => { detail.textContent = "Could not read the single-fault results."; });
