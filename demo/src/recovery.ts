export {};

type Layers = { app_calls: number; sdk_calls: number } &
  ({ transport_attempts: number } | { wire_attempts: number });
type ClientAttempts = { limit: number | null; peak: number; rejected: number };
type RecoveryWindow = { post_outage_s: number; recovery_s: number | null };
type Stability = {
  evaluable: boolean; sustained_s: number | null; follow_up_evaluable: boolean; relapse_s: number | null;
};
type Run = {
  case: string; label: string; seed: number; logical: number;
  on_time: number | null; amplification: number | null; p99_s: number | null;
  on_time_by_arrival_second: (number | null)[]; recovery_s: number | null;
  peak_queued: number; shed: number; cancelled: number; breaker_rejected: number; budget_denied: number;
  sdk_layers?: Layers; server_received?: number; confirmations?: [number, number];
  client_attempts?: ClientAttempts;
  recovery_windows?: RecoveryWindow[];
  stability?: Stability;
  backoff_base_s?: number;
};
type Report = { version: number; config: {
  rate: number; duration: number; outage: [number, number]; seeds: number; deadline: number; openai_sdk?: string;
  service_s?: number;
  post_outage_windows?: number[];
  stability_window_s?: number;
}; runs: Run[] };
const workloads = {
  standard: "Standard", timeout: "Timeout pressure", "client-cap": "Client concurrency",
  sensitivity: "Workers and timeouts", observation: "Observation windows", backoff: "Backoff base delay",
  stability: "Sustained recovery",
} as const;
type Workload = keyof typeof workloads;
const featuredCases: Partial<Record<Workload, string>> = {
  timeout: "queue-cancel", "client-cap": "client-cap", sensitivity: "two-workers-short",
  observation: "client-cap", backoff: "backoff-400ms", stability: "client-cap",
};
const matchedReferences: Partial<Record<string, string>> = {
  "queue-cancel": "bounded-queue", "budget-whole": "retry-budget",
  "stacked": "one-layer", "stack-budget": "stacked",
  "sdk-stacked": "sdk-only", "sdk-budget": "sdk-stacked",
  "two-workers-short": "two-workers",
};

const detail = document.querySelector("#run-detail")!;
const results = document.querySelector<HTMLElement>("#recovery-results")!;
const workloadField = document.querySelector<HTMLElement>("#workload-field")!;
const workloadPicker = document.querySelector<HTMLSelectElement>("#compare-workload")!;
const casePicker = document.querySelector<HTMLSelectElement>("#compare-case")!;
const seedPicker = document.querySelector<HTMLSelectElement>("#compare-seed")!;
const seedField = document.querySelector<HTMLElement>("#seed-field")!;
const legend = document.querySelector("#selected-trace")!;
const referenceLegend = document.querySelector("#reference-trace")!;
const cutoffLegend = document.querySelector<HTMLElement>("#cutoff-trace")!;
const pairedResult = document.querySelector<HTMLElement>("#paired-result")!;
const secondaryPairedResult = document.querySelector<HTMLElement>("#secondary-paired-result")!;
const cancelResult = document.querySelector<HTMLElement>("#cancel-result")!;
const clientResult = document.querySelector<HTMLElement>("#client-result")!;
const sdkResult = document.querySelector<HTMLElement>("#sdk-result")!;
const chart = document.querySelector<SVGSVGElement>("#success-chart")!;
const description = chart.querySelector("desc")!;
const table = document.querySelector("#recovery-rows")!;
const observations = document.querySelector<HTMLElement>("#observation-results")!;
const observationRows = document.querySelector("#observation-rows")!;
const observationReference = document.querySelector("#observation-reference")!;
const observationSelected = document.querySelector("#observation-selected")!;
const stabilityResults = document.querySelector<HTMLElement>("#stability-results")!;
const stabilityRows = document.querySelector("#stability-rows")!;
const stabilityReference = document.querySelector("#stability-reference")!;
const stabilitySelected = document.querySelector("#stability-selected")!;

function element(name: string, attributes: Record<string, string | number>, label?: string) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, String(value));
  if (label) node.textContent = label;
  return node;
}

function draw(report: Report, baseline: Run, picked: Run) {
  const { duration, outage } = report.config;
  const width = Math.max(270, chart.clientWidth);
  const height = 224;
  const left = 44, right = width - 12, top = 14, bottom = height - 30;
  const x = (second: number) => left + second / duration * (right - left);
  const y = (rate: number) => bottom - rate * (bottom - top);
  chart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  description.textContent = `${baseline.label} and ${picked.label}; outage from ${outage[0]} to ${outage[1]} seconds.`;
  const children: Element[] = [description, element("rect", {
    x: x(outage[0]), y: top, width: x(outage[1]) - x(outage[0]), height: bottom - top, class: "plot-outage",
  })];
  const cutoffs = (report.config.post_outage_windows ?? []).filter((window) => outage[1] + window < duration);
  cutoffLegend.hidden = !cutoffs.length;
  for (const cutoff of cutoffs) {
    const marker = x(outage[1] + cutoff);
    children.push(element("line", { x1: marker, y1: top, x2: marker, y2: bottom, class: "plot-cutoff" }));
  }
  if (cutoffs.length) description.textContent += ` Observation cutoffs: ${cutoffs.join(", ")} seconds after the outage.`;
  for (const tick of [0, .5, 1]) {
    children.push(element("line", { x1: left, y1: y(tick), x2: right, y2: y(tick), class: "plot-grid" }));
    children.push(element("text", { x: left - 8, y: y(tick) + 4, "text-anchor": "end" }, `${tick * 100}%`));
  }
  const interval = Math.max(1, Math.ceil(duration / (width < 450 ? 5 : 8)));
  for (const tick of new Set([0, ...Array.from({ length: Math.floor((duration - 1) / interval) }, (_, i) => (i + 1) * interval), duration])) {
    children.push(element("text", { x: x(tick), y: bottom + 18, "text-anchor": "middle" }, `${tick}s`));
  }
  for (const [run, style] of [[baseline, "plot-baseline"], [picked, "plot-picked"]] as const) {
    let connected = false;
    let path = "";
    run.on_time_by_arrival_second.forEach((rate, index) => {
      if (rate === null) { connected = false; return; }
      const pointX = x(index + .5), pointY = y(rate);
      path += `${connected ? "L" : "M"}${pointX},${pointY}`;
      connected = true;
      children.push(element("circle", { cx: pointX, cy: pointY, r: 3, class: style }));
    });
    children.push(element("path", { d: path, class: style, fill: "none" }));
  }
  chart.replaceChildren(...children);
}

function value(number: number | null, digits: number, suffix: string) {
  return number === null ? "n/a" : `${number.toFixed(digits)}${suffix}`;
}

function median(values: (number | null)[]): number | null {
  const observed = values.filter((v): v is number => v !== null).sort((a, b) => a - b);
  const mid = Math.floor(observed.length / 2);
  if (!observed.length) return null;
  return observed.length % 2 ? observed[mid]! : (observed[mid - 1]! + observed[mid]!) / 2;
}

const transportAttempts = (layers: Layers) =>
  "transport_attempts" in layers ? layers.transport_attempts : layers.wire_attempts;

function acrossSeeds(runs: Run[]): Run[] {
  const cases = Map.groupBy(runs, (run) => run.case);
  return [...cases.values()].map((group) => {
    const first = group[0]!;
    const middle = (key: "on_time" | "amplification" | "p99_s" | "recovery_s") => median(group.map((run) => run[key]));
    const count = (key: "peak_queued" | "shed" | "cancelled" | "breaker_rejected" | "budget_denied") =>
      median(group.map((run) => run[key]))!;
    const layers = group.flatMap((run) => run.sdk_layers ? [run.sdk_layers] : []);
    const attempts = group.flatMap((run) => run.client_attempts ? [run.client_attempts] : []);
    const sum = (key: "app_calls" | "sdk_calls") => layers.reduce((total, layer) => total + layer[key], 0);
    return {
      ...first, on_time: middle("on_time"), amplification: middle("amplification"), p99_s: middle("p99_s"),
      recovery_s: middle("recovery_s"), confirmations: [group.filter((run) => run.recovery_s !== null).length, group.length],
      on_time_by_arrival_second: first.on_time_by_arrival_second.map((_, i) =>
        median(group.map((run) => run.on_time_by_arrival_second[i] ?? null))),
      peak_queued: count("peak_queued"), shed: count("shed"), cancelled: count("cancelled"),
      breaker_rejected: count("breaker_rejected"), budget_denied: count("budget_denied"),
      sdk_layers: layers.length ? { app_calls: sum("app_calls"), sdk_calls: sum("sdk_calls"),
        transport_attempts: layers.reduce((total, layer) => total + transportAttempts(layer), 0) } : undefined,
      server_received: layers.length ? group.reduce((total, run) => total + (run.server_received ?? 0), 0) : undefined,
      client_attempts: attempts.length ? {
        limit: attempts[0]!.limit,
        peak: median(attempts.map((item) => item.peak))!,
        rejected: attempts.reduce((total, item) => total + item.rejected, 0),
      } : undefined,
    };
  });
}

function recoveryValue(run: Run): string {
  if (!run.confirmations) return run.recovery_s === null ? "Not observed" : `${run.recovery_s} s`;
  const [confirmed, total] = run.confirmations;
  return run.recovery_s === null ? `0/${total} confirmed`
    : `${confirmed}/${total} · ${run.recovery_s.toFixed(2)} s median`;
}

function showObservations(report: Report, reference: Run, picked: Run, all: boolean) {
  const windows = report.config.post_outage_windows;
  observations.hidden = !windows?.length;
  if (!windows?.length) return;
  observationReference.textContent = reference.label;
  observationSelected.textContent = picked.label;
  observationRows.replaceChildren(...windows.map((window) => {
    const row = document.createElement("tr");
    const label = document.createElement("th");
    label.scope = "row";
    label.textContent = `${window} s`;
    row.append(label);
    for (const arm of [reference, picked]) {
      const times = report.runs
        .filter((run) => run.case === arm.case && (all || run.seed === arm.seed))
        .flatMap((run) => run.recovery_windows?.filter((point) => point.post_outage_s === window)
          .map((point) => point.recovery_s) ?? []);
      const middle = median(times);
      const count = times.filter((time) => time !== null).length;
      const cell = document.createElement("td");
      cell.textContent = !times.length ? "-" : all
        ? `${count}/${times.length} confirmed${middle === null ? "" : ` · ${middle.toFixed(2)} s median`}`
        : middle === null ? "Not confirmed" : `${middle} s`;
      row.append(cell);
    }
    return row;
  }));
}

function stabilityValue(runs: Stability[], field: "sustained_s" | "relapse_s", all: boolean) {
  const drop = field === "relapse_s";
  const applicable = drop ? runs.filter((run) => run.sustained_s !== null) : runs;
  if (!applicable.length) return drop ? "No verified streak" : "n/a";
  const checked = applicable.filter((run) => drop ? run.follow_up_evaluable : run.evaluable);
  if (!checked.length) return "Not verified";
  const times = checked.map((run) => run[field]);
  const middle = median(times);
  if (!all) return middle === null ? "Not observed" : `${middle.toFixed(2)} s`;
  const observed = times.filter((time) => time !== null).length;
  const unchecked = applicable.length - checked.length;
  return `${observed}/${checked.length} observed${middle === null ? "" : ` · ${middle.toFixed(2)} s median`}${unchecked ? ` · ${unchecked} not verified` : ""}`;
}

function showStability(report: Report, reference: Run, picked: Run, all: boolean) {
  const width = report.config.stability_window_s;
  stabilityResults.hidden = !width;
  if (!width) return;
  stabilityReference.textContent = reference.label;
  stabilitySelected.textContent = picked.label;
  const arms = [reference, picked];
  const data = arms.map((arm) => report.runs.flatMap((run) =>
    run.case === arm.case && (all || run.seed === arm.seed) && run.stability ? [run.stability] : []));
  const rows = [
    ["First recovery", ...arms.map(recoveryValue)],
    [`Sustained (2 × ${width} s)`, ...data.map((runs) => stabilityValue(runs, "sustained_s", all))],
    ["Later drop", ...data.map((runs) => stabilityValue(runs, "relapse_s", all))],
  ];
  stabilityRows.replaceChildren(...rows.map(([label, ...values]) => {
    const row = document.createElement("tr");
    const header = document.createElement("th");
    header.scope = "row";
    header.textContent = label!;
    row.append(header);
    for (const value of values) row.insertCell().textContent = value;
    return row;
  }));
}

function showPairedChange(target: HTMLElement, runs: Run[], reference: Run | undefined, pairs: Run[], all: boolean,
                          metric: "on_time" | "amplification" = "on_time") {
  target.hidden = true;
  if (!reference) return;
  const references = new Map(runs.filter((run) => run.case === reference.case).map((run) => [run.seed, run[metric]]));
  const changes = pairs.flatMap((run) => {
    const baseline = references.get(run.seed);
    return baseline == null || run[metric] === null ? [] : [run[metric] - baseline];
  });
  target.hidden = !changes.length;
  const onTime = metric === "on_time";
  const format = (n: number) => {
    const amount = (onTime ? n * 100 : n).toFixed(onTime ? 1 : 2);
    return `${n >= 0 ? "+" : ""}${amount}${onTime ? " pp" : "x"}`;
  };
  if (!target.hidden) {
    const delta = all
      ? `${format(median(changes)!)} median · ${format(Math.min(...changes))} to ${format(Math.max(...changes))} across ${changes.length} paired seeds`
      : format(changes[0]!);
    target.textContent = `${onTime ? "On-time" : "Server attempts / request"} change vs ${reference.label}: ${delta}`;
  }
}

function showPaired(report: Report, reference: Run, picked: Run, all: boolean) {
  const pairs = report.runs.filter((run) => run.case === picked.case && (all || run.seed === picked.seed));
  showPairedChange(pairedResult, report.runs, reference, pairs, all);
  const alternate = picked.case === "two-workers-short"
    ? report.runs.find((run) => run.case === "short-timeout") : undefined;
  if (workloadPicker.value === "backoff") {
    showPairedChange(secondaryPairedResult, report.runs, reference, pairs, all, "amplification");
  } else {
    showPairedChange(secondaryPairedResult, report.runs, alternate, pairs, all);
  }
  cancelResult.hidden = picked.case !== "queue-cancel";
  if (!cancelResult.hidden) {
    const count = pairs.reduce((total, run) => total + run.cancelled, 0);
    cancelResult.textContent = count > 0
      ? `Admitted attempts cancelled: ${count}${all ? ` across ${pairs.length} seeds` : ""}`
      : "No admitted work was cancelled in this comparison.";
  }
  const attempts = picked.client_attempts;
  clientResult.hidden = attempts?.limit == null;
  if (attempts?.limit != null) {
    const peaks = reference.client_attempts
      ? `${reference.client_attempts.peak} uncapped, ${attempts.peak} capped` : `${attempts.peak} capped`;
    const scope = all ? ` across ${pairs.length} seeds` : "";
    clientResult.textContent = `${all ? "Median peak" : "Peak"} active attempts: ${peaks} · ${attempts.rejected} rejected before HTTP${scope}`;
  }
  sdkResult.hidden = !picked.sdk_layers;
  if (picked.sdk_layers) {
    const { app_calls, sdk_calls } = picked.sdk_layers;
    const app = app_calls ? `${app_calls} app calls · ` : "";
    sdkResult.textContent = `${all ? "SDK totals: " : "SDK: "}${app}${sdk_calls} SDK calls · ${transportAttempts(picked.sdk_layers)} transport attempts · ${picked.server_received} received by server`;
  }
}

function fillTable(runs: Run[], selected: string) {
  table.replaceChildren(...runs.map((run) => {
    const row = document.createElement("tr");
    if (run.case === selected) row.dataset.selected = "true";
    const label = document.createElement("th");
    label.scope = "row";
    label.textContent = run.label;
    row.append(label);
    for (const cell of [value(run.on_time === null ? null : run.on_time * 100, 1, "%"),
      value(run.amplification, 2, "x"), value(run.p99_s, 2, " s"),
      recoveryValue(run), run.peak_queued, run.shed,
      run.cancelled, run.breaker_rejected, run.budget_denied]) {
      const td = document.createElement("td");
      td.textContent = String(cell);
      row.append(td);
    }
    return row;
  }));
}

async function loadReport(workload: Workload) {
  const response = await fetch(`/demo/recovery-results.json?workload=${workload}`);
  if (response.status === 404) return null;
  if (!response.ok) throw Error();
  const report = await response.json() as Report;
  if (report.version !== 2 || !report.runs.some((run) => run.case === "backoff") ||
      !report.runs.some((run) => run.case !== "backoff")) throw Error();
  return report;
}

async function initialize() {
  try {
    const keys = Object.keys(workloads) as Workload[];
    const loaded = await Promise.allSettled(keys.map(loadReport));
    const available = new Map(keys.flatMap((key, i) => {
      const result = loaded[i];
      return result?.status === "fulfilled" && result.value ? [[key, result.value] as const] : [];
    }));
    if (!available.size) {
      detail.textContent = loaded.some((item) => item.status === "rejected")
        ? "Could not read the recovery results." : "No recovery run yet.";
      return;
    }
    workloadPicker.replaceChildren(...[...available.keys()].map((key) => new Option(workloads[key], key)));
    const requested = new URLSearchParams(location.search).get("workload");
    if (requested && available.has(requested as Workload)) workloadPicker.value = requested;
    workloadField.hidden = available.size < 2;
    let report = available.get(workloadPicker.value as Workload)!;
    let aggregate: Run[];
    const chosen = () => {
      const all = seedPicker.value === "all";
      const rows = all ? aggregate : report.runs.filter((run) => run.seed === Number(seedPicker.value));
      const selected = rows.find((run) => run.case === casePicker.value)!;
      const baseline = rows.find((run) => run.case === matchedReferences[selected.case])
        ?? rows.find((run) => run.case === "backoff")!;
      return { all, rows, baseline, selected };
    };
    const plot = () => {
      const { baseline, selected } = chosen();
      draw(report, baseline, selected);
    };
    const update = () => {
      const { all, rows, baseline, selected } = chosen();
      referenceLegend.textContent = baseline.label;
      legend.textContent = selected.label;
      showPaired(report, baseline, selected, all);
      showObservations(report, baseline, selected, all);
      showStability(report, baseline, selected, all);
      fillTable(rows, selected.case);
      plot();
    };
    const showWorkload = () => {
      const key = workloadPicker.value as Workload;
      report = available.get(key)!;
      if (key === "backoff") {
        report = { ...report, runs: report.runs.toSorted((a, b) => (a.backoff_base_s ?? 0) - (b.backoff_base_s ?? 0)) };
      }
      aggregate = acrossSeeds(report.runs);
      const seeds = [...new Set(report.runs.map((run) => run.seed))];
      const { config } = report;
      const sdk = config.openai_sdk ? ` | OpenAI SDK ${config.openai_sdk}` : "";
      const service = `${Math.round((config.service_s ?? .04) * 1000)} ms service`;
      detail.textContent = `${workloads[key]} | ${config.rate} req/s | ${service} | ${config.duration} s run | ${config.deadline} s deadline | ${seeds.length} ${seeds.length === 1 ? "seed" : "seeds"}${sdk}`;
      seedPicker.replaceChildren(...seeds.map((seed) => new Option(String(seed), String(seed))));
      seedField.hidden = seeds.length === 1;
      if (!seedField.hidden) {
        seedPicker.add(new Option("All seeds (median)", "all"), 0);
        seedPicker.value = "all";
      }
      const previousCase = casePicker.value;
      const cases = aggregate.filter((run) => run.case !== "backoff");
      casePicker.replaceChildren(...cases.map((run) => new Option(run.label, run.case)));
      const preferred = featuredCases[key] ?? previousCase;
      if (cases.some((run) => run.case === preferred)) {
        casePicker.value = preferred;
      }
      update();
    };
    casePicker.addEventListener("change", update);
    seedPicker.addEventListener("change", update);
    workloadPicker.addEventListener("change", showWorkload);
    results.hidden = false;
    showWorkload();
    new ResizeObserver(plot).observe(chart);
  } catch {
    detail.textContent = "Could not read the recovery results.";
    results.hidden = true;
  }
}

void initialize();
