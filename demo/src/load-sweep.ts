export {};

type Metric = "on_time" | "amplification" | "recovery_s" | "p99_s";
type Run = Record<Metric, number | null> & { case: "backoff" | "client-cap"; seed: number };
type Match = "utilization" | "rate";
const shapeLabels = { hard: "Hard outage", "early-heavy": "Early-heavy", "late-heavy": "Late-heavy" } as const;
type Shape = keyof typeof shapeLabels;
type Cell = Record<Match, number> & { workers: number; runs: Run[]; outage_shape?: Shape };
type Report = { version: number; config: {
  duration: number; outage: [number, number]; deadline: number; service_s: number;
  outage_shapes?: Partial<Record<Shape, number[]>>;
}; cells: Cell[] };
const studies = { load: "Load grid", shapes: "Outage shapes" } as const;
type Study = keyof typeof studies;

const detail = document.querySelector("#run-detail")!;
const results = document.querySelector<HTMLElement>("#load-results")!;
const table = document.querySelector<HTMLTableElement>("#load-table")!;
const comparisonTitle = document.querySelector("#comparison-title")!;
const groupKey = document.querySelector("#group-key")!;
const groupDetail = document.querySelector("#group-detail")!;
const matchField = document.querySelector<HTMLFieldSetElement>("#load-match")!;
const studyLabel = document.querySelector<HTMLElement>("#study-label")!;
const studyField = document.querySelector<HTMLSelectElement>("#load-study")!;
const rateRadio = document.querySelector<HTMLInputElement>("#match-rate")!;
const metricField = document.querySelector<HTMLSelectElement>("#load-metric")!;
const seedField = document.querySelector<HTMLSelectElement>("#load-seed")!;

function median(values: (number | null)[]) {
  const ordered = values.filter((value) => value !== null).sort((a, b) => a - b);
  const mid = Math.floor(ordered.length / 2);
  if (!ordered.length) return null;
  return ordered.length % 2 ? ordered[mid]! : (ordered[mid - 1]! + ordered[mid]!) / 2;
}

function format(value: number | null, metric: Metric, delta = false) {
  if (value === null) return metric === "recovery_s" && !delta ? "Not confirmed" : "n/a";
  const pct = metric === "on_time";
  const digits = pct ? 1 : 2;
  const formatted = new Intl.NumberFormat("en-US", {
    minimumFractionDigits: digits, maximumFractionDigits: digits, signDisplay: delta ? "exceptZero" : "never",
  }).format(pct ? value * 100 : value);
  return formatted + (pct ? (delta ? " pp" : "%") : metric === "amplification" ? "x" : " s");
}

const rate = (n: number) => `${Number(n.toFixed(3))} req/s`;
const load = (n: number) => `${Number((n * 100).toFixed(2))}%`;

function matched(cells: Cell[], by: Match) {
  return [...Map.groupBy(cells, (cell) => cell[by])]
    .filter(([, group]) => new Set(group.map((cell) => cell.workers)).size > 1)
    .sort(([a], [b]) => a - b);
}

function summarize(cell: Cell, metric: Metric, seed: string) {
  const runs = cell.runs.filter((run) => seed === "all" || run.seed === Number(seed));
  const baseline = runs.filter((run) => run.case === "backoff");
  const cap = runs.filter((run) => run.case === "client-cap");
  if (metric === "recovery_s" && seed === "all") {
    const confirmed = (rows: Run[]) => rows.filter((row) => row.recovery_s !== null).length;
    const a = confirmed(baseline), b = confirmed(cap), change = b - a;
    return [`${a}/${baseline.length}`, `${b}/${cap.length}`, `${change > 0 ? "+" : ""}${change} seeds`];
  }
  const baselineBySeed = new Map(baseline.map((run) => [run.seed, run[metric]]));
  const deltas = cap.flatMap((run) => {
    const control = baselineBySeed.get(run.seed);
    return control == null || run[metric] === null ? [] : [run[metric] - control];
  });
  return [format(median(baseline.map((run) => run[metric])), metric),
    format(median(cap.map((run) => run[metric])), metric), format(median(deltas), metric, true)];
}

function populate(reports: Map<Study, Report>) {
  studyField.replaceChildren(...[...reports.keys()].map((study) => new Option(studies[study], study)));
  studyLabel.hidden = reports.size < 2;
  const requested = new URLSearchParams(location.search).get("study");
  if (requested && reports.has(requested as Study)) studyField.value = requested;
  let report: Report;

  function render() {
    const profiles = report.config.outage_shapes;
    const sameRate = rateRadio.checked && !rateRadio.disabled;
    const metric = metricField.value as Metric;
    groupKey.textContent = profiles ? "Shape" : "Workers";
    groupDetail.textContent = profiles ? "500 probability" : sameRate ? "Nominal load" : "Offered";
    groupDetail.setAttribute("title", profiles ? "First, middle, and final third of the outage" : "");
    const groups = profiles ? [...Map.groupBy(report.cells, (cell) => {
      const workers = cell.workers === 1 ? "1 worker" : `${cell.workers} workers`;
      return `${load(cell.utilization)} nominal | ${workers} | ${rate(cell.rate)}`;
    })] : matched(report.cells, sameRate ? "rate" : "utilization").map(([key, cells]) =>
      [sameRate ? rate(key) : `${load(key)} nominal utilization`, cells] as const);
    const bodies = groups.map(([key, cells]) => {
      const body = document.createElement("tbody");
      const title = body.insertRow();
      title.className = "load-group";
      const header = document.createElement("th");
      header.scope = "rowgroup";
      header.colSpan = 5;
      header.textContent = key;
      title.append(header);
      for (const cell of cells.toSorted((a, b) => a.workers - b.workers)) {
        const row = body.insertRow();
        const worker = document.createElement("th");
        worker.scope = "row";
        const shape = cell.outage_shape;
        worker.textContent = shape ? shapeLabels[shape] : String(cell.workers);
        row.append(worker);
        const phaseDetail = shape && profiles?.[shape]?.map(load).join(" / ");
        const values = [phaseDetail || (sameRate ? load(cell.utilization) : rate(cell.rate)), ...summarize(cell, metric, seedField.value)];
        for (const value of values) row.insertCell().textContent = value;
      }
      return body;
    });
    table.replaceChildren(table.tHead!, ...bodies);
  }

  function showStudy() {
    report = reports.get(studyField.value as Study)!;
    const seeds = [...new Set(report.cells.flatMap((cell) => cell.runs.map((run) => run.seed)))].sort((a, b) => a - b);
    seedField.replaceChildren(new Option("All seeds (median)", "all"), ...seeds.map((seed) => new Option(String(seed), String(seed))));
    const { duration, outage, deadline, service_s, outage_shapes } = report.config;
    detail.textContent = `${seeds.length} paired seeds | ${service_s * 1000} ms service | ${duration} s run | outage ${outage.join("-")} s | ${deadline} s target`;
    matchField.hidden = !!outage_shapes;
    comparisonTitle.textContent = outage_shapes ? "Outage and policy comparison" : "Worker and policy comparison";
    rateRadio.disabled = !matched(report.cells, "rate").length;
    render();
  }

  results.addEventListener("change", (event) => event.target === studyField ? showStudy() : render());
  showStudy();
  results.hidden = false;
}

async function loadReport(study: Study) {
  const response = await fetch(`/demo/load-sweep-results.json?study=${study}`);
  if (response.status === 404) return null;
  if (!response.ok) throw new Error("Report unavailable");
  const report: Report = await response.json();
  if (report.version !== 1 || !report.cells.length) throw new Error("Invalid report");
  return report;
}

async function initialize() {
  const keys = Object.keys(studies) as Study[];
  const loaded = await Promise.allSettled(keys.map(loadReport));
  const reports = new Map(keys.flatMap((key, index) => {
    const item = loaded[index];
    return item?.status === "fulfilled" && item.value ? [[key, item.value] as const] : [];
  }));
  if (!reports.size) {
    detail.textContent = loaded.some((item) => item.status === "rejected") ? "Could not read the load sweep." : "No load sweep yet.";
    return;
  }
  populate(reports);
}

void initialize().catch(() => { detail.textContent = "Could not read the load sweep."; });
