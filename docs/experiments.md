# Experiment runbook

[Back to Generation Interrupted](../README.md)

Start the demo with the [quick start](../README.md#quick-start). Run report commands from the repository root in a second terminal. Reports under `results/raw/` are local outputs; the chart values in the README come from completed runs, not bundled reports.

## Streaming labs

| Lab | Setup and measurements |
| --- | --- |
| [Streams](http://127.0.0.1:8766/demo/) | Start with **After text**; compare with **Before text** and **No disconnect**. Scripted arms receive identical text over separate connections, with at most one retry. Live mode runs one policy at a time. |
| [Reconnect](http://127.0.0.1:8766/demo/reconnect/) | Cut after three nonblank chunks. A browser cut leaves one upstream generation running; reattachment replays buffered chunks. An upstream cut preserves the partial answer, but continuing or regenerating is a new model request. The single active run uses bounded memory, a 5 s detached grace period, and a 90 s generation deadline. |
| [Stall](http://127.0.0.1:8766/demo/stall/) | Scripted delivery pauses after three chunks for 2.4 s or 16 s. Compare waiting, stopping after 1.2 s of silence, and retrying at that threshold if at least 2.2 s remain. Try a 3 s deadline to see the retry skipped. |
| [Shared backend](http://127.0.0.1:8766/demo/shared-backend/) | Queue B or dispatch it after A's third nonblank chunk. The cancellation trial dispatches B at the same point in both arms. Saturation sends C after both streams start, then lets both finish or stops A after 250 ms. Trial order can be reversed for cancellation and saturation. |

A terminal frame is required for completion. First text means receipt of a nonblank chunk, not browser paint. Chunk counts are not token counts. Scripted cursor replay returns the fixture's suffix; it is not an inference checkpoint. The budgeted restart charges scripted work or reported output tokens against the restart budget. If an interrupted live attempt has no usage frame, the configured output limit is reserved instead. This is not billing or proof that upstream compute stopped.

Reconnect counts HTTP attempts after configuration passes validation. An attempt may fail to reach Ollama; reattaching the browser does not start another attempt.

A live retry can handle transient HTTP and transport failures. Missing models, invalid stream data, and output-length limits are terminal. A valid numeric `Retry-After` is a minimum delay, still subject to the deadline. An injected browser cut, an upstream connection cut, and a GPU stall are distinct.

<details>
<summary>Local Ollama setup</summary>

Live mode uses Ollama on loopback. It defaults to `openbmb/minicpm5:q8_0`; set `GENERATION_CUT_OLLAMA_MODEL` to another installed model before starting Generation Interrupted. Start with **No disconnect**, without other model traffic. Responses shorter than the configured cut finish normally.

For the live saturation comparison, use two parallel slots. To leave your regular server untouched on Windows, run a separate instance:

```powershell
$env:OLLAMA_HOST = '127.0.0.1:11435'
$env:OLLAMA_NUM_PARALLEL = '2'
ollama serve
```

Set `$env:GENERATION_CUT_OLLAMA_PORT = '11435'` in the shell that starts Generation Interrupted. Without the override it uses 11434. Live overlap means text from B arrived between chunks from A; it does not measure GPU scheduling. The saturation trial is incomplete if A and B do not remain active together, or if C emits text before the stop point. Closing a stream does not establish when upstream work stops.

</details>

## Recovery lab

The [dashboard](http://127.0.0.1:8766/demo/recovery/) reads each report independently. Paired arms use fresh loopback servers and the same scheduled arrivals. Built-in retry jitter is seeded separately from arrivals; SDK jitter is not controlled. The deadline is a scoring target, not a cancellation rule. Server attempts include shed requests and drain; a client cap can reject a logical request before an HTTP attempt, and that remains a miss.

First recovery requires three consecutive, populated post-outage arrival seconds with at least 90% on-time completions in each. The scorer uses actual completion timestamps, credits no completions after the observation cutoff, and confirms only after the buckets close. Aggregate values are descriptive medians; paired changes match seeds, not differences of independent medians. Fast rejections can lower p99 without improving success.

<details>
<summary>Standard, stacked SDK retries, and cancellation under timeout pressure</summary>

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery
uv run --no-project --python 3.12 --with-editable '.[dev]' -- python -m generation_cut.recovery --duration 8 --outage 1 3 --seeds 3 --sdk --case bounded-queue --case cancel --case queue-cancel --case sdk-only --case sdk-stacked --case sdk-budget
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery --rate 6 --service-time .30 --duration 8 --outage 1 3 --seeds 3 --case bounded-queue --case queue-cancel --case retry-budget --case budget-whole --out results/raw/recovery-timeout.json
```

The SDK run writes the Standard report; use `--out` to keep a separate copy. It uses the real OpenAI Python SDK against the local mock without credentials, with additional app and workflow retry layers implemented as lab wrappers. The dashboard separates SDK calls, transport attempts, and server receipts. In one paired run, the server received 1,683 attempts without a shared budget and 403 with it for the same 359 offered requests.

Server safeguards include bounded queues, early shedding, and cancellation of abandoned work. For **Timeout pressure**, compare bounded queue with and without cancellation. In one run cancellation stopped 79 admitted attempts but only improved on-time completion in one of three matched pairs. The fractional-credit budget lets any positive credit fund a whole retry; the separate whole-credit budget requires one full credit. The circuit breaker opens after three retryable failures, waits half a second, and allows one probe; it does not cancel in-flight requests.

</details>

<details>
<summary>Client cap, worker count, and timeout</summary>

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery --rate 16 --duration 10 --outage 2 5 --seeds 3 --case client-cap --out results/raw/recovery-client-cap.json
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery --rate 16 --service-time .04 --duration 10 --outage 2 5 --seeds 3 --deadline 1 --case short-timeout --case two-workers --case two-workers-short --out results/raw/recovery-sensitivity.json
```

The client cap holds at most four active HTTP attempts, with no local waiting queue. It does not limit work already abandoned at the server. In one run it improved on-time completion by 46.6–52.3 percentage points against uncapped backoff, but worsened some pre-outage cohorts.

The worker/timeout comparison changes one versus two workers and a 1 s versus 400 ms attempt timeout at the same offered rate. It holds the logical target at 1 s. In one run a second worker helped; shortening the timeout did not improve on-time completion or lower server attempts. This is not a matched-utilization comparison.

</details>

<details>
<summary>Observation cutoffs and backoff base delay</summary>

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery --rate 16 --duration 40 --outage 2 5 --seeds 3 --case client-cap --post-outage-windows 5 21 35 --out results/raw/recovery-observation.json
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery --rate 16 --duration 15 --outage 2 5 --seeds 3 --case backoff-25ms --case backoff-400ms --case backoff-800ms --out results/raw/recovery-backoff.json
```

**Observation windows** scores the same trace at three cutoffs, while the chart shows the full trace. The capped client confirmed in two seeds by 5 s and the third at 7.063 s; uncapped backoff did not confirm by 35 s. This measures whether recovery was ever confirmed, not whether it held.

**Backoff base delay** compares 25, 100, 400, and 800 ms bases. The base is the upper bound of the first full-jitter delay, not a fixed wait. The ceiling, attempt limit, and 1 s target stay fixed. Longer delays helped in one seed but did not confirm recovery in any; the largest base increased p99 in all three. This outage returns HTTP 500 and does not exercise `Retry-After`.

</details>

### Sustained recovery

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.recovery --rate 16 --duration 30 --outage 2 5 --seeds 3 --case client-cap --sustained-recovery
```

Open [Sustained recovery](http://127.0.0.1:8766/demo/recovery/?workload=stability). Independently of the first-recovery marker, the scorer looks for two adjacent five-second windows with at least 90% of offered requests completing on time in each. Windows start on whole seconds and must have traffic. All deadlines must elapse before either check. After a verified streak, the first later five-second window below 90% counts as a drop. **Not verified** means there was not enough fully observed, populated traffic for the check; **not observed** means the available windows did not contain the event.

In one three-seed run the cap passed at 11 s in every seed, with no later fully observed five-second drop by the 25 s cutoff. Uncapped backoff reached neither milestone. The command writes `results/raw/recovery-stability.json`.

### Load and workers

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.load_sweep
```

Open [Load and workers](http://127.0.0.1:8766/demo/recovery/load/). The default grid compares 32%, 64%, and 88% nominal utilization at one and two workers, with three seeds per cell and a two-second outage. Nominal utilization is offered rate × service time ÷ workers; it excludes retries and HTTP overhead. With 40 ms service the paired rates are 8/16, 16/32, and 22/44 requests/s. **Offered rate** compares cells at a common rate, instead of scaling load with worker count. The cap stays fixed at four active HTTP attempts across worker counts.

The default runs 36 sequential trials and writes `results/raw/load-sweep.json`. Use `--seed-start` and `--out` for a separate replication. Use `--fixed-rate 16` to include a common rate in a custom grid. In the default run, a second worker improved recovery at a fixed 16 requests/s; that advantage did not carry over to the 64% matched-utilization pair. Neither policy confirmed at 88% within seven seconds of the outage ending.

### Outage shapes

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.load_sweep --utilizations .64 --outage 1 4 --duration 11 --outage-shapes early-heavy late-heavy
```

Open [Outage shapes](http://127.0.0.1:8766/demo/recovery/load/?study=shapes). Early-heavy uses 90%, 60%, then 30% probability of a 500 across three equal phases. Late-heavy reverses the order. Both average 60% over time, but retries can change actual traffic and failure counts. Failure assignments happen at server receipt and still cost service time. Reported assignments are not the number of 500 responses clients received. Add `hard` to the shape list for a 100% failure profile. This study writes `results/raw/load-shapes.json` separately.

In the two-worker comparisons, the cap sent fewer attempts but completed fewer requests on time in every pair. The earlier hard-outage grid has different duration and severity; cross-grid differences cannot be attributed to shape alone.

### Single faults

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.single_fault
```

Open [Single faults](http://127.0.0.1:8766/demo/recovery/single-fault/). Immediate retry and backoff each get five seeds and 200 logical requests per fault and seed. The server injects a 30% per-attempt fault probability. Both arms draw from the same fault sequence, keyed to order of arrival on the wire rather than logical request identity. This is closed-loop traffic. Both allow four attempts, with a 1 s timeout. Hangs last 3 s; backoff honors the 50 ms `Retry-After` on 429s.

In one run, all paired usable-response differences were within one percentage point. Backoff increased p99 time to outcome for hangs in every seed (median +0.468 s). Per-run p99 includes failures. Seed ranges are descriptive, not confidence intervals or evidence of equivalence.

## Streaming admission

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.admission --source ollama
```

Open [Admission](http://127.0.0.1:8766/demo/admission/). Omit `--source ollama` for scripted streaming. The arms share an offered burst, one worker, one waiting slot, and no retries. Both expire queued jobs past their first-text target. Deadline-aware admission also rejects incoming work estimated to miss either target; it does not preempt active work. Estimates follow a discarded warm-up and three calibration streams. The deliberately tight request shows what happens when rejecting it leaves room for a later one. Rates use all offered requests, and p95 chunk gaps include partial streams. This is an admission comparison, not a retry storm on a real model.

## Baseline benchmark

The [saved baseline JSON](experiment.json) contains the original results. To print them without rerunning the benchmark, pass `docs/experiment.json` to the `generation_cut.report` command below. The single-fault baseline used one seed and 200 logical requests per policy and fault, with eight clients and a 30% per-attempt fault probability. The no-retry policy did not validate output, so its garbage-output result overstates usable success. Do not compare those cells as if all policies used the same validation.

<details>
<summary>Storm methodology and reproduction</summary>

The original 30 s storm offered 16 requests/s to one mock worker (40 ms per attempt, 25 attempts/s nominal capacity). A total outage at seconds 5–9 consumed capacity before returning errors. Work continued after timeouts and the queue was unbounded. The retry budget was 10% of offered requests plus ten initial credits.

| Policy | Overall success | Attempts reaching server per request | Qualifying streak |
| --- | ---: | ---: | --- |
| No retry | 84–89% | 1.00 | Started at outage end |
| Immediate retry | 13–17% | 3.5–3.6 | Not observed |
| Jittered backoff | 13–17% | 3.5–3.6 | Not observed |
| Backoff + budget | 84–89% | 1.05–1.06 | Started at outage end |

“Started at outage end” identifies the first arrival bucket in a streak that qualified **in hindsight**, not zero-second confirmation. The original scorer uses eventual outcomes rather than the newer on-time scoring and cutoff rules. Legacy scheduling can also admit one arrival beyond the nominal end. Compare the two experiments according to their own metrics.

```sh
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.experiment --seeds 3 --duration 30
uv run --no-project --python 3.12 --with-editable . -- python -m generation_cut.report results/raw/baseline-reproduction.json
```

The benchmark defaults to `results/raw/baseline-reproduction.json`. Use `--out` for a different report path; the CLI rejects the path of the saved baseline.

</details>

## Measurement limits

- **Local mock:** Each sweep fixes service time within a run. Worker slots, deadlines, and outage probabilities are controlled inputs, not a model of hosted inference or variable GPU service. Retry delay ceilings, attempt counts, budget sizes, and retry headers have not been swept jointly.
- **Finite observation and small samples:** A healthy streak does not prove sustained health indefinitely. Absence of a qualifying streak is bounded by the observation window. Seed ranges are not general guarantees.
- **Real inference:** The local Ollama labs test streaming behavior and admission on a specific model and machine. They do not establish hosted retry-storm behavior, GPU cancellation, or general throughput gains. Hedging has not been evaluated.
