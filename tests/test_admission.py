import asyncio
import time

from generation_cut.admission import Arrival, Estimate, fits, observe, run_case, summarize


def test_admission_checks_first_text_and_completion_independently():
    estimate = Estimate(.1, .5)
    assert fits(estimate, .2, .31, .7)
    assert not fits(estimate, .2, .29, 1.0)
    assert not fits(estimate, .2, .5, .69)


def test_rejecting_doomed_work_leaves_the_queue_slot_for_the_next_arrival():
    async def source():
        yield "First"
        await asyncio.sleep(.16)
        yield "Second"

    arrivals = [Arrival(0, .1, .5, "Standard"), Arrival(.02, .08, .5, "Tight first text"),
                Arrival(.04, .3, .5, "Standard")]

    async def run():
        estimate = Estimate(.01, .18)
        return [await run_case(source, arrivals, estimate, predict=predict) for predict in (False, True)]

    bounded, deadline = asyncio.run(run())
    assert [sample.status for sample in bounded] == ["complete", "expired_in_queue", "queue_full"]
    assert [sample.status for sample in deadline] == ["complete", "deadline_rejected", "complete"]
    assert summarize(bounded, arrivals)["first_on_time"] == 1
    assert summarize(deadline, arrivals)["completion_on_time"] == 2


def test_first_text_can_be_on_time_when_completion_misses_its_deadline():
    async def source():
        yield "Text"
        await asyncio.sleep(.4)
        yield "Too late"

    async def run():
        now = time.perf_counter()
        return await observe(source, now, now + .08)

    sample = asyncio.run(run())
    summary = summarize([sample], [Arrival(0, .07, .08, "Standard")])
    assert sample.status == "completion_timeout"
    assert summary["first_on_time"] == 1 and summary["completion_on_time"] == 0
    assert summary["p95_gap_ms"] is None


def test_gap_samples_exclude_blank_frames_and_do_not_require_all_requests_to_complete():
    async def source():
        yield "First"
        yield " "
        yield "Second"
        await asyncio.sleep(.4)

    async def run():
        now = time.perf_counter()
        return await observe(source, now, now + .08)

    sample = asyncio.run(run())
    summary = summarize([sample], [Arrival(0, .07, .08, "Standard")])
    assert sample.status == "completion_timeout"
    assert summary["gap_samples"] == 1 and summary["p95_gap_ms"] is not None
