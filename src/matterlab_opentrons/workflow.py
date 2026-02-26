import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from prefect import flow, get_run_logger

from .OpenTronsControl import OpenTrons


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _safe_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_jsonable(v) for v in value]
    return repr(value)


@dataclass
class WorkflowStep:
    method: str
    kwargs: Dict[str, Any] = field(default_factory=dict)
    name: Optional[str] = None
    retries: int = 0
    retry_delay_seconds: float = 0.0
    continue_on_error: bool = False
    capture_result: bool = True


def step(method: str, **kwargs: Any) -> WorkflowStep:
    """Convenience helper for concise workflow step definitions."""
    return WorkflowStep(method=method, kwargs=kwargs)


@dataclass
class StepRecord:
    index: int
    step_name: str
    method: str
    kwargs: Dict[str, Any]
    started_at: str
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    status: str = "pending"
    attempts: int = 0
    result: Any = None
    error: Optional[str] = None


def _execute_step_with_retries(
    ot: OpenTrons,
    wf_step: WorkflowStep,
    index: int,
    total_steps: int,
    logger,
) -> StepRecord:
    record = StepRecord(
        index=index,
        step_name=wf_step.name or f"{index:03d}_{wf_step.method}",
        method=wf_step.method,
        kwargs=_safe_jsonable(wf_step.kwargs),
        started_at=_utc_now_iso(),
        status="running",
    )

    if not hasattr(ot, wf_step.method):
        record.status = "failed"
        record.finished_at = _utc_now_iso()
        record.error = f"Unknown OpenTrons method: {wf_step.method}"
        return record

    method = getattr(ot, wf_step.method)
    max_attempts = max(1, wf_step.retries + 1)
    last_error = None

    for attempt in range(1, max_attempts + 1):
        record.attempts = attempt
        attempt_started = time.perf_counter()
        logger.info(
            "Step %s/%s: %s.%s(%s) [attempt %s/%s]",
            index,
            total_steps,
            ot.__class__.__name__,
            wf_step.method,
            ", ".join(f"{k}={v!r}" for k, v in wf_step.kwargs.items()),
            attempt,
            max_attempts,
        )
        try:
            result = method(**wf_step.kwargs)
            record.status = "completed"
            if wf_step.capture_result:
                record.result = _safe_jsonable(result)
            record.duration_seconds = round(time.perf_counter() - attempt_started, 3)
            record.finished_at = _utc_now_iso()
            return record
        except Exception as exc:  # noqa: BLE001 - want exact remote errors in report
            last_error = exc
            logger.exception("Step %s failed on attempt %s/%s", index, attempt, max_attempts)
            if attempt < max_attempts and wf_step.retry_delay_seconds > 0:
                time.sleep(wf_step.retry_delay_seconds)

    record.status = "failed"
    record.duration_seconds = None
    record.finished_at = _utc_now_iso()
    record.error = repr(last_error)
    return record


def _write_report(report: Dict[str, Any], report_dir: str) -> str:
    out_dir = Path(report_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = report.get("run_id") or uuid.uuid4().hex
    out_path = out_dir / f"opentrons_workflow_{run_id}.json"
    out_path.write_text(json.dumps(report, indent=2, default=str))
    return str(out_path)


@flow(log_prints=True, name="opentrons-local-workflow")
def run_local_workflow(
    steps: List[WorkflowStep],
    host_alias: str,
    password: str = "",
    simulation: bool = True,
    home_before_start: bool = True,
    home_before_close: bool = False,
    close_session: bool = True,
    stop_on_error: bool = True,
    write_report: bool = True,
    report_dir: str = "workflow_runs",
) -> Dict[str, Any]:
    """
    Execute a local, stateful Opentrons workflow with structured Prefect logging + JSON step journal.

    This is intentionally single-session and sequential so robot state (loaded labware, current location,
    picked-up tips, module state) remains valid across steps.
    """

    logger = get_run_logger()
    started = time.perf_counter()
    run_id = uuid.uuid4().hex[:12]
    step_records: List[StepRecord] = []
    failure_count = 0
    ot = None

    report: Dict[str, Any] = {
        "run_id": run_id,
        "started_at": _utc_now_iso(),
        "host_alias": host_alias,
        "simulation": simulation,
        "home_before_start": home_before_start,
        "home_before_close": home_before_close,
        "stop_on_error": stop_on_error,
        "step_count": len(steps),
        "steps": [],
        "status": "running",
    }

    logger.info("Starting local Opentrons workflow %s with %s steps", run_id, len(steps))

    try:
        ot = OpenTrons(host_alias=host_alias, password=password, simulation=simulation)
        if home_before_start:
            ot.home()

        for index, wf_step in enumerate(steps, start=1):
            record = _execute_step_with_retries(
                ot=ot,
                wf_step=wf_step,
                index=index,
                total_steps=len(steps),
                logger=logger,
            )
            step_records.append(record)
            report["steps"] = [_safe_jsonable(asdict(r)) for r in step_records]

            if record.status == "failed":
                failure_count += 1
                if stop_on_error and not wf_step.continue_on_error:
                    logger.error("Stopping workflow at failed step %s: %s", index, record.step_name)
                    break

        if home_before_close and ot is not None:
            try:
                ot.home()
            except Exception:
                logger.exception("Failed to home before close")

        report["status"] = "failed" if failure_count else "completed"
        return report
    except Exception as exc:  # noqa: BLE001
        logger.exception("Workflow crashed before normal completion")
        report["status"] = "failed"
        report["fatal_error"] = repr(exc)
        return report
    finally:
        if close_session and ot is not None:
            try:
                ot.close_session()
            except Exception:
                logger.exception("Error while closing Opentrons session")

        report["finished_at"] = _utc_now_iso()
        report["duration_seconds"] = round(time.perf_counter() - started, 3)
        report["failure_count"] = failure_count
        report["completed_steps"] = sum(1 for r in step_records if r.status == "completed")
        report["steps"] = [_safe_jsonable(asdict(r)) for r in step_records]

        if write_report:
            try:
                path = _write_report(report, report_dir=report_dir)
                report["report_path"] = path
                logger.info("Workflow report written to %s", path)
            except Exception:
                logger.exception("Failed to write workflow report")


def build_ot2_smoke_plan() -> List[WorkflowStep]:
    """Example plan for core pipetting wrappers (assumes labware/instruments already loaded)."""
    return [
        WorkflowStep("home", name="home_robot"),
        WorkflowStep("comment", {"message": "OT2 smoke plan start"}, name="comment_start"),
        WorkflowStep(
            "get_location_from_labware",
            {"labware_nickname": "tip_300_96_1", "position": "A1", "top": 0},
            name="tiprack_a1",
            capture_result=False,
        ),
        WorkflowStep("pick_up_tip", {"pip_name": "p300"}, name="pickup_p300", capture_result=False),
        WorkflowStep(
            "get_location_from_labware",
            {"labware_nickname": "beaker", "position": "A1", "bottom": 5},
            name="beaker_a1",
            capture_result=False,
        ),
        WorkflowStep("aspirate", {"pip_name": "p300", "volume": 100}, name="aspirate_100", capture_result=False),
        WorkflowStep(
            "get_location_from_labware",
            {"labware_nickname": "plate_96_1", "position": "A1", "top": -1},
            name="plate_a1",
            capture_result=False,
        ),
        WorkflowStep("dispense", {"pip_name": "p300", "volume": 100}, name="dispense_100", capture_result=False),
        WorkflowStep("blow_out", {"pip_name": "p300"}, name="blow_out", capture_result=False),
        WorkflowStep("return_tip", {"pip_name": "p300"}, name="return_tip", capture_result=False),
    ]


def build_flex_gripper_smoke_plan() -> List[WorkflowStep]:
    """Example plan for module latch + low-level gripper jaw wrappers."""
    return [
        WorkflowStep("home", name="home_robot"),
        WorkflowStep("hs_latch_open", {"nickname": "hs"}, name="hs_latch_open", capture_result=False),
        WorkflowStep("hs_latch_close", {"nickname": "hs"}, name="hs_latch_close", capture_result=False),
        WorkflowStep("gripper_is_attached", name="gripper_check"),
        WorkflowStep("gripper_jaw_limits", name="gripper_jaw_limits"),
        WorkflowStep("gripper_jaw_width", name="gripper_jaw_width"),
        WorkflowStep("gripper_open_jaw", name="gripper_open", capture_result=False, continue_on_error=True),
        WorkflowStep("gripper_close_jaw", name="gripper_close", capture_result=False, continue_on_error=True),
        WorkflowStep("gripper_open_jaw", name="gripper_open_restore", capture_result=False, continue_on_error=True),
    ]
