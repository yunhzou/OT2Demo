import json
from pathlib import Path

from prefect import flow

from matterlab_opentrons import run_local_workflow
from matterlab_opentrons.experiment_spec import (
    PlateProcessSpec,
    compile_plate_process_steps,
)


DEFAULT_SPEC_PATH = Path("demo/plate_process_spec.json")


def write_default_spec(path: Path = DEFAULT_SPEC_PATH) -> Path:
    spec = PlateProcessSpec()
    path.write_text(json.dumps(spec.to_dict(), indent=2))
    return path


@flow(log_prints=True)
def demo_ot2_optic_hte_dilution_workflow(
    simulation: bool = True,
    spec_path: str = "",
):
    spec_file = Path(spec_path) if spec_path else DEFAULT_SPEC_PATH
    if spec_file.exists():
        spec = PlateProcessSpec.from_json(str(spec_file))
        spec.simulation = simulation
    else:
        spec = PlateProcessSpec(simulation=simulation)

    steps = compile_plate_process_steps(spec)
    return run_local_workflow(
        steps=steps,
        host_alias=spec.host_alias,
        password=spec.password,
        simulation=spec.simulation,
        home_before_start=True,
        home_before_close=False,
        close_session=True,
        stop_on_error=True,
        write_report=True,
        report_dir="workflow_runs",
    )


if __name__ == "__main__":
    # Uncomment to bootstrap a JSON spec you can edit instead of changing code.
    # write_default_spec()
    demo_ot2_optic_hte_dilution_workflow(simulation=True)
