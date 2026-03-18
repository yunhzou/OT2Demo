import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .workflow import WorkflowStep


def _well_name(i: int) -> str:
    return f"{chr(65 + i // 12)}{i % 12 + 1}"


def _slug_token(value: str) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value))
    while "__" in token:
        token = token.replace("__", "_")
    token = token.strip("_")
    return token or "stage"


def _parse_well_token(well: str) -> Tuple[int, int]:
    well = well.strip().upper()
    if len(well) < 2 or not well[0].isalpha():
        raise ValueError(f"Invalid well token: {well!r}")
    row = ord(well[0]) - ord("A")
    col = int(well[1:]) - 1
    if row < 0 or row > 7 or col < 0 or col > 11:
        raise ValueError(f"Well out of 96-well range: {well!r}")
    return row, col


def _well_from_row_col(row: int, col: int) -> str:
    return f"{chr(ord('A') + row)}{col + 1}"


def _expand_well_range(token: str) -> List[str]:
    start, end = [part.strip().upper() for part in token.split(":", 1)]
    r1, c1 = _parse_well_token(start)
    r2, c2 = _parse_well_token(end)
    r_lo, r_hi = sorted((r1, r2))
    c_lo, c_hi = sorted((c1, c2))
    return [_well_from_row_col(r, c) for r in range(r_lo, r_hi + 1) for c in range(c_lo, c_hi + 1)]


def resolve_well_selection(sample_num: int, well_selection: Optional[List[str]] = None) -> List[str]:
    """
    Resolve execution wells for a 96-well plate.

    Supported entries in `well_selection`:
    - explicit well: `A1`
    - rectangular range: `A1:H12`
    - first-N shorthand: `first:24`
    """
    if not well_selection:
        return [_well_name(i) for i in range(sample_num)]

    wells: List[str] = []
    for token in well_selection:
        normalized = token.strip().upper()
        if normalized.startswith("FIRST:"):
            n = int(normalized.split(":", 1)[1])
            wells.extend(_well_name(i) for i in range(n))
            continue
        if ":" in normalized:
            wells.extend(_expand_well_range(normalized))
            continue
        _parse_well_token(normalized)  # validate
        wells.append(normalized)

    # preserve order while deduplicating
    seen = set()
    ordered = []
    for w in wells:
        if w not in seen:
            seen.add(w)
            ordered.append(w)
    return ordered


@dataclass
class LabwareSpec:
    nickname: str
    location: str
    ot_default: bool = True
    loadname: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    config_path: str = ""
    tip_status_file: str = ""

    def to_runtime_dict(self, base_dir: Optional[Path] = None) -> Dict[str, Any]:
        resolved_config = self.config
        resolved_tip_status_file = self.tip_status_file
        if not self.ot_default and not resolved_config and self.config_path:
            cfg_path = Path(self.config_path)
            if not cfg_path.is_absolute() and base_dir is not None:
                cfg_path = base_dir / cfg_path
            resolved_config = json.loads(cfg_path.read_text())
        if resolved_tip_status_file:
            status_path = Path(resolved_tip_status_file)
            if not status_path.is_absolute() and base_dir is not None:
                status_path = base_dir / status_path
            resolved_tip_status_file = str(status_path)

        data = {
            "nickname": self.nickname,
            "location": self.location,
            "ot_default": self.ot_default,
            "config": resolved_config,
        }
        if self.ot_default:
            data["loadname"] = self.loadname
        if resolved_tip_status_file:
            data["tip_status_file"] = resolved_tip_status_file
        return data


@dataclass
class InstrumentSpec:
    nickname: str
    instrument_name: str
    mount: str
    ot_default: bool = True
    config: Dict[str, Any] = field(default_factory=dict)

    def to_runtime_dict(self) -> Dict[str, Any]:
        data = {
            "nickname": self.nickname,
            "instrument_name": self.instrument_name,
            "mount": self.mount,
            "ot_default": self.ot_default,
        }
        if not self.ot_default:
            data["config"] = self.config
        return data


@dataclass
class DissolveSpec:
    """Add solvent/reagent to each target well and mix in place."""

    source_labware: str = "resovir"
    source_well: str = "A1"
    source_bottom: float = 10
    add_volume_ul: float = 300

    target_labware: str = "plate_96_1"
    target_bottom_dispense: float = 1

    mix_repetitions: int = 3
    mix_volume_ul: float = 200
    delay_seconds: float = 1


@dataclass
class TransferSpec:
    """Transfer aliquot from per-well source to matching per-well destination."""

    source_labware: str = "plate_96_1"
    source_bottom: float = 3
    aspirate_volume_ul: float = 30
    source_lift_top: Optional[float] = 5

    destination_labware: str = "plate_96_2"
    destination_top: Optional[float] = -1
    destination_bottom: Optional[float] = None
    dispense_volume_ul: Optional[float] = 30

    delay_seconds: float = 1
    blow_out: bool = True
    mix_after_dispense_repetitions: int = 0
    mix_after_dispense_volume_ul: Optional[float] = None


@dataclass
class DilutionSpec:
    """Optional generic dilution step: add diluent to each well and mix."""

    diluent_labware: str = "resovir"
    diluent_well: str = "A1"
    diluent_bottom: float = 10
    diluent_volume_ul: float = 100

    target_labware: str = "plate_96_2"
    target_bottom_dispense: float = 1

    mix_repetitions: int = 0
    mix_volume_ul: float = 50
    delay_seconds: float = 1


@dataclass
class CsvDissolutionTarget:
    plate: str
    mix_repetitions: int = 3
    mix_volume_ul: float = 200
    delay_seconds: float = 1
    blow_out: bool = False


@dataclass
class CsvDilutionTarget:
    plate: str
    prefill: bool = True
    mix_repetitions: int = 3
    mix_volume_ul: float = 200
    delay_seconds: float = 1
    blow_out: bool = True


@dataclass
class CsvPlateSeriesSpec:
    """CSV matrix workflow keyed by plate nickname (<plate>.csv)."""

    # Dissolution volumes are loaded from <dissolution.plate>.csv
    dissolution: CsvDissolutionTarget = field(default_factory=lambda: CsvDissolutionTarget(plate="plate_96_1"))
    # Dilution transfer volumes are loaded from <plate>.csv for each target plate.
    dilutions: List[CsvDilutionTarget] = field(default_factory=list)

    solvent_labware: str = "resovir"
    solvent_well: str = "A1"
    solvent_bottom: float = 10

    dissolution_target_bottom: float = 1
    transfer_source_bottom: float = 3
    transfer_source_lift_top: Optional[float] = 5
    transfer_destination_bottom: float = 3




def _parse_csv_plate_series(raw: Dict[str, Any]) -> CsvPlateSeriesSpec:
    payload = dict(raw)

    dissolution_raw = payload.get("dissolution")
    if isinstance(dissolution_raw, list):
        if len(dissolution_raw) != 1:
            raise ValueError("csv_plate_series.dissolution list must contain exactly one object")
        dissolution_raw = dissolution_raw[0]
    if not isinstance(dissolution_raw, dict):
        raise ValueError("csv_plate_series.dissolution must be an object or [object]")
    payload["dissolution"] = CsvDissolutionTarget(**dissolution_raw)

    payload["dilutions"] = [CsvDilutionTarget(**item) for item in payload.get("dilutions", [])]
    return CsvPlateSeriesSpec(**payload)


@dataclass
class PlateProcessSpec:
    """Generic well-wise OT2 process composed from dissolve/transfer/dilution blocks."""

    host_alias: str = "ot2_training"
    password: str = "accelerate"
    simulation: bool = True
    sample_num: int = 96
    well_selection: List[str] = field(default_factory=list)

    pipette_name: str = "p300"
    tiprack_nickname: str = "tip_300_96_1"

    labware: List[LabwareSpec] = field(
        default_factory=lambda: [
            LabwareSpec(
                nickname="plate_96_1",
                loadname="corning_96_wellplate_360ul_flat",
                location="1",
                ot_default=True,
            ),
            LabwareSpec(
                nickname="plate_96_2",
                loadname="corning_96_wellplate_360ul_flat",
                location="2",
                ot_default=True,
            ),
            LabwareSpec(
                nickname="resovir",
                loadname="agilent_1_reservoir_290ml",
                location="3",
                ot_default=True,
            ),
            LabwareSpec(
                nickname="tip_300_96_1",
                loadname="opentrons_96_tiprack_300ul",
                location="4",
                ot_default=True,
            ),
        ]
    )
    instrument: InstrumentSpec = field(
        default_factory=lambda: InstrumentSpec(
            nickname="p300",
            instrument_name="p300_single_gen2",
            mount="left",
            ot_default=True,
        )
    )

    dissolve: Optional[DissolveSpec] = field(default_factory=DissolveSpec)
    transfer: Optional[TransferSpec] = field(default_factory=TransferSpec)
    prefill: Optional[DilutionSpec] = None
    transfer_2: Optional[TransferSpec] = None
    dilution: Optional[DilutionSpec] = None
    csv_plate_series: Optional[CsvPlateSeriesSpec] = None
    stages: List[Dict[str, Any]] = field(default_factory=list)
    _source_dir: str = field(default="", repr=False, compare=False)

    def validate(self) -> None:
        if self.sample_num < 1 or self.sample_num > 96:
            raise ValueError(f"sample_num must be 1..96 for 96-well layout, got {self.sample_num}")
        if self.well_selection:
            resolve_well_selection(self.sample_num, self.well_selection)
        if self.pipette_name != self.instrument.nickname:
            raise ValueError("pipette_name must match instrument.nickname in this compiler")
        if self.dissolve and self.dissolve.mix_repetitions < 0:
            raise ValueError("dissolve.mix_repetitions must be >= 0")
        if self.transfer and self.transfer.mix_after_dispense_repetitions < 0:
            raise ValueError("transfer.mix_after_dispense_repetitions must be >= 0")
        if self.transfer_2 and self.transfer_2.mix_after_dispense_repetitions < 0:
            raise ValueError("transfer_2.mix_after_dispense_repetitions must be >= 0")
        if self.prefill and self.prefill.mix_repetitions < 0:
            raise ValueError("prefill.mix_repetitions must be >= 0")
        if self.dilution and self.dilution.mix_repetitions < 0:
            raise ValueError("dilution.mix_repetitions must be >= 0")
        if self.csv_plate_series:
            dis = self.csv_plate_series.dissolution
            if dis.mix_repetitions < 0:
                raise ValueError("csv_plate_series.dissolution.mix_repetitions must be >= 0")
            for i, dil in enumerate(self.csv_plate_series.dilutions, start=1):
                if dil.mix_repetitions < 0:
                    raise ValueError(f"csv_plate_series.dilutions[{i}].mix_repetitions must be >= 0")
        for idx, stage in enumerate(self.stages):
            if not isinstance(stage, dict):
                raise ValueError(f"stages[{idx}] must be an object")
            kind = str(stage.get("kind", "")).strip().lower()
            if kind not in {"dissolve", "transfer", "dilution"}:
                raise ValueError(f"stages[{idx}].kind must be dissolve/transfer/dilution, got {kind!r}")
            payload = {k: v for k, v in stage.items() if k not in {"kind", "name"}}
            if kind == "dissolve":
                op = DissolveSpec(**payload)
                if op.mix_repetitions < 0:
                    raise ValueError(f"stages[{idx}].mix_repetitions must be >= 0")
            elif kind == "transfer":
                op = TransferSpec(**payload)
                if op.mix_after_dispense_repetitions < 0:
                    raise ValueError(f"stages[{idx}].mix_after_dispense_repetitions must be >= 0")
            else:
                op = DilutionSpec(**payload)
                if op.mix_repetitions < 0:
                    raise ValueError(f"stages[{idx}].mix_repetitions must be >= 0")
        if (
            not self.stages
            and self.csv_plate_series is None
            and self.prefill is None
            and self.dissolve is None
            and self.transfer is None
            and self.transfer_2 is None
            and self.dilution is None
        ):
            raise ValueError("At least one of csv_plate_series/stages/prefill/dissolve/transfer/transfer_2/dilution must be provided")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str) -> str:
        out = Path(path)
        out.write_text(json.dumps(self.to_dict(), indent=2))
        return str(out)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PlateProcessSpec":
        defaults = cls()
        return cls(
            host_alias=data.get("host_alias", defaults.host_alias),
            password=data.get("password", defaults.password),
            simulation=data.get("simulation", defaults.simulation),
            sample_num=data.get("sample_num", defaults.sample_num),
            well_selection=data.get("well_selection", []) or [],
            pipette_name=data.get("pipette_name", defaults.pipette_name),
            tiprack_nickname=data.get("tiprack_nickname", defaults.tiprack_nickname),
            labware=[LabwareSpec(**item) for item in data.get("labware", [])] or defaults.labware,
            instrument=InstrumentSpec(**data.get("instrument", {})) if data.get("instrument") else defaults.instrument,
            dissolve=DissolveSpec(**data["dissolve"]) if data.get("dissolve") is not None else None,
            transfer=TransferSpec(**data["transfer"]) if data.get("transfer") is not None else None,
            prefill=DilutionSpec(**data["prefill"]) if data.get("prefill") is not None else None,
            transfer_2=TransferSpec(**data["transfer_2"]) if data.get("transfer_2") is not None else None,
            dilution=DilutionSpec(**data["dilution"]) if data.get("dilution") is not None else None,
            csv_plate_series=_parse_csv_plate_series(data["csv_plate_series"]) if data.get("csv_plate_series") is not None else None,
            stages=data.get("stages", []) or [],
        )

    @classmethod
    def from_json(cls, path: str) -> "PlateProcessSpec":
        src = Path(path)
        spec = cls.from_dict(json.loads(src.read_text()))
        spec._source_dir = str(src.parent)
        return spec


def _set_location(
    steps: List[WorkflowStep],
    name: str,
    labware_nickname: str,
    position: str,
    top: Optional[float] = None,
    bottom: Optional[float] = None,
) -> None:
    kwargs: Dict[str, Any] = {"labware_nickname": labware_nickname, "position": position}
    if top is not None:
        kwargs["top"] = top
    if bottom is not None:
        kwargs["bottom"] = bottom
    steps.append(WorkflowStep("get_location_from_labware", kwargs, name=name, capture_result=False))


def _delay_step(seconds: float, name: str) -> WorkflowStep:
    return WorkflowStep("delay", {"seconds": seconds}, name=name, capture_result=False)


def _resolve_path(base_dir: Optional[Path], token: str) -> Path:
    path = Path(token)
    if not path.is_absolute() and base_dir is not None:
        path = base_dir / path
    return path


def _load_volume_matrix(path: Path) -> Dict[str, float]:
    # Fixed format: 8 rows (A-H) x 12 columns (1-12)
    rows = [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    matrix = [[float(cell.strip()) for cell in row.split(",")] for row in rows]

    if len(matrix) != 8 or any(len(row) != 12 for row in matrix):
        raise ValueError(f"{path}: expected 8 rows x 12 columns")

    well_volumes: Dict[str, float] = {}
    for r in range(8):
        for c in range(12):
            well_volumes[_well_from_row_col(r, c)] = matrix[r][c]
    return well_volumes


def _compile_dissolve_for_well(
    steps: List[WorkflowStep],
    well: str,
    pipette_name: str,
    op: DissolveSpec,
    name_prefix: str = "dissolve",
) -> None:
    _set_location(
        steps,
        name=f"{well}_{name_prefix}_source_loc",
        labware_nickname=op.source_labware,
        position=op.source_well,
        bottom=op.source_bottom,
    )
    steps.append(
        WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_{name_prefix}_move_source", capture_result=False)
    )
    steps.append(
        WorkflowStep(
            "aspirate",
            {"pip_name": pipette_name, "volume": op.add_volume_ul},
            name=f"{well}_{name_prefix}_asp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_delay_a"))

    _set_location(
        steps,
        name=f"{well}_{name_prefix}_target_loc",
        labware_nickname=op.target_labware,
        position=well,
        bottom=op.target_bottom_dispense,
    )
    steps.append(
        WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_{name_prefix}_move_target", capture_result=False)
    )
    steps.append(
        WorkflowStep(
            "dispense",
            {"pip_name": pipette_name, "volume": op.add_volume_ul},
            name=f"{well}_{name_prefix}_disp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_delay_b"))

    for mix_idx in range(op.mix_repetitions):
        steps.append(
            WorkflowStep(
                "aspirate",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_{name_prefix}_mix{mix_idx+1}_asp",
                capture_result=False,
            )
        )
        if op.delay_seconds:
            steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_mix{mix_idx+1}_delay_a"))
        steps.append(
            WorkflowStep(
                "dispense",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_{name_prefix}_mix{mix_idx+1}_disp",
                capture_result=False,
            )
        )
        if op.delay_seconds:
            steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_mix{mix_idx+1}_delay_b"))


def _compile_transfer_for_well(
    steps: List[WorkflowStep],
    well: str,
    pipette_name: str,
    op: TransferSpec,
    name_prefix: str = "transfer",
) -> None:
    _set_location(
        steps,
        name=f"{well}_{name_prefix}_source_loc",
        labware_nickname=op.source_labware,
        position=well,
        bottom=op.source_bottom,
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_delay_a"))
    steps.append(
        WorkflowStep(
            "aspirate",
            {"pip_name": pipette_name, "volume": op.aspirate_volume_ul},
            name=f"{well}_{name_prefix}_asp",
            capture_result=False,
        )
    )
    if op.source_lift_top is not None:
        _set_location(
            steps,
            name=f"{well}_{name_prefix}_source_lift",
            labware_nickname=op.source_labware,
            position=well,
            top=op.source_lift_top,
        )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_delay_b"))

    if op.destination_bottom is not None:
        _set_location(
            steps,
            name=f"{well}_{name_prefix}_dest_loc",
            labware_nickname=op.destination_labware,
            position=well,
            bottom=op.destination_bottom,
        )
    else:
        _set_location(
            steps,
            name=f"{well}_{name_prefix}_dest_loc",
            labware_nickname=op.destination_labware,
            position=well,
            top=op.destination_top if op.destination_top is not None else 0,
        )
    steps.append(
        WorkflowStep(
            "dispense",
            {"pip_name": pipette_name, "volume": op.dispense_volume_ul or op.aspirate_volume_ul},
            name=f"{well}_{name_prefix}_disp",
            capture_result=False,
        )
    )
    mix_volume = op.mix_after_dispense_volume_ul or op.dispense_volume_ul or op.aspirate_volume_ul
    for mix_idx in range(op.mix_after_dispense_repetitions):
        steps.append(
            WorkflowStep(
                "aspirate",
                {"pip_name": pipette_name, "volume": mix_volume},
                name=f"{well}_{name_prefix}_mix{mix_idx+1}_asp",
                capture_result=False,
            )
        )
        if op.delay_seconds:
            steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_mix{mix_idx+1}_delay_a"))
        steps.append(
            WorkflowStep(
                "dispense",
                {"pip_name": pipette_name, "volume": mix_volume},
                name=f"{well}_{name_prefix}_mix{mix_idx+1}_disp",
                capture_result=False,
            )
        )
        if op.delay_seconds:
            steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_mix{mix_idx+1}_delay_b"))
    if op.blow_out:
        steps.append(
            WorkflowStep("blow_out", {"pip_name": pipette_name}, name=f"{well}_{name_prefix}_blow_out", capture_result=False)
        )


def _compile_dilution_for_well(
    steps: List[WorkflowStep],
    well: str,
    pipette_name: str,
    op: DilutionSpec,
    name_prefix: str = "dilution",
) -> None:
    _set_location(
        steps,
        name=f"{well}_{name_prefix}_source_loc",
        labware_nickname=op.diluent_labware,
        position=op.diluent_well,
        bottom=op.diluent_bottom,
    )
    steps.append(
        WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_{name_prefix}_move_source", capture_result=False)
    )
    steps.append(
        WorkflowStep(
            "aspirate",
            {"pip_name": pipette_name, "volume": op.diluent_volume_ul},
            name=f"{well}_{name_prefix}_asp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_delay_a"))
    _set_location(
        steps,
        name=f"{well}_{name_prefix}_target_loc",
        labware_nickname=op.target_labware,
        position=well,
        bottom=op.target_bottom_dispense,
    )
    steps.append(
        WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_{name_prefix}_move_target", capture_result=False)
    )
    steps.append(
        WorkflowStep(
            "dispense",
            {"pip_name": pipette_name, "volume": op.diluent_volume_ul},
            name=f"{well}_{name_prefix}_disp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_{name_prefix}_delay_b"))
    for mix_idx in range(op.mix_repetitions):
        steps.append(
            WorkflowStep(
                "aspirate",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_{name_prefix}_mix{mix_idx+1}_asp",
                capture_result=False,
            )
        )
        steps.append(
            WorkflowStep(
                "dispense",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_{name_prefix}_mix{mix_idx+1}_disp",
                capture_result=False,
            )
        )


def compile_plate_process_steps(spec: PlateProcessSpec) -> List[WorkflowStep]:
    spec.validate()
    steps: List[WorkflowStep] = []
    base_dir = Path(spec._source_dir) if spec._source_dir else None

    for lw in spec.labware:
        steps.append(
            WorkflowStep(
                "load_labware",
                {"labware": lw.to_runtime_dict(base_dir=base_dir)},
                name=f"load_{lw.nickname}",
                capture_result=False,
            )
        )
    steps.append(
        WorkflowStep(
            "load_instrument",
            {"instrument": spec.instrument.to_runtime_dict()},
            name=f"load_{spec.instrument.nickname}",
            capture_result=False,
        )
    )

    wells = resolve_well_selection(spec.sample_num, spec.well_selection)

    if spec.csv_plate_series is not None:
        op = spec.csv_plate_series
        dissolution_cfg = op.dissolution
        dissolution_plate = dissolution_cfg.plate
        dissolution_by_well = _load_volume_matrix(_resolve_path(base_dir, f"{dissolution_plate}.csv"))
        dilution_steps = op.dilutions
        dilution_by_well = [_load_volume_matrix(_resolve_path(base_dir, f"{item.plate}.csv")) for item in dilution_steps]

        for well in wells:
            _set_location(
                steps,
                name=f"{well}_tip_loc",
                labware_nickname=spec.tiprack_nickname,
                position=well,
                top=0,
            )
            steps.append(
                WorkflowStep("pick_up_tip", {"pip_name": spec.pipette_name}, name=f"{well}_pickup_tip", capture_result=False)
            )

            # Prefill all downstream plates first for this well-series before any dissolution/transfer.
            for idx, dilutions in enumerate(dilution_by_well, start=1):
                dilution_volume = dilutions[well]
                if dilution_volume <= 0:
                    continue

                target = dilution_steps[idx - 1]
                destination_plate = target.plate
                prefill_volume = 300.0 - dilution_volume
                if target.prefill and prefill_volume > 0:
                    _set_location(
                        steps,
                        name=f"{well}_prefill{idx}_source_loc",
                        labware_nickname=op.solvent_labware,
                        position=op.solvent_well,
                        bottom=op.solvent_bottom,
                    )
                    steps.append(
                        WorkflowStep("move_to_pip", {"pip_name": spec.pipette_name}, name=f"{well}_prefill{idx}_move_source", capture_result=False)
                    )
                    steps.append(
                        WorkflowStep(
                            "aspirate",
                            {"pip_name": spec.pipette_name, "volume": prefill_volume},
                            name=f"{well}_prefill{idx}_asp",
                            capture_result=False,
                        )
                    )
                    if target.delay_seconds:
                        steps.append(_delay_step(target.delay_seconds, f"{well}_prefill{idx}_delay_a"))
                    _set_location(
                        steps,
                        name=f"{well}_prefill{idx}_target_loc",
                        labware_nickname=destination_plate,
                        position=well,
                        bottom=op.transfer_destination_bottom,
                    )
                    steps.append(
                        WorkflowStep("move_to_pip", {"pip_name": spec.pipette_name}, name=f"{well}_prefill{idx}_move_target", capture_result=False)
                    )
                    steps.append(
                        WorkflowStep(
                            "dispense",
                            {"pip_name": spec.pipette_name, "volume": prefill_volume},
                            name=f"{well}_prefill{idx}_disp",
                            capture_result=False,
                        )
                    )
                    if target.delay_seconds:
                        steps.append(_delay_step(target.delay_seconds, f"{well}_prefill{idx}_delay_b"))

            dissolution_volume = dissolution_by_well[well]
            if dissolution_volume > 0:
                _set_location(
                    steps,
                    name=f"{well}_dissolve_source_loc",
                    labware_nickname=op.solvent_labware,
                    position=op.solvent_well,
                    bottom=op.solvent_bottom,
                )
                steps.append(
                    WorkflowStep("move_to_pip", {"pip_name": spec.pipette_name}, name=f"{well}_dissolve_move_source", capture_result=False)
                )
                steps.append(
                    WorkflowStep(
                        "aspirate",
                        {"pip_name": spec.pipette_name, "volume": dissolution_volume},
                        name=f"{well}_dissolve_asp",
                        capture_result=False,
                    )
                )
                if dissolution_cfg.delay_seconds:
                    steps.append(_delay_step(dissolution_cfg.delay_seconds, f"{well}_dissolve_delay_a"))
                _set_location(
                    steps,
                    name=f"{well}_dissolve_target_loc",
                    labware_nickname=dissolution_plate,
                    position=well,
                    bottom=op.dissolution_target_bottom,
                )
                steps.append(
                    WorkflowStep("move_to_pip", {"pip_name": spec.pipette_name}, name=f"{well}_dissolve_move_target", capture_result=False)
                )
                steps.append(
                    WorkflowStep(
                        "dispense",
                        {"pip_name": spec.pipette_name, "volume": dissolution_volume},
                        name=f"{well}_dissolve_disp",
                        capture_result=False,
                    )
                )
                if dissolution_cfg.delay_seconds:
                    steps.append(_delay_step(dissolution_cfg.delay_seconds, f"{well}_dissolve_delay_b"))
                for mix_idx in range(dissolution_cfg.mix_repetitions):
                    steps.append(
                        WorkflowStep(
                            "aspirate",
                            {"pip_name": spec.pipette_name, "volume": dissolution_cfg.mix_volume_ul},
                            name=f"{well}_dissolve_mix{mix_idx+1}_asp",
                            capture_result=False,
                        )
                    )
                    if dissolution_cfg.delay_seconds:
                        steps.append(_delay_step(dissolution_cfg.delay_seconds, f"{well}_dissolve_mix{mix_idx+1}_delay_a"))
                    steps.append(
                        WorkflowStep(
                            "dispense",
                            {"pip_name": spec.pipette_name, "volume": dissolution_cfg.mix_volume_ul},
                            name=f"{well}_dissolve_mix{mix_idx+1}_disp",
                            capture_result=False,
                        )
                    )
                    if dissolution_cfg.delay_seconds:
                        steps.append(_delay_step(dissolution_cfg.delay_seconds, f"{well}_dissolve_mix{mix_idx+1}_delay_b"))

            if dissolution_cfg.blow_out:
                steps.append(
                    WorkflowStep(
                        "blow_out",
                        {"pip_name": spec.pipette_name},
                        name=f"{well}_dissolve_blow_out",
                        capture_result=False,
                    )
                )

            source_plate = dissolution_plate
            for idx, dilutions in enumerate(dilution_by_well, start=1):
                target = dilution_steps[idx - 1]
                destination_plate = target.plate
                dilution_volume = dilutions[well]
                if dilution_volume > 0:
                    _set_location(
                        steps,
                        name=f"{well}_transfer{idx}_source_loc",
                        labware_nickname=source_plate,
                        position=well,
                        bottom=op.transfer_source_bottom,
                    )
                    if target.delay_seconds:
                        steps.append(_delay_step(target.delay_seconds, f"{well}_transfer{idx}_delay_a"))
                    steps.append(
                        WorkflowStep(
                            "aspirate",
                            {"pip_name": spec.pipette_name, "volume": dilution_volume},
                            name=f"{well}_transfer{idx}_asp",
                            capture_result=False,
                        )
                    )
                    if op.transfer_source_lift_top is not None:
                        _set_location(
                            steps,
                            name=f"{well}_transfer{idx}_source_lift",
                            labware_nickname=source_plate,
                            position=well,
                            top=op.transfer_source_lift_top,
                        )
                    if target.delay_seconds:
                        steps.append(_delay_step(target.delay_seconds, f"{well}_transfer{idx}_delay_b"))
                    _set_location(
                        steps,
                        name=f"{well}_transfer{idx}_dest_loc",
                        labware_nickname=destination_plate,
                        position=well,
                        bottom=op.transfer_destination_bottom,
                    )
                    steps.append(
                        WorkflowStep(
                            "dispense",
                            {"pip_name": spec.pipette_name, "volume": dilution_volume},
                            name=f"{well}_transfer{idx}_disp",
                            capture_result=False,
                        )
                    )
                    for mix_idx in range(target.mix_repetitions):
                        steps.append(
                            WorkflowStep(
                                "aspirate",
                                {"pip_name": spec.pipette_name, "volume": target.mix_volume_ul},
                                name=f"{well}_transfer{idx}_mix{mix_idx+1}_asp",
                                capture_result=False,
                            )
                        )
                        if target.delay_seconds:
                            steps.append(_delay_step(target.delay_seconds, f"{well}_transfer{idx}_mix{mix_idx+1}_delay_a"))
                        steps.append(
                            WorkflowStep(
                                "dispense",
                                {"pip_name": spec.pipette_name, "volume": target.mix_volume_ul},
                                name=f"{well}_transfer{idx}_mix{mix_idx+1}_disp",
                                capture_result=False,
                            )
                        )
                        if target.delay_seconds:
                            steps.append(_delay_step(target.delay_seconds, f"{well}_transfer{idx}_mix{mix_idx+1}_delay_b"))
                    if target.blow_out:
                        steps.append(
                            WorkflowStep(
                                "blow_out",
                                {"pip_name": spec.pipette_name},
                                name=f"{well}_transfer{idx}_blow_out",
                                capture_result=False,
                            )
                        )

                source_plate = destination_plate

            steps.append(
                WorkflowStep("drop_tip", {"pip_name": spec.pipette_name}, name=f"{well}_drop_tip", capture_result=False)
            )

        return steps

    for well in wells:
        _set_location(
            steps,
            name=f"{well}_tip_loc",
            labware_nickname=spec.tiprack_nickname,
            position=well,
            top=0,
        )
        steps.append(
            WorkflowStep("pick_up_tip", {"pip_name": spec.pipette_name}, name=f"{well}_pickup_tip", capture_result=False)
        )

        if spec.stages:
            prefix_counts: Dict[str, int] = {}
            for stage_idx, stage in enumerate(spec.stages, start=1):
                kind = str(stage.get("kind", "")).strip().lower()
                payload = {k: v for k, v in stage.items() if k not in {"kind", "name"}}
                if "name" in stage:
                    base_prefix = _slug_token(stage["name"])
                else:
                    base_prefix = f"{kind}{stage_idx}"
                prefix_counts[base_prefix] = prefix_counts.get(base_prefix, 0) + 1
                prefix = base_prefix if prefix_counts[base_prefix] == 1 else f"{base_prefix}_{prefix_counts[base_prefix]}"

                if kind == "dissolve":
                    _compile_dissolve_for_well(
                        steps, well, spec.pipette_name, DissolveSpec(**payload), name_prefix=prefix
                    )
                elif kind == "transfer":
                    _compile_transfer_for_well(
                        steps, well, spec.pipette_name, TransferSpec(**payload), name_prefix=prefix
                    )
                elif kind == "dilution":
                    _compile_dilution_for_well(
                        steps, well, spec.pipette_name, DilutionSpec(**payload), name_prefix=prefix
                    )
                else:
                    raise ValueError(f"Unsupported stage kind: {kind!r}")
        else:
            if spec.prefill is not None:
                _compile_dilution_for_well(steps, well, spec.pipette_name, spec.prefill, name_prefix="prefill")
            if spec.dissolve is not None:
                _compile_dissolve_for_well(steps, well, spec.pipette_name, spec.dissolve)
            if spec.transfer is not None:
                _compile_transfer_for_well(steps, well, spec.pipette_name, spec.transfer)
            if spec.transfer_2 is not None:
                _compile_transfer_for_well(steps, well, spec.pipette_name, spec.transfer_2, name_prefix="transfer2")
            if spec.dilution is not None:
                _compile_dilution_for_well(steps, well, spec.pipette_name, spec.dilution)

        steps.append(
            WorkflowStep("drop_tip", {"pip_name": spec.pipette_name}, name=f"{well}_drop_tip", capture_result=False)
        )

    return steps


# Backward-compatible names for existing demos/imports.
Ot2OpticHteDilutionSpec = PlateProcessSpec


def compile_ot2_optic_hte_dilution_steps(spec: PlateProcessSpec) -> List[WorkflowStep]:
    return compile_plate_process_steps(spec)
