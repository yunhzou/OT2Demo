import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .workflow import WorkflowStep


def _well_name(i: int) -> str:
    return f"{chr(65 + i // 12)}{i % 12 + 1}"


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

    def to_runtime_dict(self, base_dir: Optional[Path] = None) -> Dict[str, Any]:
        resolved_config = self.config
        if not self.ot_default and not resolved_config and self.config_path:
            cfg_path = Path(self.config_path)
            if not cfg_path.is_absolute() and base_dir is not None:
                cfg_path = base_dir / cfg_path
            resolved_config = json.loads(cfg_path.read_text())

        data = {
            "nickname": self.nickname,
            "location": self.location,
            "ot_default": self.ot_default,
            "config": resolved_config,
        }
        if self.ot_default:
            data["loadname"] = self.loadname
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
    dilution: Optional[DilutionSpec] = None
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
        if self.dilution and self.dilution.mix_repetitions < 0:
            raise ValueError("dilution.mix_repetitions must be >= 0")
        if self.dissolve is None and self.transfer is None and self.dilution is None:
            raise ValueError("At least one of dissolve/transfer/dilution must be provided")

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
            dilution=DilutionSpec(**data["dilution"]) if data.get("dilution") is not None else None,
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


def _compile_dissolve_for_well(
    steps: List[WorkflowStep],
    well: str,
    pipette_name: str,
    op: DissolveSpec,
) -> None:
    _set_location(
        steps,
        name=f"{well}_dissolve_source_loc",
        labware_nickname=op.source_labware,
        position=op.source_well,
        bottom=op.source_bottom,
    )
    steps.append(WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_dissolve_move_source", capture_result=False))
    steps.append(
        WorkflowStep(
            "aspirate",
            {"pip_name": pipette_name, "volume": op.add_volume_ul},
            name=f"{well}_dissolve_asp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_dissolve_delay_a"))

    _set_location(
        steps,
        name=f"{well}_dissolve_target_loc",
        labware_nickname=op.target_labware,
        position=well,
        bottom=op.target_bottom_dispense,
    )
    steps.append(WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_dissolve_move_target", capture_result=False))
    steps.append(
        WorkflowStep(
            "dispense",
            {"pip_name": pipette_name, "volume": op.add_volume_ul},
            name=f"{well}_dissolve_disp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_dissolve_delay_b"))

    for mix_idx in range(op.mix_repetitions):
        steps.append(
            WorkflowStep(
                "aspirate",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_dissolve_mix{mix_idx+1}_asp",
                capture_result=False,
            )
        )
        if op.delay_seconds:
            steps.append(_delay_step(op.delay_seconds, f"{well}_dissolve_mix{mix_idx+1}_delay_a"))
        steps.append(
            WorkflowStep(
                "dispense",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_dissolve_mix{mix_idx+1}_disp",
                capture_result=False,
            )
        )
        if op.delay_seconds:
            steps.append(_delay_step(op.delay_seconds, f"{well}_dissolve_mix{mix_idx+1}_delay_b"))


def _compile_transfer_for_well(
    steps: List[WorkflowStep],
    well: str,
    pipette_name: str,
    op: TransferSpec,
) -> None:
    _set_location(
        steps,
        name=f"{well}_transfer_source_loc",
        labware_nickname=op.source_labware,
        position=well,
        bottom=op.source_bottom,
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_transfer_delay_a"))
    steps.append(
        WorkflowStep(
            "aspirate",
            {"pip_name": pipette_name, "volume": op.aspirate_volume_ul},
            name=f"{well}_transfer_asp",
            capture_result=False,
        )
    )
    if op.source_lift_top is not None:
        _set_location(
            steps,
            name=f"{well}_transfer_source_lift",
            labware_nickname=op.source_labware,
            position=well,
            top=op.source_lift_top,
        )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_transfer_delay_b"))

    if op.destination_bottom is not None:
        _set_location(
            steps,
            name=f"{well}_transfer_dest_loc",
            labware_nickname=op.destination_labware,
            position=well,
            bottom=op.destination_bottom,
        )
    else:
        _set_location(
            steps,
            name=f"{well}_transfer_dest_loc",
            labware_nickname=op.destination_labware,
            position=well,
            top=op.destination_top if op.destination_top is not None else 0,
        )
    steps.append(
        WorkflowStep(
            "dispense",
            {"pip_name": pipette_name, "volume": op.dispense_volume_ul or op.aspirate_volume_ul},
            name=f"{well}_transfer_disp",
            capture_result=False,
        )
    )
    if op.blow_out:
        steps.append(WorkflowStep("blow_out", {"pip_name": pipette_name}, name=f"{well}_transfer_blow_out", capture_result=False))


def _compile_dilution_for_well(
    steps: List[WorkflowStep],
    well: str,
    pipette_name: str,
    op: DilutionSpec,
) -> None:
    _set_location(
        steps,
        name=f"{well}_dilution_source_loc",
        labware_nickname=op.diluent_labware,
        position=op.diluent_well,
        bottom=op.diluent_bottom,
    )
    steps.append(WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_dilution_move_source", capture_result=False))
    steps.append(
        WorkflowStep(
            "aspirate",
            {"pip_name": pipette_name, "volume": op.diluent_volume_ul},
            name=f"{well}_dilution_asp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_dilution_delay_a"))
    _set_location(
        steps,
        name=f"{well}_dilution_target_loc",
        labware_nickname=op.target_labware,
        position=well,
        bottom=op.target_bottom_dispense,
    )
    steps.append(WorkflowStep("move_to_pip", {"pip_name": pipette_name}, name=f"{well}_dilution_move_target", capture_result=False))
    steps.append(
        WorkflowStep(
            "dispense",
            {"pip_name": pipette_name, "volume": op.diluent_volume_ul},
            name=f"{well}_dilution_disp",
            capture_result=False,
        )
    )
    if op.delay_seconds:
        steps.append(_delay_step(op.delay_seconds, f"{well}_dilution_delay_b"))
    for mix_idx in range(op.mix_repetitions):
        steps.append(
            WorkflowStep(
                "aspirate",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_dilution_mix{mix_idx+1}_asp",
                capture_result=False,
            )
        )
        steps.append(
            WorkflowStep(
                "dispense",
                {"pip_name": pipette_name, "volume": op.mix_volume_ul},
                name=f"{well}_dilution_mix{mix_idx+1}_disp",
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

        if spec.dissolve is not None:
            _compile_dissolve_for_well(steps, well, spec.pipette_name, spec.dissolve)
        if spec.transfer is not None:
            _compile_transfer_for_well(steps, well, spec.pipette_name, spec.transfer)
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
