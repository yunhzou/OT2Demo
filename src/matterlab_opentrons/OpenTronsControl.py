import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect import flow

from .sshclient import SSHClient


PROTOCOL_API_VERSION = "2.21"


class OpenTrons:
    """Thin SSH-backed wrapper for streaming Opentrons Python commands."""

    def __init__(self, host_alias: Optional[str] = None, password: str = "", simulation: bool = False):
        self._labware_meta: Dict[str, Dict[str, Any]] = {}
        self._current_location_context: Dict[str, Optional[str]] = {"labware_nickname": None, "position": None}
        self._tip_trackers: Dict[str, Path] = {}
        self._mounted_tips: Dict[str, Dict[str, Optional[str]]] = {}
        self._connect(host_alias, password)
        self._get_protocol(simulation)

    def _connect(self, host_alias: Optional[str] = None, password: str = ""):
        self.client = SSHClient(
            hostname=os.getenv("HOSTNAME"),
            username=os.getenv("USERNAME"),
            key_file_path=os.getenv("KEY_FILE_PATH"),
            host_alias=host_alias,
            password=password,
        )
        self.client.connect()

    def invoke(self, code):
        return self.client.invoke(code)

    def _disconnect(self):
        self.client.close()

    def _get_protocol(self, simulation: bool):
        self.invoke("from opentrons.types import Point, Location")
        self.invoke("from opentrons import protocol_api")
        self.invoke("import json")
        if simulation:
            self.invoke("from opentrons import simulate")
            self.invoke(f"protocol = simulate.get_protocol_api('{PROTOCOL_API_VERSION}')")
        else:
            self.invoke("from opentrons import execute")
            self.invoke(f"protocol = execute.get_protocol_api('{PROTOCOL_API_VERSION}')")

    def _invoke_lines(self, code: str):
        return self.invoke(code).split("\r\n")

    def _invoke_scalar_line(self, code: str) -> str:
        return self._invoke_lines(code)[-2].strip()

    def _invoke_float(self, code: str) -> float:
        return float(self._invoke_scalar_line(code))

    def _invoke_bool(self, code: str) -> bool:
        value = self._invoke_scalar_line(code)
        if value == "True":
            return True
        if value == "False":
            return False
        raise ValueError(f"Expected bool-like SSH response, got: {value!r}")

    def _py_repr(self, value):
        return "None" if value is None else repr(value)

    def _format_kwargs(self, **kwargs) -> str:
        parts = [f"{key} = {self._py_repr(value)}" for key, value in kwargs.items() if value is not None]
        return ", ".join(parts)

    def _init_low_level_hardware(self) -> None:
        """Initialize low-level hardware/gripper handles in the remote interpreter."""
        self.invoke("hardware = protocol._core_get_hardware()")
        self.invoke("hardware.cache_instruments()")
        self.invoke("from opentrons import types as _ot_types")
        self.invoke("gripper_mount = getattr(getattr(_ot_types, 'OT3Mount', object), 'GRIPPER', None)")
        self.invoke("gripper_mount = gripper_mount or getattr(getattr(_ot_types, 'Mount', object), 'GRIPPER', None)")
        self.invoke("gripper = hardware._gripper_handler.get_gripper() if hardware.has_gripper() else None")

    @flow
    def _load_custom_labware(self, nickname: str, labware_config: Dict, location: str):
        loadname = labware_config["parameters"]["loadName"]
        self.invoke(f"{loadname}={labware_config}")
        self.invoke(
            f"{nickname} = protocol.load_labware_from_definition("
            f"labware_def = {loadname}, location = '{location}')"
        )

    @flow
    def _load_default_labware(self, nickname: str, loadname: str, location: str):
        self.invoke(f"{nickname} = protocol.load_labware(load_name = '{loadname}', location = '{location}')")

    @flow
    def _load_default_instrument(self, nickname: str, instrument_name: str, mount: str):
        self.invoke(f"{nickname} = protocol.load_instrument(instrument_name = '{instrument_name}', mount = '{mount}')")

    @flow
    def _load_custom_instrument(self, nickname: str, instrument_config: Dict, mount: str):
        raise NotImplementedError("custom instrument not implemented")

    def _setup_device_metadata(self):
        self.invoke("p300.well_bottom_clearance.dispense=10")

    def _is_tiprack_labware(self, labware: Dict[str, Any]) -> bool:
        config = labware.get("config") or {}
        if isinstance(config, dict):
            params = config.get("parameters") or {}
            if isinstance(params, dict) and params.get("isTiprack"):
                return True
        loadname = str(labware.get("loadname") or "").lower()
        return "tiprack" in loadname

    def _has_tip_status_payload(self, labware: Dict[str, Any]) -> bool:
        if "tip_status" in labware:
            return True
        config = labware.get("config")
        return isinstance(config, dict) and "tip_status" in config

    def _resolve_labware_input(self, labware: Union[Dict[str, Any], str, Path]) -> Tuple[Dict[str, Any], Optional[Path]]:
        if isinstance(labware, dict):
            return dict(labware), None
        if isinstance(labware, (str, Path)):
            src = Path(labware)
            if not src.is_absolute():
                src = Path.cwd() / src
            payload = json.loads(src.read_text())
            if not isinstance(payload, dict):
                raise ValueError(f"Labware JSON must contain an object: {src}")
            return payload, src
        raise TypeError(f"Unsupported labware input type: {type(labware)!r}")

    def _register_tip_tracker(self, tiprack_nickname: str, status_file: str) -> None:
        path = Path(status_file)
        if not path.is_absolute():
            path = Path.cwd() / path
        self._tip_trackers[tiprack_nickname] = path
        if not path.exists() or path.stat().st_size == 0:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"tip_status": {}}, indent=2))

    def _read_tip_status_doc(self, tiprack_nickname: str) -> Dict[str, Any]:
        path = self._tip_trackers[tiprack_nickname]
        if not path.exists() or path.stat().st_size == 0:
            return {"tip_status": {}}
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            raise ValueError(f"Tip status file must contain a JSON object: {path}")
        return raw

    def _well_to_row_col(self, well: str) -> Optional[tuple]:
        token = str(well).strip().upper()
        if len(token) < 2 or not token[0].isalpha():
            return None
        row = ord(token[0]) - ord("A")
        try:
            col = int(token[1:]) - 1
        except ValueError:
            return None
        if row < 0 or col < 0:
            return None
        return row, col

    def _well_sort_key(self, well: str) -> Tuple[int, int]:
        rc = self._well_to_row_col(well)
        if rc is None:
            return (10_000, 10_000)
        return rc

    def _status_obj_to_map(self, status_obj: Any) -> Dict[str, Any]:
        if status_obj is None:
            return {}
        if isinstance(status_obj, dict):
            return status_obj
        if isinstance(status_obj, list):
            out: Dict[str, Any] = {}
            for r, row_vals in enumerate(status_obj):
                if not isinstance(row_vals, list):
                    continue
                for c, value in enumerate(row_vals):
                    out[f"{chr(ord('A') + r)}{c + 1}"] = value
            return out
        raise ValueError("tip_status must be a JSON object {well: status} or 2D list [rows][cols]")

    def _write_status_obj_like(self, status_obj: Any, well: str, value: str) -> Any:
        rc = self._well_to_row_col(well)
        if isinstance(status_obj, list) and rc:
            row_idx, col_idx = rc
            while len(status_obj) <= row_idx:
                status_obj.append([])
            while len(status_obj[row_idx]) <= col_idx:
                status_obj[row_idx].append("new")
            status_obj[row_idx][col_idx] = value
            return status_obj
        if isinstance(status_obj, dict):
            status_obj[well] = value
            return status_obj
        out = self._status_obj_to_map(status_obj)
        out[well] = value
        return out

    def _get_tip_status_map(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        if "tip_status" in doc:
            return self._status_obj_to_map(doc.get("tip_status"))
        config = doc.get("config")
        if isinstance(config, dict) and "tip_status" in config:
            return self._status_obj_to_map(config.get("tip_status"))
        return {}

    def _list_tracked_tip_wells(self, tiprack_nickname: str) -> List[str]:
        doc = self._read_tip_status_doc(tiprack_nickname)
        status_obj: Any = None
        if "tip_status" in doc:
            status_obj = doc.get("tip_status")
        elif isinstance(doc.get("config"), dict):
            status_obj = doc["config"].get("tip_status")

        if isinstance(status_obj, list):
            row_count = len(status_obj)
            col_count = max((len(r) for r in status_obj if isinstance(r, list)), default=0)
            if row_count and col_count:
                return [f"{chr(ord('A') + r)}{c + 1}" for c in range(col_count) for r in range(row_count)]

        status_map = self._status_obj_to_map(status_obj)
        if status_map:
            return sorted(status_map.keys(), key=self._well_sort_key)

        # Default 96-well ordering (column-major) used by standard Opentrons tipracks.
        return [f"{chr(ord('A') + r)}{c + 1}" for c in range(12) for r in range(8)]

    def _write_tip_status(self, tiprack_nickname: str, well: str, status: str) -> None:
        doc = self._read_tip_status_doc(tiprack_nickname)
        if "tip_status" in doc:
            doc["tip_status"] = self._write_status_obj_like(doc.get("tip_status"), well, status)
        elif isinstance(doc.get("config"), dict) and "tip_status" in doc["config"]:
            doc["config"]["tip_status"] = self._write_status_obj_like(doc["config"].get("tip_status"), well, status)
        else:
            existing = doc.get("tip_status", {})
            doc["tip_status"] = self._write_status_obj_like(existing, well, status)
        path = self._tip_trackers[tiprack_nickname]
        path.write_text(json.dumps(doc, indent=2, sort_keys=True))

    def _get_tip_status(self, tiprack_nickname: str, well: str) -> Optional[str]:
        doc = self._read_tip_status_doc(tiprack_nickname)
        status_map = self._get_tip_status_map(doc)
        status = status_map.get(well)
        if status is None:
            return None
        return str(status)

    def _is_tiprack(self, labware_nickname: Optional[str]) -> bool:
        if not labware_nickname:
            return False
        return bool((self._labware_meta.get(labware_nickname) or {}).get("is_tiprack"))

    def _validate_tip_pick(
        self,
        tiprack_nickname: str,
        well: str,
        sample_id: Optional[str] = None,
        force_pick: bool = False,
    ) -> Optional[str]:
        if tiprack_nickname not in self._tip_trackers:
            return None

        status = self._get_tip_status(tiprack_nickname, well)
        normalized = (status or "").strip().lower()
        if normalized in {"", "new", "unused", "clean", "available"}:
            return None
        if normalized == "empty":
            raise ValueError(f"Tip not available: {tiprack_nickname} {well} is empty.")
        if sample_id and status == sample_id:
            return status
        if force_pick:
            return status
        if sample_id:
            raise ValueError(
                f"Tip not available: {tiprack_nickname} {well} touched {status!r}, "
                f"requested {sample_id!r}. Use force_pick=True to override."
            )
        raise ValueError(
            f"Tip not available: {tiprack_nickname} {well} touched {status!r}. "
            "Provide sample_id matching that status or set force_pick=True."
        )

    def _mark_tip_touched_from_current_location(self, pip_name: str) -> None:
        mounted = self._mounted_tips.get(pip_name)
        if not mounted:
            return
        labware_nickname = self._current_location_context.get("labware_nickname")
        position = self._current_location_context.get("position")
        if not labware_nickname or not position:
            return
        if self._is_tiprack(labware_nickname):
            return
        sample_id = f"{labware_nickname}_{position}"
        mounted["last_sample"] = sample_id
        tiprack = mounted.get("tiprack_nickname")
        well = mounted.get("well")
        if tiprack and well and tiprack in self._tip_trackers:
            self._write_tip_status(tiprack, well, sample_id)

    def load_labware(self, labware: Union[Dict[str, Any], str, Path]):
        # sample labware Dict
        # lw = {
        #     "nickname": "96 well plate",
        #     "loadname": "opentrons_96_tiprack_1000ul",
        #     "location": "1",
        #     "ot_default": True,
        #     "config": {}
        # }
        labware, source_path = self._resolve_labware_input(labware)
        if labware["ot_default"]:
            self._load_default_labware(
                nickname=labware["nickname"],
                loadname=labware["loadname"],
                location=labware["location"],
            )
        else:
            self._load_custom_labware(
                nickname=labware["nickname"],
                labware_config=labware["config"],
                location=labware["location"],
            )
        nickname = labware["nickname"]
        self._labware_meta[nickname] = {"is_tiprack": self._is_tiprack_labware(labware)}
        tip_status_file = labware.get("tip_status_file")
        if not tip_status_file and source_path and self._has_tip_status_payload(labware):
            tip_status_file = str(source_path)
        elif tip_status_file and source_path:
            tip_path = Path(tip_status_file)
            if not tip_path.is_absolute():
                tip_status_file = str((source_path.parent / tip_path).resolve())
        if tip_status_file:
            if not self._labware_meta[nickname]["is_tiprack"]:
                raise ValueError(f"tip_status_file was set for non-tiprack labware: {nickname}")
            self._register_tip_tracker(tiprack_nickname=nickname, status_file=tip_status_file)

    @flow
    def configure_tip_tracker(self, tiprack_nickname: str, status_file: str):
        self._register_tip_tracker(tiprack_nickname=tiprack_nickname, status_file=status_file)

    @flow
    def get_next_available_tip(
        self,
        tiprack_nickname: str,
        sample_id: Optional[str] = None,
        force_pick: bool = False,
        start_well: Optional[str] = None,
    ):
        if tiprack_nickname not in self._tip_trackers:
            raise ValueError(
                f"No tip tracker configured for {tiprack_nickname!r}. "
                "Provide tip_status_file or load labware from a JSON containing tip_status."
            )

        wells = self._list_tracked_tip_wells(tiprack_nickname)
        if start_well:
            if start_well not in wells:
                raise ValueError(f"start_well {start_well!r} is not in tracked wells for {tiprack_nickname!r}")
            wells = wells[wells.index(start_well) :]

        for well in wells:
            try:
                self._validate_tip_pick(
                    tiprack_nickname=tiprack_nickname,
                    well=well,
                    sample_id=sample_id,
                    force_pick=force_pick,
                )
                return well
            except ValueError:
                continue

        raise ValueError(
            f"No available tip found in {tiprack_nickname!r} "
            f"(sample_id={sample_id!r}, force_pick={force_pick})."
        )

    @flow
    def pick_up_next_available_tip(
        self,
        pip_name: str,
        tiprack_nickname: str,
        sample_id: Optional[str] = None,
        force_pick: bool = False,
        start_well: Optional[str] = None,
    ):
        well = self.get_next_available_tip(
            tiprack_nickname=tiprack_nickname,
            sample_id=sample_id,
            force_pick=force_pick,
            start_well=start_well,
        )
        self.get_location_from_labware(labware_nickname=tiprack_nickname, position=well, top=0)
        self.pick_up_tip(pip_name=pip_name, sample_id=sample_id, force_pick=force_pick)
        return well
    
    @flow
    def load_instrument(self, instrument: Dict):
        # sample instrument Dict
        # ins = {
        #     "nickname": "p1000",
        #     "instrument_name": "p1000_single_gen2",
        #     "mount": "right",
        #     "ot_default": True,
        #     "config": {}
        # }
        if instrument["ot_default"]:
            self._load_default_instrument(
                nickname=instrument["nickname"],
                instrument_name=instrument["instrument_name"],
                mount=instrument["mount"],
            )
        else:
            self._load_custom_instrument(
                nickname=instrument["nickname"],
                instrument_config=instrument["config"],
                mount=instrument["mount"],
            )

    @flow
    def load_module(self, module: Dict):
        # sample module Dict
        # module = {
        #     "nickname": "hs",
        #     "module_name": "heaterShakerModuleV1",
        #     "location": "A1",
        #     "adapter": "opentrons_universal_flat_adapter"
        # }
        nickname = module["nickname"]
        module_name = module["module_name"]
        location = module["location"]
        adapter = module["adapter"]
        self.invoke(f"{nickname} = protocol.load_module(module_name = '{module_name}', location = '{location}')")
        self.invoke(f"{nickname}_adapter = {nickname}.load_adapter(name = '{adapter}')")

    @flow
    def load_trash_bin(self, nickname: str = "default_trash", location: str = "A3"):
        self.invoke(f"{nickname} = protocol.load_trash_bin(location = '{location}')")

    @flow
    def home(self):
        self.invoke("protocol.home()")

    @flow
    def comment(self, message: str):
        self.invoke(f"protocol.comment({self._py_repr(message)})")

    @flow
    def set_rail_lights(self, on: bool = True):
        self.invoke(f"protocol.set_rail_lights({self._py_repr(on)})")

    @flow
    def get_rail_lights(self):
        return self._invoke_bool("protocol.rail_lights_on")

    @flow
    def set_max_speed(self, axis: str, speed: float):
        # axis examples: 'X', 'Y', 'Z', 'A'
        self.invoke(f"protocol.max_speeds[{self._py_repr(axis)}] = {speed}")

    @flow
    def clear_max_speed(self, axis: str):
        self.invoke(f"protocol.max_speeds[{self._py_repr(axis)}] = None")

    @flow 
    def well_diameter(self, labware_nickname: str, position: str):
        return self._invoke_float(f"{labware_nickname}['{position}'].diameter")
    
    @flow 
    def well_depth(self, labware_nickname: str, position: str):
        return self._invoke_float(f"{labware_nickname}['{position}'].depth")
    
    @flow
    def tip_length(self, labware_nickname: str, position: str):
        rtn = self._invoke_lines(f"{labware_nickname}['{position}'].length")
        if len(rtn) == 3:
            return float(rtn[-2])
        return None

    def _location_suffix(self, top: float = 0, bottom: float = 0, center: float = 0) -> str:
        if top:
            return f".top({top})"
        if bottom:
            return f".bottom({bottom})"
        if center:
            return ".center()"
        return ".top(0)"  # preserve default behavior

    @flow
    def get_location_from_labware(
        self,
        labware_nickname: str,
        position: str,
        top: float = 0,
        bottom: float = 0,
        center: float = 0,
    ):
        append = self._location_suffix(top=top, bottom=bottom, center=center)
        self.invoke(f"location = {labware_nickname}['{position}']{append}")
        self._current_location_context = {"labware_nickname": labware_nickname, "position": position}
        
    @flow
    def get_location_absolute(self, x: float, y: float, z: float, reference: Optional[str] = None):
        # reference is deck position "1" "D1" etc. Default is None as deck itself
        self.invoke(f"location = Location(Point({x},{y},{z}), '{str(reference)}')")
        self._current_location_context = {"labware_nickname": None, "position": None}

    @flow
    def move_to_pip(self, pip_name: str):
        self.invoke(f"{pip_name}.move_to(location = location)")

    @flow
    def move_to_pip_advanced(
        self,
        pip_name: str,
        speed: Optional[float] = None,
        force_direct: Optional[bool] = None,
        minimum_z_height: Optional[float] = None,
    ):
        kwargs = self._format_kwargs(
            location="__LOCATION_SENTINEL__",
            speed=speed,
            force_direct=force_direct,
            minimum_z_height=minimum_z_height,
        )
        kwargs = kwargs.replace("'__LOCATION_SENTINEL__'", "location")
        self.invoke(f"{pip_name}.move_to({kwargs})")

    @flow
    def pick_up_tip(self, pip_name: str, sample_id: Optional[str] = None, force_pick: bool = False):
        tiprack = self._current_location_context.get("labware_nickname")
        well = self._current_location_context.get("position")
        prior_status = None
        if tiprack and well and self._is_tiprack(tiprack):
            prior_status = self._validate_tip_pick(
                tiprack_nickname=tiprack,
                well=well,
                sample_id=sample_id,
                force_pick=force_pick,
            )
        self.invoke(f"{pip_name}.pick_up_tip(location = location)")
        if tiprack and well and self._is_tiprack(tiprack):
            self._mounted_tips[pip_name] = {
                "tiprack_nickname": tiprack,
                "well": well,
                "last_sample": sample_id or prior_status,
                "origin_status": prior_status,
            }

    @flow
    def pick_up_tip_advanced(
        self,
        pip_name: str,
        location: bool = True,
        presses: Optional[int] = None,
        increment: Optional[float] = None,
        prep_after: Optional[bool] = None,
        sample_id: Optional[str] = None,
        force_pick: bool = False,
    ):
        tiprack = self._current_location_context.get("labware_nickname")
        well = self._current_location_context.get("position")
        prior_status = None
        if location and tiprack and well and self._is_tiprack(tiprack):
            prior_status = self._validate_tip_pick(
                tiprack_nickname=tiprack,
                well=well,
                sample_id=sample_id,
                force_pick=force_pick,
            )
        kwargs = self._format_kwargs(
            location="__LOCATION_SENTINEL__" if location else None,
            presses=presses,
            increment=increment,
            prep_after=prep_after,
        )
        kwargs = kwargs.replace("'__LOCATION_SENTINEL__'", "location")
        self.invoke(f"{pip_name}.pick_up_tip({kwargs})")
        if location and tiprack and well and self._is_tiprack(tiprack):
            self._mounted_tips[pip_name] = {
                "tiprack_nickname": tiprack,
                "well": well,
                "last_sample": sample_id or prior_status,
                "origin_status": prior_status,
            }

    @flow
    def return_tip(self, pip_name: str):
        self.invoke(f"{pip_name}.return_tip()")
        mounted = self._mounted_tips.pop(pip_name, None)
        if not mounted:
            return
        tiprack = mounted.get("tiprack_nickname")
        well = mounted.get("well")
        if not tiprack or not well or tiprack not in self._tip_trackers:
            return
        status = mounted.get("last_sample") or mounted.get("origin_status") or "new"
        self._write_tip_status(tiprack, well, status)

    @flow
    def return_tip_advanced(self, pip_name: str, home_after: Optional[bool] = None):
        kwargs = self._format_kwargs(home_after=home_after)
        self.invoke(f"{pip_name}.return_tip({kwargs})" if kwargs else f"{pip_name}.return_tip()")
        mounted = self._mounted_tips.pop(pip_name, None)
        if not mounted:
            return
        tiprack = mounted.get("tiprack_nickname")
        well = mounted.get("well")
        if not tiprack or not well or tiprack not in self._tip_trackers:
            return
        status = mounted.get("last_sample") or mounted.get("origin_status") or "new"
        self._write_tip_status(tiprack, well, status)

    @flow
    def drop_tip(self, pip_name: str):
        self.invoke(f"{pip_name}.drop_tip()")
        mounted = self._mounted_tips.pop(pip_name, None)
        if not mounted:
            return
        tiprack = mounted.get("tiprack_nickname")
        well = mounted.get("well")
        if tiprack and well and tiprack in self._tip_trackers:
            self._write_tip_status(tiprack, well, "empty")

    @flow
    def drop_tip_advanced(self, pip_name: str, location: bool = False, home_after: Optional[bool] = None):
        kwargs = self._format_kwargs(
            location="__LOCATION_SENTINEL__" if location else None,
            home_after=home_after,
        )
        kwargs = kwargs.replace("'__LOCATION_SENTINEL__'", "location")
        self.invoke(f"{pip_name}.drop_tip({kwargs})" if kwargs else f"{pip_name}.drop_tip()")
        mounted = self._mounted_tips.pop(pip_name, None)
        if not mounted:
            return
        tiprack = mounted.get("tiprack_nickname")
        well = mounted.get("well")
        if tiprack and well and tiprack in self._tip_trackers:
            self._write_tip_status(tiprack, well, "empty")

    @flow
    def prepare_aspirate(self, pip_name: str):
        self.invoke(f"{pip_name}.prepare_to_aspirate()")

    @flow
    def aspirate(
        self,
        pip_name: str,
        volume: Optional[float] = None,
        rate: float = 1.0,
        flow_rate: Optional[float] = None,
        movement_delay: Optional[float] = None,
    ):
        kwargs = self._format_kwargs(
            volume=volume,
            location="__LOCATION_SENTINEL__",
            rate=rate,
            flow_rate=flow_rate,
            movement_delay=movement_delay,
        )
        kwargs = kwargs.replace("'__LOCATION_SENTINEL__'", "location")
        self.invoke(f"{pip_name}.aspirate({kwargs})")
        self._mark_tip_touched_from_current_location(pip_name)

    @flow
    def dispense(
        self,
        pip_name: str,
        volume: Optional[float] = None,
        push_out: Optional[float] = None,
        rate: float = 1.0,
        flow_rate: Optional[float] = None,
        movement_delay: Optional[float] = None,
    ):
        kwargs = self._format_kwargs(
            volume=volume,
            location="__LOCATION_SENTINEL__",
            rate=rate,
            push_out=push_out,
            flow_rate=flow_rate,
            movement_delay=movement_delay,
        )
        kwargs = kwargs.replace("'__LOCATION_SENTINEL__'", "location")
        self.invoke(f"{pip_name}.dispense({kwargs})")
        self._mark_tip_touched_from_current_location(pip_name)

    @flow
    def air_gap(
        self,
        pip_name: str,
        volume: float,
        height: Optional[float] = None,
        in_place: Optional[bool] = None,
        rate: float = 1.0,
        flow_rate: Optional[float] = None,
    ):
        kwargs = self._format_kwargs(
            volume=volume,
            height=height,
            in_place=in_place,
            rate=rate,
            flow_rate=flow_rate,
        )
        self.invoke(f"{pip_name}.air_gap({kwargs})")

    @flow
    def mix(self, pip_name: str, repetitions: int, volume: Optional[float] = None, rate: float = 1.0):
        kwargs = self._format_kwargs(
            repetitions=repetitions,
            volume=volume,
            location="__LOCATION_SENTINEL__",
            rate=rate,
        )
        kwargs = kwargs.replace("'__LOCATION_SENTINEL__'", "location")
        self.invoke(f"{pip_name}.mix({kwargs})")
        self._mark_tip_touched_from_current_location(pip_name)

    @flow
    def touch_tip(
        self,
        pip_name: str,
        labware_nickname: str,
        position: str,
        radius: float = 1.0,
        v_offset: float = -1.0,
        speed: float = 60.0,
    ):
        self.invoke(
            f"{pip_name}.touch_tip({labware_nickname}['{position}'], "
            f"radius = {radius}, v_offset = {v_offset}, speed = {speed})"
        )
        if not self._is_tiprack(labware_nickname):
            mounted = self._mounted_tips.get(pip_name)
            if mounted:
                sample_id = f"{labware_nickname}_{position}"
                mounted["last_sample"] = sample_id
                tiprack = mounted.get("tiprack_nickname")
                well = mounted.get("well")
                if tiprack and well and tiprack in self._tip_trackers:
                    self._write_tip_status(tiprack, well, sample_id)

    @flow
    def blow_out(self, pip_name: str):
        self.invoke(f"{pip_name}.blow_out(location = location)")

    @flow
    def blow_out_in_place(self, pip_name: str):
        self.invoke(f"{pip_name}.blow_out()")

    @flow
    def set_speed(self, pip_name: str, speed: float):
        self.invoke(f"{pip_name}.default_speed = {speed}")

    @flow
    def set_flow_rate(
        self,
        pip_name: str,
        aspirate: Optional[float] = None,
        dispense: Optional[float] = None,
        blow_out: Optional[float] = None,
    ):
        if aspirate is not None:
            self.invoke(f"{pip_name}.flow_rate.aspirate = {aspirate}")
        if dispense is not None:
            self.invoke(f"{pip_name}.flow_rate.dispense = {dispense}")
        if blow_out is not None:
            self.invoke(f"{pip_name}.flow_rate.blow_out = {blow_out}")

    @flow
    def get_flow_rate(self, pip_name: str):
        aspirate = self._invoke_float(f"{pip_name}.flow_rate.aspirate")
        dispense = self._invoke_float(f"{pip_name}.flow_rate.dispense")
        blow_out = self._invoke_float(f"{pip_name}.flow_rate.blow_out")
        return {"aspirate": aspirate, "dispense": dispense, "blow_out": blow_out}

    @flow
    def set_plunger_speed(
        self,
        pip_name: str,
        aspirate: Optional[float] = None,
        dispense: Optional[float] = None,
        blow_out: Optional[float] = None,
    ):
        # Legacy-style plunger speed properties; may be unavailable on newer API contexts.
        if aspirate is not None:
            self.invoke(f"{pip_name}.speed.aspirate = {aspirate}")
        if dispense is not None:
            self.invoke(f"{pip_name}.speed.dispense = {dispense}")
        if blow_out is not None:
            self.invoke(f"{pip_name}.speed.blow_out = {blow_out}")

    @flow
    def set_well_bottom_clearance(
        self,
        pip_name: str,
        aspirate: Optional[float] = None,
        dispense: Optional[float] = None,
    ):
        if aspirate is not None:
            self.invoke(f"{pip_name}.well_bottom_clearance.aspirate = {aspirate}")
        if dispense is not None:
            self.invoke(f"{pip_name}.well_bottom_clearance.dispense = {dispense}")

    @flow
    def get_well_bottom_clearance(self, pip_name: str):
        aspirate = self._invoke_float(f"{pip_name}.well_bottom_clearance.aspirate")
        dispense = self._invoke_float(f"{pip_name}.well_bottom_clearance.dispense")
        return {"aspirate": aspirate, "dispense": dispense}

    @flow
    def reset_tipracks(self, pip_name: str):
        self.invoke(f"{pip_name}.reset_tipracks()")

    @flow
    def has_tip(self, pip_name: str):
        return self._invoke_bool(f"{pip_name}.has_tip")

    @flow
    def current_volume(self, pip_name: str):
        return self._invoke_float(f"{pip_name}.current_volume")

    @flow
    def set_starting_tip(self, pip_name: str, tiprack_nickname: str, position: str):
        self.invoke(f"{pip_name}.starting_tip = {tiprack_nickname}['{position}']")
    
    @flow
    def delay(self, seconds: float = 0, minutes: float = 0):
        self.invoke(f"protocol.delay(seconds={seconds}, minutes = {minutes})")
    
    @flow
    def resume(self):
        self.invoke("protocol.resume()")

    @flow
    def pause(self):
        self.invoke("protocol.pause()")

    @flow
    def move_labware_w_gripper(self, labware_nickname: str, new_location: str):
        # labware_nickname is the name of labware to move, not the loadname which could duplicate
        # new_location "1", "D1", certain module (heater/shaker etc., protocol_api.OFF_DECK)
        if new_location == "OFF_DECK":
            self.invoke(
                f"protocol.move_labware(labware = {labware_nickname}, "
                "new_location = protocol_api.OFF_DECK, use_gripper = True)"
            )
        elif "adapter" in new_location:
            self.invoke(
                f"protocol.move_labware(labware = {labware_nickname}, "
                f"new_location = {new_location}, use_gripper = True)"
            )
        else:
            self.invoke(
                f"protocol.move_labware(labware = {labware_nickname}, "
                f"new_location = '{new_location}', use_gripper = True)"
            )

    @flow
    def gripper_is_attached(self):
        self._init_low_level_hardware()
        return self._invoke_bool("hardware.has_gripper()")

    @flow
    def gripper_close_jaw(self):
        """Close gripper jaws (hardware `grip()`)."""
        self._init_low_level_hardware()
        self.invoke("assert hardware.has_gripper(), 'No gripper attached'")
        self.invoke("hardware.grip()")

    @flow
    def gripper_open_jaw(self):
        """Open gripper jaws (hardware `ungrip()`)."""
        self._init_low_level_hardware()
        self.invoke("assert hardware.has_gripper(), 'No gripper attached'")
        self.invoke("hardware.ungrip()")

    @flow
    def gripper_jaw_width(self):
        self._init_low_level_hardware()
        self.invoke("assert gripper is not None, 'No gripper attached'")
        return self._invoke_float("gripper.jaw_width")

    @flow
    def gripper_jaw_limits(self):
        self._init_low_level_hardware()
        self.invoke("assert gripper is not None, 'No gripper attached'")
        min_width = self._invoke_float("gripper.min_jaw_width")
        max_width = self._invoke_float("gripper.max_jaw_width")
        return {"min": min_width, "max": max_width}

    @flow
    def gripper_move_to_absolute(self, x: float, y: float, z: float, speed: Optional[float] = None):
        """Move gripper critical point to an absolute deck coordinate using low-level hardware API."""
        self._init_low_level_hardware()
        self.invoke("assert hardware.has_gripper(), 'No gripper attached'")
        self.invoke("assert gripper_mount is not None, 'Could not resolve gripper mount enum'")
        if speed is None:
            self.invoke(f"hardware.move_to(mount = gripper_mount, abs_position = Point({x}, {y}, {z}))")
        else:
            self.invoke(
                f"hardware.move_to(mount = gripper_mount, abs_position = Point({x}, {y}, {z}), speed = {speed})"
            )
    
    @flow
    def hs_latch_open(self, nickname: str):
        self.invoke(f"{nickname}.open_labware_latch()")

    @flow
    def hs_latch_close(self, nickname: str):
        self.invoke(f"{nickname}.close_labware_latch()")

    @flow
    def hs_set_and_wait_shake_speed(self, nickname: str, rpm: int):
        self.invoke(f"{nickname}.set_and_wait_for_shake_speed(rpm = {rpm})")

    @flow
    def hs_deactivate_shaker(self, nickname: str):
        self.invoke(f"{nickname}.deactivate_shaker()")

    @flow
    def hs_set_and_wait_temperature(self, nickname: str, celsius: float):
        self.invoke(f"{nickname}.set_and_wait_for_temperature(celsius = {celsius})")

    @flow
    def hs_set_target_temperature(self, nickname: str, celsius: float):
        self.invoke(f"{nickname}.set_target_temperature(celsius = {celsius})")

    @flow
    def hs_wait_for_temperature(self, nickname: str):
        self.invoke(f"{nickname}.wait_for_temperature()")

    @flow
    def hs_deactivate_heater(self, nickname: str):
        self.invoke(f"{nickname}.deactivate_heater()")

    @flow
    def hs_deactivate(self, nickname: str):
        self.invoke(f"{nickname}.deactivate()")
    
    @flow
    def set_rpm(self, nickname: str, rpm: int):
        if rpm in range(200, 3000):
            self.invoke(f"{nickname}.set_and_wait_for_shake_speed(rpm = {rpm})")
        else:
            self.invoke(f"{nickname}.deactivate_shaker()")
    
    @flow
    def set_temp(self, nickname: str, temp: float):
        if temp in range(27, 95):
            self.invoke(f"{nickname}.set_and_wait_for_temperature(celsius = {temp})")
        else:
            self.invoke(f"{nickname}.deactivate_heater()")

    @flow
    def get_rpm(self, nickname: str):
        return self.invoke(f"{nickname}.current_speed")
    
    @flow
    def get_temp(self, nickname: str):
        return self.invoke(f"{nickname}.current_temperature")   

    @flow
    def tempmod_set_temperature(self, nickname: str, celsius: float):
        self.invoke(f"{nickname}.set_temperature(celsius = {celsius})")

    @flow
    def tempmod_await_temperature(self, nickname: str):
        self.invoke(f"{nickname}.await_temperature()")

    @flow
    def tempmod_deactivate(self, nickname: str):
        self.invoke(f"{nickname}.deactivate()")

    @flow
    def magmod_engage(
        self,
        nickname: str,
        height_from_base: Optional[float] = None,
        offset: Optional[float] = None,
    ):
        kwargs = self._format_kwargs(height_from_base=height_from_base, offset=offset)
        self.invoke(f"{nickname}.engage({kwargs})" if kwargs else f"{nickname}.engage()")

    @flow
    def magmod_disengage(self, nickname: str):
        self.invoke(f"{nickname}.disengage()")

    @flow
    def thermocycler_open_lid(self, nickname: str):
        self.invoke(f"{nickname}.open_lid()")

    @flow
    def thermocycler_close_lid(self, nickname: str):
        self.invoke(f"{nickname}.close_lid()")

    @flow
    def thermocycler_open_labware_latch(self, nickname: str):
        self.invoke(f"{nickname}.open_labware_latch()")

    @flow
    def thermocycler_close_labware_latch(self, nickname: str):
        self.invoke(f"{nickname}.close_labware_latch()")

    @flow
    def thermocycler_set_block_temperature(
        self,
        nickname: str,
        temperature: float,
        hold_time_seconds: Optional[float] = None,
        hold_time_minutes: Optional[float] = None,
        block_max_volume: Optional[float] = None,
        ramp_rate: Optional[float] = None,
    ):
        kwargs = self._format_kwargs(
            temperature=temperature,
            hold_time_seconds=hold_time_seconds,
            hold_time_minutes=hold_time_minutes,
            block_max_volume=block_max_volume,
            ramp_rate=ramp_rate,
        )
        self.invoke(f"{nickname}.set_block_temperature({kwargs})")

    @flow
    def thermocycler_set_lid_temperature(self, nickname: str, temperature: float):
        self.invoke(f"{nickname}.set_lid_temperature(temperature = {temperature})")

    @flow
    def thermocycler_deactivate_block(self, nickname: str):
        self.invoke(f"{nickname}.deactivate_block()")

    @flow
    def thermocycler_deactivate_lid(self, nickname: str):
        self.invoke(f"{nickname}.deactivate_lid()")

    @flow
    def thermocycler_deactivate(self, nickname: str):
        self.invoke(f"{nickname}.deactivate()")

    @flow
    def remove_labware(self, labware_nickname: str):
        self.invoke(f"deck_pos = {labware_nickname}.parent")
        self.invoke(f"del protocol.deck[deck_pos]")

    @flow
    def home_pipette(self, pip_name: str):
        self.invoke(f"{pip_name}.home()")

    @flow
    def home_plunger(self, pip_name: str):
        self.invoke(f"{pip_name}.home_plunger()")

    @flow
    def close_session(self):
        self.home()
        self._disconnect()


@flow(log_prints=True)
def demo_ot2(simulation: bool = True):
    """Minimal OT-2 smoke test for core pipetting wrappers."""
    ot = OpenTrons(host_alias="ot2_training", password="accelerate", simulation=simulation)
    ot.home()

    with open(Path(r"C:\Users\aag\Downloads\matterlab_24_vialplate_3700ul.json"), "r") as f:
        plate_1 = json.load(f)
    with open(Path(r"C:\Users\aag\Downloads\matterlab_1_beaker_30000ul.json"), "r") as f:
        beaker_1 = json.load(f)
    with open(Path(r"C:\Users\aag\Downloads\matterlab_96_tiprack_10ul.json"), "r") as f:
        tips_1 = json.load(f)

    plates = [
        {
            "nickname": "plate_96_1",
            "loadname": "corning_96_wellplate_360ul_flat",
            "location": "1",
            "ot_default": True,
            "config": {},
        },
        {"nickname": "beaker", "config": beaker_1, "location": "4", "ot_default": False},
        {"nickname": "vial_24_well_1", "config": plate_1, "location": "5", "ot_default": False},
    ]
    tips = [
        {"nickname": "tip_20_96_1", "config": tips_1, "location": "7", "ot_default": False},
        {
            "nickname": "tip_300_96_1",
            "loadname": "opentrons_96_tiprack_300ul",
            "location": "8",
            "ot_default": True,
            "config": {},
        },
    ]
    for labware in plates + tips:
        ot.load_labware(labware)

    ot.load_instrument({"nickname": "p20", "instrument_name": "p20_single_gen2", "mount": "left", "ot_default": True})
    ot.load_instrument({"nickname": "p300", "instrument_name": "p300_single_gen2", "mount": "right", "ot_default": True})

    # p300 smoke test: beaker -> plate
    ot.get_location_from_labware(labware_nickname="tip_300_96_1", position="A1", top=0)
    ot.pick_up_tip(pip_name="p300")
    ot.get_location_from_labware(labware_nickname="beaker", position="A1", bottom=5)
    ot.move_to_pip(pip_name="p300")
    ot.aspirate(pip_name="p300", volume=150)
    ot.get_location_from_labware(labware_nickname="plate_96_1", position="A1", top=-1)
    ot.move_to_pip(pip_name="p300")
    ot.dispense(pip_name="p300", volume=150)
    ot.blow_out(pip_name="p300")
    ot.return_tip(pip_name="p300")

    # p20 smoke test: vial -> plate
    ot.get_location_from_labware(labware_nickname="tip_20_96_1", position="A1", top=0)
    ot.pick_up_tip(pip_name="p20")
    ot.get_location_from_labware(labware_nickname="vial_24_well_1", position="A1", bottom=5)
    ot.move_to_pip("p20")
    ot.aspirate(pip_name="p20", volume=10)
    ot.get_location_from_labware(labware_nickname="plate_96_1", position="B1", top=-1)
    ot.move_to_pip("p20")
    ot.dispense(pip_name="p20", volume=10)
    ot.blow_out(pip_name="p20")
    ot.drop_tip("p20")

    ot.close_session()


@flow(log_prints=True)
def demo_flex(simulation: bool = True):
    """Minimal Flex smoke test for module + gripper wrappers."""
    ot = OpenTrons(host_alias="otflex", password="accelerate", simulation=simulation)
    ot.home()

    for labware in [
        {
            "nickname": "plate_96_1",
            "loadname": "corning_96_wellplate_360ul_flat",
            "location": "B3",
            "ot_default": True,
            "config": {},
        },
        {
            "nickname": "tip_1000_96_1",
            "loadname": "opentrons_flex_96_filtertiprack_1000ul",
            "location": "B1",
            "ot_default": True,
            "config": {},
        },
    ]:
        ot.load_labware(labware)

    ot.load_instrument(
        {"nickname": "p1000", "instrument_name": "flex_1channel_1000", "mount": "left", "ot_default": True}
    )
    ot.load_module(
        {
            "nickname": "hs",
            "module_name": "heaterShakerModuleV1",
            "location": "A1",
            "adapter": "opentrons_universal_flat_adapter",
        }
    )

    # Heater-shaker latch smoke test (returns to closed state)
    ot.hs_latch_open(nickname="hs")
    ot.hs_latch_close(nickname="hs")

    # Low-level gripper smoke test (real hardware only)
    if not simulation and ot.gripper_is_attached():
        print("Gripper jaw limits:", ot.gripper_jaw_limits())
        print("Gripper jaw width:", ot.gripper_jaw_width())
        ot.gripper_open_jaw()
        ot.gripper_close_jaw()
        ot.gripper_open_jaw()  # return to open/idle state

        # Optional: only use a validated safe coordinate for your deck setup.
        # ot.gripper_move_to_absolute(x=200.0, y=200.0, z=250.0)

    ot.close_session()


if __name__ == "__main__":
    pass
    # demo_ot2(simulation=True)
    # demo_flex(simulation=True)
