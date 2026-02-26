import os
import json
from pathlib import Path
from typing import Dict

from prefect import flow

from .sshclient import SSHClient


PROTOCOL_API_VERSION = "2.21"


class OpenTrons:
    """Thin SSH-backed wrapper for streaming Opentrons Python commands."""

    def __init__(self, host_alias: str = None, password: str = "", simulation: bool = False):
        self._connect(host_alias, password)
        self._get_protocol(simulation)

    def _connect(self, host_alias: str = None, password: str = ""):
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

    def load_labware(self, labware: Dict):
        # sample labware Dict
        # lw = {
        #     "nickname": "96 well plate",
        #     "loadname": "opentrons_96_tiprack_1000ul",
        #     "location": "1",
        #     "ot_default": True,
        #     "config": {}
        # }
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
    def home(self):
        self.invoke("protocol.home()")

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
        
    @flow
    def get_location_absolute(self, x: float, y: float, z: float, reference: str = None):
        # reference is deck position "1" "D1" etc. Default is None as deck itself
        self.invoke(f"location = Location(Point({x},{y},{z}), '{str(reference)}')")

    @flow
    def move_to_pip(self, pip_name: str):
        self.invoke(f"{pip_name}.move_to(location = location)")

    @flow
    def pick_up_tip(self, pip_name: str):
        self.invoke(f"{pip_name}.pick_up_tip(location = location)")

    @flow
    def return_tip(self, pip_name: str):
        self.invoke(f"{pip_name}.return_tip()")

    @flow
    def drop_tip(self, pip_name: str):
        self.invoke(f"{pip_name}.drop_tip()")

    @flow
    def prepare_aspirate(self, pip_name: str):
        self.invoke(f"{pip_name}.prepare_to_aspirate()")

    @flow
    def aspirate(self, pip_name: str, volume: float):
        self.invoke(f"{pip_name}.aspirate(volume = {volume}, location = location)")

    @flow
    def dispense(self, pip_name: str, volume: float, push_out: float = None):
        self.invoke(f"{pip_name}.dispense(volume = {volume}, location = location, push_out = {str(push_out)})")

    @flow
    def touch_tip(self, pip_name: str, labware_nickname: str, position: str, radius: float = 1.0, v_offset: float = -1.0):
        self.invoke(
            f"{pip_name}.touch_tip({labware_nickname}['{position}'], "
            f"radius = {radius}, v_offset = {v_offset})"
        )

    @flow
    def blow_out(self, pip_name: str):
        self.invoke(f"{pip_name}.blow_out(location = location)")

    @flow
    def set_speed(self, pip_name: str, speed: float):
        self.invoke(f"{pip_name}.default_speed = {speed}")
    
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
    def gripper_move_to_absolute(self, x: float, y: float, z: float, speed: float = None):
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
    def set_rpm(self, nickname: str, rpm: int):
        if rpm in range(200, 3000):
            self.invoke(f"{nickname}.set_and_wait_for_shake_speed(rpm={rpm})")
        else:
            self.invoke(f"{nickname}.deactivate_shaker()")
    
    @flow
    def set_temp(self, nickname: str, temp: float):
        if temp in range(27, 95):
            self.invoke(f"{nickname}.set_and_wait_for_temperature(temp={temp})")
        else:
            self.invoke(f"{nickname}.deactivate_heater()")

    @flow
    def get_rpm(self, nickname: str):
        return self.invoke(f"{nickname}.current_speed")
    
    @flow
    def get_temp(self, nickname: str):
        return self.invoke(f"{nickname}.current_temperature")   

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
    ot = OpenTrons(host_alias="ot2", password="accelerate", simulation=simulation)
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
    # demo_ot2(simulation=False)
    # demo_flex(simulation=True)
