from prefect import flow
from matterlab_opentrons import OpenTrons

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
        ot.gripper_move_to_absolute(x=200.0, y=200.0, z=250.0)

    ot.close_session()

demo_flex(simulation=True)