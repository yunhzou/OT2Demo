from prefect import flow
from pathlib import Path
import json
import time
from matterlab_opentrons import OpenTrons

@flow(log_prints=True)
def demo_ot2(simulation:bool = True):
    ot = OpenTrons(host_alias="ot2_training", password = "accelerate", simulation=simulation)
    ot.home()

    plates = [
        {"nickname": "plate_96_1", "loadname": "corning_96_wellplate_360ul_flat", "location": "1", "ot_default": True, "config": {}},
        {"nickname": "plate_96_2", "loadname": "corning_96_wellplate_360ul_flat", "location": "2", "ot_default": True, "config": {}},
        {"nickname": "resovir", "loadname": "agilent_1_reservoir_290ml", "location": "3", "ot_default": True, "config": {}},
        ]
    tips = [
        {"nickname": "tip_300_96_1", "loadname": "opentrons_96_tiprack_300ul", "location": "4", "ot_default": True, "config": {}},
    ]
    for plate in plates:
        ot.load_labware(plate)
    for tip in tips:
        ot.load_labware(tip)

    # ot.load_instrument({"nickname": "p20", "instrument_name": "p20_single_gen2", "mount": "left", "ot_default": True})
    ot.load_instrument({"nickname": "p300", "instrument_name": "p300_single_gen2", "mount": "left", "ot_default": True})
    
    sample_num=96

    for i in range(0, sample_num):
        location = f"{chr(65+i//12)}{i%12+1}"
        ot.get_location_from_labware(labware_nickname="tip_300_96_1", position= location, top=0)
        ot.pick_up_tip(pip_name="p300")

        ot.get_location_from_labware(labware_nickname="resovir", position="A1", bottom=10)
        ot.move_to_pip("p300")
        ot.aspirate(pip_name="p300", volume=300)
        time.sleep(1)

        ot.get_location_from_labware(labware_nickname="plate_96_1", position=location, bottom=1)
        ot.move_to_pip("p300")
        ot.dispense(pip_name="p300", volume=300)
        time.sleep(1)

        for _ in range(0,3):
            ot.aspirate(pip_name="p300", volume=200)
            time.sleep(1)
            ot.dispense(pip_name="p300", volume=200)
            time.sleep(1)
        
        ot.get_location_from_labware(labware_nickname="plate_96_1", position=location, bottom=3)
        time.sleep(1)
        ot.aspirate(pip_name="p300", volume=30)
        ot.get_location_from_labware(labware_nickname="plate_96_1", position=location, top=5)
        time.sleep(1)

        ot.get_location_from_labware(labware_nickname="plate_96_2", position=location, top=-1)
        ot.dispense(pip_name="p300", volume=30)
        ot.blow_out(pip_name="p300")

        ot.drop_tip(pip_name="p300")   

    ot.close_session()



if __name__ == "__main__":
    demo_ot2(simulation=False)