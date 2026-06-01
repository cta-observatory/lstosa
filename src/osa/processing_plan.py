from dataclasses import dataclass

@dataclass
class ProcessingPlan:
    input_state: str
    run_catA_calibration: bool
    run_pedestal_correction: bool
    run_time_calibration: bool  
    run_systematic_correction: bool

def build_processing_plan(input_state: str) -> ProcessingPlan:

    if input_state == "legacy_raw":
        return ProcessingPlan(
            input_state=input_state,
            run_catA_calibration=True,
            run_pedestal_correction=True,
            run_time_calibration=True,
            run_systematic_correction=True,
        )

    elif input_state == "gain_selected":
        return ProcessingPlan(
            input_state=input_state,
            run_catA_calibration=True,
            run_pedestal_correction=True,
            run_time_calibration=True,
            run_systematic_correction=True,
        )

    elif input_state == "catA_calibrated":
        return ProcessingPlan(
            input_state=input_state,
            run_catA_calibration=False,
            run_pedestal_correction=False,
            run_time_calibration=False,
            run_systematic_correction=False,
        )

    else:
        raise ValueError(f"Unknown input_state: {input_state}")
