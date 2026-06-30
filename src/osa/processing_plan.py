from dataclasses import dataclass


@dataclass
class ProcessingPlan:
    input_state: str
    needs_calibration: bool


def build_processing_plan(input_state: str) -> ProcessingPlan:
    """Build a simplified processing plan based on input_state.

    Philosophy:
    - legacy_raw / gain_selected → aplicar TODA la cadena de calibración
    - catA_calibrated → NO aplicar ninguna calibración (ya vienen aplicadas)
    """
    if input_state == "catA_calibrated":
        return ProcessingPlan(
            input_state=input_state,
            needs_calibration=False,
        )

    elif input_state in ["legacy_raw", "gain_selected"]:
        return ProcessingPlan(
            input_state=input_state,
            needs_calibration=True,
        )

    else:
        raise ValueError(f"Unknown input_state: {input_state}")
