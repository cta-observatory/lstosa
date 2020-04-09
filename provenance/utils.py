"""
Utility functions for OSA pipeline provenance
"""

__all__ = ["parse_variables"]


def parse_variables(class_instance):
    """Parse variables needed in model"""
    # python datasequence.py -c cfg/sequencer.cfg -d 2020_02_18 -o output_directory calibration.file pedestal.file timecalibration.file drive.file 02006 LST1

    from osa.configs.config import cfg
    configfile = cfg.get('LSTOSA', 'CONFIGFILE')

    if class_instance.__name__ == "r0_to_dl1":
        class_instance.config_file = configfile
        print("config", configfile)

    return class_instance

