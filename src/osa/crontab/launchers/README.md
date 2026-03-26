
# Launchers

Currently, there are 8 launchers:

`launch_GainSel.sh`

- Executes Gain Selection  
- Stops when the flag `GainSelFinished.txt` exists  

`launch_GainSelCheck.sh`

- Launches:
  
  - Gain Selection Check  
  - Web interface

- Stops when the flag `GainSelFinished.txt` exists  

Both of them are planned for removal when GainSel is no longer needed.

`launch_Sequencer1.sh`

- Starts DL1a production
- Runs only when `GainSelFinished.txt` flag exists  

Future option: trigger when the first `gainselection*.closed` file exists.

`launch_SequencerCatB.sh`

- Launches `sequencer_catB_tailcuts`  
- Starts when the date directory is created in running_analysis

`launch_Sequencer2.sh`

- Starts DL1ab production using the first config file  
- Includes an alternative condition (commented out, fallback option)  

`launch_SequencerWeb.sh`

- Launches web Sequencer 1 and 2  
- Requires `GainSelFinished.txt` flag  

`launch_Autocloser.sh`

- Responsible for closing the night  
- Executes only when a `tailcut` directory exists in running_analysis

Sequencer and autocloser stop execution once `NightFinished.txt` exists.

`launch_Datacheck.sh`

- Executes `copy_datacheck` for DL1 files  
- Starts when the date directory exists in running_analysis

## Environment Configuration

All launchers source a common environment file `osa-env.sh`

This file:

- Defines global parameters

- Ensures consistency across all launchers

- Enables scalable design for future extensions (e.g., adding new telescopes)

## Standalone Usage

Launchers can also be executed manually (outside crontab). They allow you to

- Define the desired environment variables in the terminal

- Optionally pass additional arguments

### Example

Run in a single line:

```bash
OBS_DATE=2026-01-28 CFG=/path/to/mysequencer.cfg ./launch_Sequencer2.sh -s
```

This sets a custom observation date (OBS_DATE) and config (CFG).
It runs Sequencer2 in simulation mode (-s).
