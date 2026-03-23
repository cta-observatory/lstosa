# Launcher Framework

The launcher framework was introduced to reduce the computational load
caused by processes running continuously without interruption.

---

## Launchers

Currently, there are 8 launchers:

### 1. `launch_GainSel`

- Executes Gain Selection  
- Stops when the flag `GainSelFinished` exists  

### 2. `launch_GainSelCheck`

- Launches:
  
  - Gain Selection Check  
  - Web interface

- Stops when the flag `GainSelFinished` exists  

Both of them are planned for removal when no longer GainSel.



### 3. `launch_Sequencer1`

- Starts DL1a production
- Runs only when `GainSelFinished` flag exists  

Future option: trigger when the first `gainselection*.closed` file exists.

### 4. `launch_sequencerCatB`

- Launches `sequencer_catb_tailcuts`  
- Starts when the date directory is created in running_analysis

### 5. `launch_Sequencer2`

- Starts DL1ab production using the first config file  
- Includes an alternative condition (commented out, fallback option)  

### 6. `launch_SequencerWeb`

- Launches web Sequencer 1 and 2  
- Requires `GainSelFinished` flag  

### 7. `launch_autocloser`

- Responsible for closing the night  
- Executes only when a `tailcut` directory exists in running_analysis

Sequencer and autocloser stop execution once `NightFinished.txt` exists.

### 8. `launch_Datacheck`

- Executes `copy_datacheck` for DL1 files  
- Starts when the date directory exists in running_analysis

---

### Environment Configuration

All launchers source a common environment file `osa-env.sh`

This file:

- Defines global parameters

- Ensures consistency across all launchers

- Enables scalable design for future extensions (e.g., adding new telescopes)

### Standalone Usage

Launchers can also be executed manually (outside crontab). They allow you to

- Define the desired environment variables in the terminal

- Optionally pass additional arguments

### Example

Run in a single line:

```bash
OBS_DATE=2026-01-28 
CFG=/fefs/aswg/workspace/maria.rivero/lstosa/src/osa/configs/mysequencer1.cfg 
./launch_Sequencer2.sh -s
```

This sets a custom observation date (OBS_DATE) and config (CFG).
It runs Sequencer2 in simulation mode (-s).
