#!/bin/sh

OBS_DATE=${OBS_DATE:-$(date +%Y-%m-%d -d yesterday)}
CFG=${CFG:-"/fefs/aswg/lstosa/cfg/sequencer_v0.11.cfg"}
CONDA_ENV=${CONDA_ENV:-"/fefs/aswg/lstosa/utils/osa-conda"}

LSTN1=${LSTN1:-"/fefs/onsite/data/lst-pipe/LSTN-01"}

export OBS_DATE CFG CONDA_ENV LSTN1
