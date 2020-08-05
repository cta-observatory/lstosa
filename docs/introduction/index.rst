.. _introduction:

Introduction
************

LSTOSA is born out of experience gained on *On-Site Analysis* (OSA) of the MAGIC
data pipeline. Due to the large size of the daily recorded data, transferring the raw data 
through the network connection from La Palma island to continental Europe in
due time is an issue for the LST.
Therefore a fast *LST On-Site Analysis* (LSTOSA) chain is being developed, aimed at performing
a reduction the raw data at LST site, so that the high level data can be
delivered by internet to the CTA data centers.

1. A cron job creates a list of all the runs taken in the night (~ it takes 10 mins). The list is called a **NightSummary** file. 

2. A **sequencer** script prepares a job for each run ( also called a sequence because it also includes the necesary calibration files).

3.  These jobs are sent to the **slurm** batch system as array jobs which process each subrun in parallel. 

4. In each **subrun** 2 steps are performed:

   A. **R0 to Dl1**: including DL1 production, Dl1 datacheck and muon extraction and processing.

   B. **DL1 to Dl2** generation.

5. A **closer script** (present an operator) checks all the sequences and merges the subrun results.

6. Plots and results are transferred to their final locations








The basic scheme is shown in the :numref:`data flow`:

.. _data flow:

.. figure:: LSTOSA_flow.png

    Data flow scheme of LST onsite analysis.

