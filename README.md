# LST Onsite Analysis (LSTOSA)

Repository for the On-Site data analysis pipeline of the LST-1, the Large-Sized Telescope prototype of the LST.

## How to use it
* You will need to install anaconda first.
* Create and activate the conda environment including [lstchain](https://github.com/cta-observatory/cta-lstchain):
```
git clone https://gitlab.cta-observatory.org/cta-array-elements/lst/analysis/lstosa.git
cd lstosa
conda env create -f environment.yml
conda activate osa
```

* To update the environment (provided dependencies get updated) use:
```
conda env update -n osa -f environment.yml
```
* Install `lstosa` (using conda-build):
```
conda develop .
```

### Further information

 - Docs: https://lstosa.readthedocs.io/ 
   - Status ![build status](https://gitlab.cta-observatory.org/cta-array-elements/lst/analysis/lstosa/badges/master/pipeline.svg)
 - Check also the Wiki for more information
