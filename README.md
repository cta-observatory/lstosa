# LST Onsite Analysis (LSTOSA)

Repository for the On-Site data reduction pipeline of the LST.

## How to use it
* You will need to install anaconda first.
* Create and activate the conda environment including [lstchain](https://github.com/cta-observatory/cta-lstchain):
```
git clone https://gitlab.com/contrera/lstosa.git
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

 - Docs: https://contrera.gitlab.io/lstosa/ 
   - Status ![build status](https://gitlab.com/contrera/lstosa/badges/master/pipeline.svg)
 - Wiki: https://gitlab.com/contrera/lstosa/-/wikis/home