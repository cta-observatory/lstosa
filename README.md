# LST Onsite Analysis (LSTOSA)

Repository for the On-Site data analysis pipeline of the LST-1, the Large-Sized Telescope prototype of the LST.

* Supported Python version: 3.7
* Docs: https://lstosa.readthedocs.io/
* Source: https://gitlab.cta-observatory.org/cta-array-elements/lst/analysis/lstosa
* License: [BSD-3-Clause](LICENSE)

## How to use it
* You will need to install anaconda first.
* Create and activate the conda environment including [lstchain](https://github.com/cta-observatory/cta-lstchain) and
  [ctapipe_io_lst](https://github.com/cta-observatory/ctapipe_io_lst):
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
* Install `lstosa`:
```
pip install -e .
```

