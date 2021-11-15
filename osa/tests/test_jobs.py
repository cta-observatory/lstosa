import os
from pathlib import Path
from textwrap import dedent

from osa.configs import options
from osa.configs.config import cfg

extra_files = Path(os.getenv("OSA_TEST_DATA", "extra"))
datasequence_history_file = extra_files / "history_files/sequence_LST1_04185.0010.history"
calibration_history_file = extra_files / "history_files/sequence_LST1_04183.history"
options.date = "2020_01_17"
options.tel_id = "LST1"


def test_historylevel():
    from osa.job import historylevel

    options.dl1_prod_id = "tailcut84"
    options.dl2_prod_id = "model1"

    level, rc = historylevel(datasequence_history_file, "DATA")
    assert level == 0
    assert rc == 0

    level, rc = historylevel(calibration_history_file, "PEDCALIB")
    assert level == 0
    assert rc == 0

    options.dl1_prod_id = "tailcut84"
    options.dl2_prod_id = "model2"

    level, rc = historylevel(datasequence_history_file, "DATA")
    assert level == 1
    assert rc == 0


def test_preparejobs(running_analysis_dir, sequence_list):
    from osa.job import preparejobs

    options.simulate = False
    options.directory = running_analysis_dir
    preparejobs(sequence_list)
    expected_calib_script = os.path.join(running_analysis_dir, "sequence_LST1_01805.py")
    expected_data_script = os.path.join(running_analysis_dir, "sequence_LST1_01807.py")
    assert os.path.isfile(os.path.abspath(expected_calib_script))
    assert os.path.isfile(os.path.abspath(expected_data_script))


def test_sequence_filenames(running_analysis_dir, sequence_list):
    from osa.job import sequence_filenames

    for sequence in sequence_list:
        sequence_filenames(sequence)
        assert sequence.script == running_analysis_dir / f"sequence_LST1_{sequence.run:05d}.py"


def test_scheduler_env_variables(sequence_list, running_analysis_dir):
    from osa.job import scheduler_env_variables
    # Extract the first sequence
    first_sequence = sequence_list[0]
    env_variables = scheduler_env_variables(first_sequence)
    assert env_variables == [
        '#SBATCH --job-name=LST1_01805',
        '#SBATCH --cpus-per-task=1',
        f'#SBATCH --chdir={running_analysis_dir}',
        '#SBATCH --output=log/slurm_01805.%4a_%A.out',
        '#SBATCH --error=log/slurm_01805.%4a_%A.err',
        '#SBATCH --partition=short',
        '#SBATCH --mem-per-cpu=5GB'
    ]
    # Extract the second sequence
    second_sequence = sequence_list[1]
    env_variables = scheduler_env_variables(second_sequence)
    assert env_variables == [
        '#SBATCH --job-name=LST1_01807',
        '#SBATCH --cpus-per-task=1',
        f'#SBATCH --chdir={running_analysis_dir}',
        '#SBATCH --output=log/slurm_01807.%4a_%A.out',
        '#SBATCH --error=log/slurm_01807.%4a_%A.err',
        '#SBATCH --array=0-18',
        '#SBATCH --partition=short',
        '#SBATCH --mem-per-cpu=16GB'
    ]


def test_job_header_template(sequence_list, running_analysis_dir):
    """Extract and check the header for the first two sequences."""
    from osa.job import job_header_template
    # Extract the first sequence
    first_sequence = sequence_list[0]
    header = job_header_template(first_sequence)
    output_string1 = dedent(f"""\
    #!/bin/env python

    #SBATCH --job-name=LST1_01805
    #SBATCH --cpus-per-task=1
    #SBATCH --chdir={running_analysis_dir}
    #SBATCH --output=log/slurm_01805.%4a_%A.out
    #SBATCH --error=log/slurm_01805.%4a_%A.err
    #SBATCH --partition=short
    #SBATCH --mem-per-cpu=5GB""")
    assert header == output_string1

    # Extract the second sequence
    second_sequence = sequence_list[1]
    header = job_header_template(second_sequence)
    output_string2 = dedent(f"""\
    #!/bin/env python
    
    #SBATCH --job-name=LST1_01807
    #SBATCH --cpus-per-task=1
    #SBATCH --chdir={running_analysis_dir}
    #SBATCH --output=log/slurm_01807.%4a_%A.out
    #SBATCH --error=log/slurm_01807.%4a_%A.err
    #SBATCH --array=0-18
    #SBATCH --partition=short
    #SBATCH --mem-per-cpu=16GB""")
    assert header == output_string2


def test_create_job_template(sequence_list):
    from osa.job import create_job_template
    options.test = True
    content = create_job_template(sequence_list[1], get_content=True)
    expected_content = dedent(f"""\
    #!/bin/env python

    #SBATCH --job-name=LST1_01807
    #SBATCH --cpus-per-task=1
    #SBATCH --chdir={Path.cwd()}/test_osa/test_files0/running_analysis/20200117/v0.1.0_v01
    #SBATCH --output=log/slurm_01807.%4a_%A.out
    #SBATCH --error=log/slurm_01807.%4a_%A.err
    #SBATCH --array=0-18
    #SBATCH --partition=short
    #SBATCH --mem-per-cpu=16GB

    import os
    import subprocess
    import sys
    import tempfile

    subruns = 0

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ['NUMBA_CACHE_DIR'] = tmpdirname
        proc = subprocess.run([
            'datasequence',
            '-c',
            '{Path.cwd()}/cfg/sequencer.cfg',
            '-d',
            '2020_01_17',
            '--prod-id',
            'v0.1.0_v01',
            '{Path.cwd()}/test_osa/test_files0/running_analysis/20200117/v0.1.0_v01/calibration.Run01805.0000.hdf5',
            '{Path.cwd()}/test_osa/test_files0/running_analysis/20200117/v0.1.0_v01/drs4_pedestal.Run01804.0000.fits',
            '{Path.cwd()}/test_osa/test_files0/running_analysis/20200117/v0.1.0_v01/time_calibration.Run01805.0000.hdf5',
            'extra/monitoring/DrivePositioning/drive_log_20_01_17.txt',
            'extra/monitoring/RunSummary/RunSummary_20200117.ecsv',
            '01807.{{0}}'.format(str(subruns).zfill(4)),
            'LST1'
        ])

    sys.exit(proc.returncode)""")
    assert content == expected_content


def test_set_cache_dirs():
    from osa.job import set_cache_dirs

    cache = set_cache_dirs()
    cache_dirs = dedent(f"""\
    os.environ['CTAPIPE_CACHE'] = '{cfg.get('CACHE', 'CTAPIPE_CACHE')}'
    os.environ['CTAPIPE_SVC_PATH'] = '{cfg.get('CACHE', 'CTAPIPE_SVC_PATH')}'
    os.environ['MPLCONFIGDIR'] = '{cfg.get('CACHE', 'MPLCONFIGDIR')}'""")
    assert cache_dirs == cache
