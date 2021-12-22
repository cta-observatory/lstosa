import os
from pathlib import Path
from textwrap import dedent

import pytest

from osa.configs import options
from osa.configs.config import cfg

extra_files = Path(os.getenv("OSA_TEST_DATA", "extra"))
datasequence_history_file = (
        extra_files / "history_files/sequence_LST1_04185.0010.history"
)
calibration_history_file = (
        extra_files / "history_files/sequence_LST1_04183.history"
)
options.date = "2020_01_17"
options.tel_id = "LST1"
options.prod_id = "v0.1.0"


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
    from osa.job import prepare_jobs

    options.simulate = False
    options.directory = running_analysis_dir
    prepare_jobs(sequence_list)
    expected_calib_script = os.path.join(running_analysis_dir, "sequence_LST1_01805.py")
    expected_data_script = os.path.join(running_analysis_dir, "sequence_LST1_01807.py")
    assert os.path.isfile(os.path.abspath(expected_calib_script))
    assert os.path.isfile(os.path.abspath(expected_data_script))


def test_sequence_filenames(running_analysis_dir, sequence_list):
    from osa.job import sequence_filenames

    for sequence in sequence_list:
        sequence_filenames(sequence)
        assert sequence.script == running_analysis_dir / \
               f"sequence_LST1_{sequence.run:05d}.py"


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
        '#SBATCH --array=0-10',
        '#SBATCH --partition=long',
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
    #SBATCH --array=0-10
    #SBATCH --partition=long
    #SBATCH --mem-per-cpu=16GB""")
    assert header == output_string2


def test_create_job_template_scheduler(
        sequence_list,
        drs4_time_calibration_files,
        drs4_baseline_file,
        calibration_file,
        run_summary_file
):
    from osa.job import create_job_template
    options.test = False
    options.simulate = False
    content = create_job_template(sequence_list[1], get_content=True)
    expected_content = dedent(f"""\
    #!/bin/env python

    #SBATCH --job-name=LST1_01807
    #SBATCH --cpus-per-task=1
    #SBATCH --chdir={Path.cwd()}/test_osa/test_files0/running_analysis/20200117/v0.1.0
    #SBATCH --output=log/slurm_01807.%4a_%A.out
    #SBATCH --error=log/slurm_01807.%4a_%A.err
    #SBATCH --array=0-10
    #SBATCH --partition={cfg.get('SLURM', 'PARTITION_DATA')}
    #SBATCH --mem-per-cpu={cfg.get('SLURM', 'MEMSIZE_DATA')}

    import os
    import subprocess
    import sys
    import tempfile

    os.environ['CTAPIPE_CACHE'] = '/fefs/aswg/lstanalyzer/.ctapipe/ctapipe_cache'
    os.environ['CTAPIPE_SVC_PATH'] = '/fefs/aswg/lstanalyzer/.ctapipe/service'
    os.environ['MPLCONFIGDIR'] = '/fefs/aswg/lstanalyzer/.cache/matplotlib'
    subruns = os.getenv('SLURM_ARRAY_TASK_ID')
    job_id = os.getenv('SLURM_JOB_ID')

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ['NUMBA_CACHE_DIR'] = tmpdirname
        proc = subprocess.run([
            'datasequence',
            '--config',
            '{Path.cwd()}/cfg/sequencer.cfg',
            '--date=2020_01_17',
            '--prod-id=v0.1.0',
            '--drs4-pedestal-file={drs4_baseline_file}',
            '--time-calib-file={drs4_time_calibration_files[0]}',
            '--pedcal-file={calibration_file}',
            '--drive-file={Path.cwd()}/test_osa/test_files0/monitoring/DrivePositioning/drive_log_20_01_17.txt',
            '--run-summary={run_summary_file}',
            '--stderr=log/sequence_LST1_01807.{{0}}_{{1}}.err'.format(str(subruns).zfill(4), str(job_id)),
            '--stdout=log/sequence_LST1_01807.{{0}}_{{1}}.out'.format(str(subruns).zfill(4), str(job_id)),
            '01807.{{0}}'.format(str(subruns).zfill(4)),
            'LST1'
        ])

    sys.exit(proc.returncode)""")
    options.simulate = True
    assert content == expected_content


def test_create_job_template_local(
        sequence_list,
        drs4_time_calibration_files,
        drs4_baseline_file,
        calibration_file,
        run_summary_file
):
    """Check the job file in local mode (assuming no scheduler)."""
    from osa.job import create_job_template
    options.test = True
    options.simulate = False
    content = create_job_template(sequence_list[1], get_content=True)
    expected_content = dedent(f"""\
    #!/bin/env python

    import os
    import subprocess
    import sys
    import tempfile

    subruns = 0

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ['NUMBA_CACHE_DIR'] = tmpdirname
        proc = subprocess.run([
            'datasequence',
            '--config',
            '{Path.cwd()}/cfg/sequencer.cfg',
            '--date=2020_01_17',
            '--prod-id=v0.1.0',
            '--drs4-pedestal-file={drs4_baseline_file}',
            '--time-calib-file={drs4_time_calibration_files[0]}',
            '--pedcal-file={calibration_file}',
            '--drive-file={Path.cwd()}/test_osa/test_files0/monitoring/DrivePositioning/drive_log_20_01_17.txt',
            '--run-summary={run_summary_file}',
            '01807.{{0}}'.format(str(subruns).zfill(4)),
            'LST1'
        ])

    sys.exit(proc.returncode)""")
    options.simulate = True
    assert content == expected_content


def test_create_job_scheduler_calibration(sequence_list):
    """Check the pilot job file for the calibration pipeline."""
    from osa.job import create_job_template
    options.test = True
    options.simulate = False
    content = create_job_template(sequence_list[0], get_content=True)
    expected_content = dedent(f"""\
    #!/bin/env python

    import os
    import subprocess
    import sys
    import tempfile

    subruns = 0

    with tempfile.TemporaryDirectory() as tmpdirname:
        os.environ['NUMBA_CACHE_DIR'] = tmpdirname
        proc = subprocess.run([
            'calibration_pipeline',
            '--config',
            '{Path.cwd()}/cfg/sequencer.cfg',
            '--date=2020_01_17',
            '--drs4-pedestal-run=01804',
            '--pedcal-run=01805',
            'LST1'
        ])

    sys.exit(proc.returncode)""")
    options.simulate = True
    assert content == expected_content


def test_set_cache_dirs():
    from osa.job import set_cache_dirs

    cache = set_cache_dirs()
    cache_dirs = dedent(f"""\
    os.environ['CTAPIPE_CACHE'] = '{cfg.get('CACHE', 'CTAPIPE_CACHE')}'
    os.environ['CTAPIPE_SVC_PATH'] = '{cfg.get('CACHE', 'CTAPIPE_SVC_PATH')}'
    os.environ['MPLCONFIGDIR'] = '{cfg.get('CACHE', 'MPLCONFIGDIR')}'""")
    assert cache_dirs == cache


def test_calibration_history_level():
    from osa.job import check_history_level
    levels = {
        "onsite_create_drs4_pedestal_file": 1,
        "onsite_create_calibration_file": 0
    }
    level, exit_status = check_history_level(
        calibration_history_file, levels
    )
    assert level == 0
    assert exit_status == 0


@pytest.fixture
def mock_sacct_output():
    """Mock output of sacct to be able to use it in get_squeue_output function."""
    return Path("./extra") / 'sacct_output.csv'


@pytest.fixture
def mock_squeue_output():
    """Mock output of squeue to be able to use it in get_squeue_output function."""
    return Path("./extra") / 'squeue_output.csv'


@pytest.fixture
def sacct_output(mock_sacct_output):
    from osa.job import get_sacct_output
    return get_sacct_output(mock_sacct_output)


@pytest.fixture
def squeue_output(mock_squeue_output):
    from osa.job import get_squeue_output
    return get_squeue_output(mock_squeue_output)


def test_set_queue_values(
        sacct_output,
        squeue_output,
        sequence_list
):
    from osa.job import set_queue_values
    set_queue_values(
        sacct_info=sacct_output,
        squeue_info=squeue_output,
        sequence_list=sequence_list,
    )
    assert sequence_list[0].state == "RUNNING"
    assert sequence_list[0].exit is None
    assert sequence_list[0].jobid == 12951086
    assert sequence_list[0].cputime == "00:36:00"
    assert sequence_list[0].tries == 4
    assert sequence_list[1].state == "PENDING"
    assert sequence_list[1].tries == 2
    assert sequence_list[1].exit is None
    assert sequence_list[2].state == "PENDING"
    assert sequence_list[2].exit is None
    assert sequence_list[2].tries == 1


def test_plot_job_statistics(sacct_output, running_analysis_dir):
    from osa.job import plot_job_statistics
    log_dir = running_analysis_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    assert log_dir.exists()
    plot_job_statistics(sacct_output, log_dir)
    plot_file = log_dir / "job_statistics.pdf"
    assert plot_file.exists()


def test_get_time_calibration_file(drs4_time_calibration_files):
    from osa.job import get_time_calibration_file
    for file in drs4_time_calibration_files:
        assert file.exists()

    run = 1616
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[0]

    run = 1625
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[0]

    run = 1900
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[0]

    run = 4211
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[1]

    run = 5000
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[1]

    run = 5979
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[2]

    run = 6000
    time_file = get_time_calibration_file(run)
    assert time_file == drs4_time_calibration_files[2]
