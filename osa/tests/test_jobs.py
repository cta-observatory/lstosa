import os
from pathlib import Path
from textwrap import dedent

from osa.configs import options

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


def test_preparejobs(test_data, sequence_list):
    from osa.job import preparejobs

    options.simulate = False
    options.directory = test_data[3]
    preparejobs(sequence_list)
    expected_calib_script = os.path.join(test_data[3], "sequence_LST1_01805.py")
    expected_data_script = os.path.join(test_data[3], "sequence_LST1_01807.py")
    assert os.path.isfile(os.path.abspath(expected_calib_script))
    assert os.path.isfile(os.path.abspath(expected_data_script))


def test_setsequencefilenames(test_data, sequence_list):
    from osa.job import setsequencefilenames

    for sequence in sequence_list:
        setsequencefilenames(sequence)
        assert sequence.script == os.path.join(test_data[3], f"sequence_LST1_{sequence.run:05d}.py")


def test_scheduler_env_variables(sequence_list):
    from osa.job import scheduler_env_variables
    # Extract the first sequence
    first_sequence = sequence_list[0]
    env_variables = scheduler_env_variables(first_sequence)
    assert env_variables == [
        'SBATCH --job-name=LST1_01805',
        'SBATCH --cpus-per-task=1',
        'SBATCH --chdir=testfiles/running_analysis/20200117/v0.1.0_v01',
        'SBATCH --output=log/slurm_01805.%4a_%A.out',
        'SBATCH --error=log/slurm_01805.%4a_%A.err',
        'SBATCH --partition=short',
        'SBATCH --mem-per-cpu=5GB'
    ]
    # Extract the second sequence
    second_sequence = sequence_list[1]
    env_variables = scheduler_env_variables(second_sequence)
    assert env_variables == [
        'SBATCH --job-name=LST1_01807',
        'SBATCH --cpus-per-task=1',
        'SBATCH --chdir=testfiles/running_analysis/20200117/v0.1.0_v01',
        'SBATCH --output=log/slurm_01807.%4a_%A.out',
        'SBATCH --error=log/slurm_01807.%4a_%A.err',
        'SBATCH --array=0-18',
        'SBATCH --partition=short',
        'SBATCH --mem-per-cpu=16GB'
    ]


def test_job_header_template(sequence_list):
    """Extract and check the header for the first two sequences."""
    from osa.job import job_header_template
    # Extract the first sequence
    first_sequence = sequence_list[0]
    header = job_header_template(first_sequence)
    output_string1 = dedent("""\
    #!/bin/env python

    SBATCH --job-name=LST1_01805
    SBATCH --cpus-per-task=1
    SBATCH --chdir=testfiles/running_analysis/20200117/v0.1.0_v01
    SBATCH --output=log/slurm_01805.%4a_%A.out
    SBATCH --error=log/slurm_01805.%4a_%A.err
    SBATCH --partition=short
    SBATCH --mem-per-cpu=5GB""")
    assert header == output_string1

    # Extract the second sequence
    second_sequence = sequence_list[1]
    header = job_header_template(second_sequence)
    output_string2 = dedent("""\
    #!/bin/env python
    
    SBATCH --job-name=LST1_01807
    SBATCH --cpus-per-task=1
    SBATCH --chdir=testfiles/running_analysis/20200117/v0.1.0_v01
    SBATCH --output=log/slurm_01807.%4a_%A.out
    SBATCH --error=log/slurm_01807.%4a_%A.err
    SBATCH --array=0-18
    SBATCH --partition=short
    SBATCH --mem-per-cpu=16GB""")
    assert header == output_string2
