"""
This script find runs within specified DataFrame index.
"""
import numpy as np


def find_runs(rows, window):
    """
    This function finds runs within DataFrame index and only considered when the length of the runs is equal or more
    than the specified window.
    :param rows: DataFrame rows
    :param window: Specified window
    :return: List containing lists of runs start and end index.
    """
    runs = list()  # Runs list result
    error_ind = rows.index.tolist()  # Find row with value above the threshold
    padded_ind = np.concatenate(([0], error_ind, [0]))  # Add zero at start and end of the array
    ind_diff = np.diff(padded_ind)

    if ind_diff[0] == 1:  # This means there is a run a the beginning
        error_runs_end = list([0])
        error_runs_end = error_runs_end + np.where(ind_diff != 1)[0].tolist()
    else:
        error_runs_end = np.where(ind_diff != 1)[0].tolist()

    runs_count = len(error_runs_end) - 1  # The total count of runs found in the column

    for runs_end in range(0, runs_count):
        start = error_runs_end[runs_end]
        end = error_runs_end[runs_end+1]
        run_index = rows.index[start:end].tolist()

        if len(run_index) >= window:  # Check the runs length
            runs.append(run_index)

    return _run_to_range(runs)


def _run_to_range(runs_list):
    """
    This function converts runs index to runs start and end only.
    :param runs_list: The runs index list
    :return: Returns list.
    """
    error_ranges = list()

    for run in runs_list:
        run_starts = run[0]
        run_ends = run[len(run) - 1]
        index_range = [run_starts, run_ends]
        error_ranges.append(index_range)

    return error_ranges
