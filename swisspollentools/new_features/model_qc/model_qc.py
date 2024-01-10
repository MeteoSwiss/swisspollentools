import os
import glob

from datetime import date, datetime, timedelta
from typing import Any, List

import numpy as np
import scipy as sp
import pandas as pd

def get_poleno_data(
    poleno_paths: List[str],
    model_class_dict: dict,
    supervisor_params: dict,
    atol: float=1e-1
) -> pd.DataFrame:
    """
    The function is responsible to read files created with the SPT inference
    pipeline and to run the supervisor on the data to create the time series.

    Parameters:
    -----------
    - poleno_paths: A list of .csv files produced with the SPT inference pipeline
        or a list of directories containing the files
    - model_class_dict: Translation dictionnary from integer class to named 
        class, i.e. `{0: "alnus", 1: "betula", 2: "corylus"}`
    - supervisor_params: Dictionary with the parameters for the supervisor as
        `{key: (threshold_soft, threshold_hard, supervisor_numb)}`
    - atol: Tolerance for the verification that prediction/probs field contains
        probabilities

    Raise:
    ------
    - ValueError when the prediction/probs entries does not sum to 1
    
    Return:
    -------
    - pd.DataFrame

    Example:
    --------
    ```
    poleno_paths = [
        "../../../../swisspollen-reanalysis/results/2021/3/poleno-4/0/"
    ]
    model_class_dict = {0: "alnus", 1: "betula", 2: "corylus"}
    supervisor_params = {
        "alnus": (0.85, 0.998, 100),
        "betula": (0.95, 0.999, 50),
        "corylus": (0.97, 0.9999, 100)
    }

    poleno_data = get_poleno_data(
        poleno_paths,
        model_class_dict,
        supervisor_params
    )
    ```

    Notes:
    ------
    - Supervisor description:
        https://link.springer.com/article/10.1007/s10453-022-09757-4
    """
    # Assert the model_class_dict and supervisor_param have the same keys
    keys = set(model_class_dict.values()).intersection(set(supervisor_params.keys()))
    assert len(keys) == len(model_class_dict)

    # Format the paths and create the dataframe with all the data
    def add_suffix(path: str, suffix: str="*.csv") -> str:
        if not path.endswith(suffix):
            return path + suffix
        return path
    poleno_paths = [add_suffix(path) for path in poleno_paths]
    poleno_paths = [path for pattern in poleno_paths for path in glob.glob(pattern)]

    data = pd.concat([pd.read_csv(path) for path in poleno_paths])

    # Format the prediction/probs field
    data["prediction/probs"] = data["prediction/probs"] \
        .apply(lambda x: x[1:-1].split(',')) \
        .apply(lambda x: np.array(x).astype(np.float32)) \
    
    # Assert the probabilities sums to 1
    check_sum = data["prediction/probs"] \
        .apply(sum) \
        .apply(lambda x: np.isclose(x, b=1, atol=atol))
    assert check_sum.sum() == len(data)

    # Format the metadata/utcEvent as a date
    data["metadata/date"] = data["metadata/utcEvent"] \
        .apply(datetime.fromtimestamp) \
        .apply(lambda dt: dt.date())

    # Select the data of interest based on the predicted class
    data_of_interest = data["prediction/class"] \
        .apply(lambda x: x in model_class_dict.keys())
    data = data[data_of_interest]

    # Select the data necessary for counting
    data = data[["metadata/date", "prediction/class", "prediction/certainity"]]

    # Name the predicted class
    data["prediction/class"] = data["prediction/class"].apply(lambda x: model_class_dict[x])

    # Soft & hard filter for the supervisor
    soft_filter = data \
        .apply(
            lambda row: row["prediction/certainity"] > supervisor_params[row["prediction/class"]][0],
            axis=1
        )
    hard_filter = data \
        .apply(
            lambda row: row["prediction/certainity"] > supervisor_params[row["prediction/class"]][1],
            axis=1
        )

    # Count the data for each filter
    def get_counts(filter):
        counts = data[filter] \
            .rename(
                {
                    "metadata/date": "date",
                    "prediction/class": "class",
                    "prediction/certainity": "count"
                }, 
                axis=1
            ) \
            .groupby(["class", "date"]) \
            .count() \
            .reset_index() \
            .pivot(index="date", columns="class") \
            .fillna(0) \
            .astype(np.int32)
        counts.columns = [col for _, col in counts.columns]

        # Fill the missing columns
        empty_columns = list(keys.difference(set(counts.columns)))
        counts[empty_columns] = 0

        counts = counts[list(keys)]
        return counts

    soft_data = get_counts(soft_filter)
    hard_data = get_counts(hard_filter)

    # Fill the missing index
    min_date = soft_data.index.min()
    max_date = soft_data.index.max()
    delta = timedelta(1)

    real_index = pd.DataFrame(
        index=[
            min_date + i*delta \
                for i in range(int((max_date - min_date) / delta))
        ]
    )
    soft_data = real_index.join(soft_data).fillna(0).sort_index()
    hard_data = real_index.join(hard_data).fillna(0).sort_index()

    # Compute the supervisor mask
    supervisor = hard_data \
        .rolling(window=3) \
        .apply(lambda window: sum(window.iloc[:-1])).fillna(0)
    for key in keys:
        supervisor[key] = supervisor[key] > supervisor_params[key][2]
    supervisor = supervisor.sort_index().sort_index(axis=1)

    # Apply the supervisor mask to the data
    data = pd.DataFrame.multiply(soft_data, supervisor).astype(np.int32)

    return data

def get_hirst_data(
    hirst_path: str,
    poleno_id: int,
    hirst_class_dict: dict,
    nan_value: Any=32767
) -> pd.DataFrame:
    """
    This function is responsible for reading the hirst data. The hirst data
    is a csv file that should have a `STA` column with integers indicating 
    the poleno ids that will be used to filter the data frame. The file also
    has the `JAHR`, `MO`, `TG` columns to specify the date. Such will can be 
    obtained using Climap with outputs a tabular file than can then be
    processed, i.e. using awk, to get a csv file.

    Parameters:
    -----------
    - hirst_path: Path to the csv file containing the hirst data
    - poleno_id: Identifier of the poleno/station of interest
    - hirst_class_dict: Translation dictionnary from integer class to named 
        class, i.e. `{"4837": "alnus", "4839": "betula", "4844": "corylus"}`
    - nan_value: list of values that should be replaced by a `np.nan`

    Return:
    -------
    - pd.DataFrame

    Example:
    --------
    ```
    hirst_path = "../../../../swisspollen-reanalysis/data/donnes_manuelles.csv"
    poleno_id = 965
    hirst_class_dict = {"4837": "alnus", "4839": "betula", "4844": "corylus"}

    hirst_data = get_hirst_data(
        hirst_path,
        poleno_id,
        hirst_class_dict
    )
    ```
    """
    # Load the data and filter according to the poleno_id
    data = pd.read_csv(hirst_path)
    data = data[data["STA"] == poleno_id]

    # Format the JAHR, MO, TG columns as a date
    data["date"] = data[["JAHR", "MO", "TG"]] \
        .apply(lambda row: date(row["JAHR"], row["MO"], row["TG"]), axis=1)
    
    # Keep only the date (as index) and the counts columns defined in the 
    # hirst class dictionary. 
    data = data[
        ["date"] + [col for col in data.columns if col in hirst_class_dict]
    ]
    data = data.set_index("date")
    data = data.rename(hirst_class_dict, axis=1)

    # Replace the nan_value by np.nan
    data = data.replace(nan_value, np.nan)

    # Fill the missing index
    min_date = data.index.min()
    max_date = data.index.max()
    delta = timedelta(1)

    real_index = pd.DataFrame(
        index=[
            min_date + i*delta \
                for i in range(int((max_date - min_date) / delta))
        ]
    )
    data = real_index.join(data).sort_index().sort_index(axis=1)

    return data

def stats_fn(
    poleno_data: pd.DataFrame,
    hirst_data: pd.DataFrame,
    flow: float=40 * 0.001 / 60
) -> pd.DataFrame:
    """
    This function compute the various defined statistics between the poleno
    data and the hirst data. The four defined statistics are:
        - Kendall Correlation Coefficient
        - Spearman Correlation Coefficient
        - Kurtosis over the ratio Hirst / Poleno
        - Sampling Coefficient: (N * R^(-1)) / (F * C) with 
            - N: the number of particles detected by the Poleno with a given
                model
            - R: the temporal scale, i.e. 24*60*60 seconds for a day
            - F: the flow, i.e. 40L/min -> 40 * 10^(-3) / 60 m^3/s
            - C: the concentration detected by the hirst in particles / m^3

    Parameters:
    -----------
    - poleno_data: a dataframe computed with the get_poleno_data function
    - hirst_data: a dataframe computed with the get_hirst_data function
    - flow: poleno air flow in m^3/s

    Return:
    -------
    - pd.DataFrame

    Example:
    --------
    ```
    poleno_paths = [
        "../../../../swisspollen-reanalysis/results/2021/3/poleno-4/0/"
    ]
    hirst_path = "../../../../swisspollen-reanalysis/data/donnes_manuelles.csv"
    poleno_id = 965
    model_class_dict = {0: "alnus", 1: "betula", 2: "corylus"}
    hirst_class_dict = {"4837": "alnus", "4839": "betula", "4844": "corylus"}
    supervisor_params = {
        "alnus": (0.85, 0.998, 100),
        "betula": (0.95, 0.999, 50),
        "corylus": (0.97, 0.9999, 100)
    }

    poleno_data = get_poleno_data(
        poleno_paths,
        model_class_dict,
        supervisor_params
    )
    hirst_data = get_hirst_data(
        hirst_path,
        poleno_id,
        hirst_class_dict
    )

    statistics = stats_fn(poleno_data, hirst_data)
    ```
    """
    TEMPORAL_SCALE = 24 * 60 * 60

    # Assert the poleno dataframe and hirst dataframe have the same columns
    keys = set(poleno_data.columns).intersection(set(hirst_data.columns))
    assert len(keys) == len(poleno_data.columns)

    # Join the poleno dataframe and hirst dataframe
    joined = poleno_data.join(
        hirst_data,
        validate="one_to_one",
        lsuffix="Poleno",
        rsuffix="Hirst"
    )

    # Compute kendall, spearman, kurtosis and sampling statistic for each 
    # pollen class
    statistics = {}
    for key in keys:
        key_poleno = key + "Poleno"
        key_hirst = key + "Hirst"

        kendalltau = sp.stats.kendalltau(
            joined[key_poleno],
            joined[key_hirst],
            nan_policy="omit"
        ).statistic
        spearmanr = sp.stats.spearmanr(
            joined[key_poleno],
            joined[key_hirst],
            nan_policy="omit"
        ).statistic
        kurtosis = sp.stats.kurtosis(
            joined[key_hirst][joined[key_poleno] > 0] / \
                joined[key_poleno][joined[key_poleno] > 0],
            fisher=True,
            nan_policy="omit"
        )
        sampling_numerator = \
            joined[key_poleno][joined[key_hirst] > 0] * (1 / TEMPORAL_SCALE)
        sampling_denominator = \
            flow * joined[key_hirst][joined[key_hirst] > 0]
        sampling = sampling_numerator / sampling_denominator

        sampling_mean = np.mean(sampling)
        sampling_sem = sp.stats.sem(sampling)
        sampling_ci_inf, sampling_ci_sup = sp.stats.norm.interval(
            0.95, loc=sampling_mean, scale=sampling_sem
        )

        statistics[key] = (
            kendalltau,
            spearmanr,
            kurtosis,
            sampling_mean,
            sampling_ci_inf,
            sampling_ci_sup
        )

    # Convert the dictionnary into a dataframe
    statistics = pd.DataFrame.from_dict(statistics).transpose()
    statistics.columns = [
        "Kendall", "Spearman", "Tailedness",
        "SamplingMean", "SamplingCiInf", "SamplingCiSup"
    ]

    return statistics

def model_qc(
    poleno_paths: List[str],
    hirst_path: str,
    poleno_id: int,
    model_class_dict: dict,
    hirst_class_dict: dict,
    supervisor_params: dict,
    atol: float=1e-1,
    nan_value: Any=32767,
    flow: float=40 * 0.001 / 60
) -> pd.DataFrame:
    """
    This function compute the various statistics for the model quality check 
    given a list of files produced with the SPT Inference pipeline and a Hirst
    data file. The hirst data is a csv file that should have a `STA` column 
    with integers indicating the poleno ids that will be used to filter the 
    data frame. The file also has the `JAHR`, `MO`, `TG` columns to specify the
    date. Such will can be obtained using Climap with outputs a tabular file 
    than can then be processed, i.e. using awk, to get a csv file.

    Parameters:
    -----------
    - poleno_paths: A list of .csv files produced with the SPT inference pipeline
        or a list of directories containing the files
    - hirst_path: Path to the csv file containing the hirst data
    - poleno_id: Identifier of the poleno/station of interest
    - model_class_dict: Translation dictionnary from integer class to named 
        class, i.e. `{0: "alnus", 1: "betula", 2: "corylus"}`
    - hirst_class_dict: Translation dictionnary from integer class to named 
        class, i.e. `{"4837": "alnus", "4839": "betula", "4844": "corylus"}
    - supervisor_params: Dictionary with the parameters for the supervisor as
        `{key: (threshold_soft, threshold_hard, supervisor_numb)}`
    - atol: Tolerance for the verification that prediction/probs field contains
        probabilities`
    - nan_value: list of values that should be replaced by a `np.nan`
    - flow: poleno air flow in m^3/s
    
    Return:
    -------
    - pd.DataFrame

    Example:
    --------
    ```
    poleno_paths = ["../../../../swisspollen-reanalysis/results/2021/3/poleno-4/0/"]
    hirst_path = "../../../../swisspollen-reanalysis/data/donnes_manuelles.csv"
    poleno_id = 965
    model_class_dict = {0: "alnus", 1: "betula", 2: "corylus"}
    supervisor_params = {"alnus": (0.85, 0.998, 10), "betula": (0.95, 0.999, 10), "corylus": (0.97, 0.9999, 10)}
    hirst_class_dict = {"4837": "alnus", "4839": "betula", "4844": "corylus"}

    statistics = model_qc(
        poleno_paths,
        hirst_path,
        poleno_id,
        model_class_dict,
        hirst_class_dict,
        supervisor_params
    )
    ```
    """
    poleno_data = get_poleno_data(
        poleno_paths=poleno_paths,
        model_class_dict=model_class_dict,
        supervisor_params=supervisor_params,
        atol=atol
    )
    hirst_data = get_hirst_data(
        hirst_path=hirst_path,
        poleno_id=poleno_id,
        hirst_class_dict=hirst_class_dict,
        nan_value=nan_value
    )
    stats = stats_fn(
        poleno_data=poleno_data,
        hirst_data=hirst_data,
        flow=flow
    )
    return stats