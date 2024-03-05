from typing import Union
import pytz
import pandas as pd
from datetime import datetime
import os
import numpy as np
import requests
import json
from requests.packages.urllib3.util.retry import Retry
import logging
from timeout_http_adapter import TimeoutHTTPAdapter
from timeout_http_adapter import DEFAULT_TIMEOUT


def get_data(site_code: str,        
    variable_code: Union[str, None] = None,
    start_date: Union[pd.Timestamp, None] = None,
    end_date: Union[pd.Timestamp, None] = None):


    root = logging.getLogger()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    retry_strategy = Retry(
        total=10,
        status_forcelist=[413, 429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
        backoff_factor=1,
    )    

    adapter = TimeoutHTTPAdapter(timeout=DEFAULT_TIMEOUT, max_retries=retry_strategy)
    mmw_session = requests.Session()
    mmw_session.mount("https://", adapter)
    mmw_session.mount("http://", adapter)

    logging.info("Logging in to MonitorMW")
    login_url = "https://monitormywatershed.org/login/"
    login_get_cookies = mmw_session.get(login_url).cookies
    #csfr_token = login_get_cookies["csrftoken"]
    xsrf_token = login_get_cookies["XSRF-TOKEN"]
    # http.cookies.set(name='csrftoken',value=csfr_token)
    login_data = dict(
        username="Ghimire.Santosh@epa.gov",
        password="Wat3rsh3d@2080",
        csrfmiddlewaretoken=xsrf_token,
        next="/",
    )
    r = mmw_session.post(login_url, data=login_data, headers=dict(Referer=login_url))

    mmw_session.headers.update(
    {
        "Referer": "https://monitormywatershed.org/tsv/",
        "Origin": "https://monitormywatershed.org",
    })

    data_online = get_monitormw_data(
        site_code=site_code,
        variable_code=variable_code,
        start_date=start_date,
        end_date=end_date,
        mmw_session=mmw_session
    )

    #print(data_online)
    cwd = os.getcwd()
    print(cwd)
    file_name = variable_code + "_" + start_date.strftime("%Y-%m-%d") +"_" + end_date.strftime("%Y-%m-%d")
    data_online.to_csv(file_name)


    #data_online.plot(x='timestamp',y='datavalue')
    
def get_monitormw_data(
    site_code: str,
    *,
    uuid: Union[str, None] = None,
    variable_code: Union[str, None] = None,
    start_date: Union[pd.Timestamp, None] = None,
    end_date: Union[pd.Timestamp, None] = None,
    mmw_session
) -> pd.DataFrame:
    """Fetches data from the Time Series Visualization endpoint for a specific site and variable and returns a pandas dataframe with the results.

    Args:
        site_code (str): The text site code
        uuid (str, optional): The 36 character UUID for the result; *including hyphens*.  Either the UUID *OR* the variable code should be specified, not both.  Defaults to None.
        variable_code (str, optional): The text variable code exactly as seen on MonitorMyWatershed.  Either the UUID *OR* the variable code should be specified, not both. Defaults to None.
        start_date (Timestamp, optional):  The first date to pull data from. If omitted, fetches from the beginning of available data.  **WARNING: fetching the full range of data may be very slow for time series with large amounts of data.**
        end_date (Timestamp, optional): The last date to pull data from. If omitted, fetches up to the most recent data available.  **WARNING: fetching the full range of data may be very slow for time series with large amounts of data.**

    Raises:
        ValueError: If no matching UUID or variable code can be found for the site.

    Returns:
        pd.DataFrame: The available data as a pandas data frame.
    """

    if uuid is None and variable_code is None:
        raise ValueError("Either UUID or variable code must be specified!")

    tsv_data_url = "https://monitormywatershed.org/dataloader/ajax/"

    logging.info(
        "Requesting MonitorMW metadata for {} at {}".format(
            site_code,
            pytz.utc.localize(datetime.utcnow())
            .astimezone(pytz.FixedOffset(-5 * 60))
            .isoformat(sep=" ", timespec="seconds"),
        )
    )
    tsv_result_id = None
    try:
        request_data = {
            "method": "get_sampling_feature_metadata",
            "sampling_feature_code": site_code,
        }
        request_data_str = json.dumps(request_data)
        payload = {"request_data": request_data_str}
        logging.debug(payload)

        tsv_site_req = mmw_session.post(tsv_data_url, data=payload)
        tsv_site_variables = json.loads(tsv_site_req.json())
        logging.debug(tsv_site_variables)
        for tsv_result in tsv_site_variables:
            if (uuid is not None and tsv_result["resultuuid"] == uuid) or (
                variable_code is not None
                and tsv_result["variablecode"] == variable_code
            ):
                tsv_result_id = tsv_result["resultid"]
                break
        if tsv_result_id is None:
            raise ValueError("No matching UUID or variable code found in site results!")

    except Exception as e:
        logging.warning("Error getting MMW data: {}".format(e))
        return pd.DataFrame(
            columns=[
                "timestamp",
                "datavalue",
                "valuedatetimeutcoffset",
            ]
        )

    logging.info(
        "Requesting MonitorMW data values for {} ({}, {}) at {} from {} to {} at {}".format(
            tsv_result_id,
            tsv_result["variablecode"],
            tsv_result["resultuuid"],
            tsv_result["samplingfeaturecode"],
            start_date.isoformat(sep=" ", timespec="seconds")
            if pd.notna(start_date)
            else "start of record",
            end_date.isoformat(sep=" ", timespec="seconds")
            if pd.notna(end_date)
            and end_date
            != pd.Timestamp(year=2025, month=1, day=1, tzinfo=pytz.FixedOffset(-5 * 60))
            else "end of record",
            pytz.utc.localize(datetime.utcnow())
            .astimezone(pytz.FixedOffset(-5 * 60))
            .isoformat(sep=" ", timespec="seconds"),
        )
    )

    data_online = None
    try:
        request_data = {
            "method": "get_result_timeseries",
            "resultid": "{}".format(tsv_result_id),
            "start_date": start_date.isoformat() if pd.notna(start_date) else None,
            "end_date": end_date.isoformat()
            if pd.notna(end_date)
            and end_date
            != pd.Timestamp(year=2025, month=1, day=1, tzinfo=pytz.FixedOffset(-5 * 60))
            else None,
        }
        request_data_str = json.dumps(request_data)
        payload = {"request_data": request_data_str}
        logging.debug(payload)

        data_tsv_result_req = mmw_session.post(tsv_data_url, data=payload)
        data_tsv_results = json.loads(data_tsv_result_req.json())
        data_online = pd.DataFrame.from_dict(data_tsv_results)

    except Exception as e:
        logging.warning("Error getting MMW data: {}".format(e))
        return pd.DataFrame(
            columns=["timestamp", "datavalue", "valuedatetimeutcoffset"]
        )

    # Convert the time column into a pandas datetime
    data_online["timestamp"] = pd.to_datetime(data_online["valuedatetime"], unit="ms")

    # apply timezone
    data_online["timestamp"] = data_online.groupby(
        "valuedatetimeutcoffset", dropna=False, sort=False
    )["timestamp"].transform(lambda x: x.dt.tz_localize(pytz.FixedOffset(x.name * 60)))

    if len(data_online.index) == 0:
        logging.info("   No data at all currently on MonitoryMyWatershed")
        return pd.DataFrame(
            columns=[
                "timestamp",
                "datavalue",
                "valuedatetimeutcoffset",
            ]
        )

    logging.info(
        "   {} points currently on MonitorMyWatershed between {} and {}".format(
            len(data_online.index),
            data_online["timestamp"].min().strftime("%m/%d/%Y"),
            data_online["timestamp"].max().strftime("%m/%d/%Y"),
        )
    )

    # sort by the timestamp
    data_online = data_online.sort_values(by=["timestamp"]).reset_index(drop=True)

    # infer the frequency from the most common (mode) spacing
    # NOTE: The pandas infer_freq function isn't up to the irregularities
    # in the spacing of the online data
    data_online["t_delta"] = (
        data_online["valuedatetime"] - data_online["valuedatetime"].shift(1)
    ) / 6e4
    data_online.loc[
        (data_online["t_delta"] < 0) | (data_online["t_delta"] > 1440), "t_delta"
    ] = np.nan
    # mean_freq = data_online['t_delta'].mean()
    # median_freq = data_online['t_delta'].median()
    mode_freq = data_online["t_delta"].mode().iloc[0]
    data_online["frequency"] = mode_freq

    return data_online.drop(columns=["t_delta"]).copy()



if __name__ == "__main__":
    print("Hello, World!")
    site_code = "R-RWH"
    variable_code = "Meter_Hydros21_Depth"
    now = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.FixedOffset(-5 * 60))
    start_date = now - pd.Timedelta(value=30, unit="days")
    start_date = datetime(2024, 2, 1)
    end_date = datetime(2024, 3, 1)

    cwd = os.getcwd()
    print(cwd)

    get_data(site_code, variable_code, start_date, end_date)