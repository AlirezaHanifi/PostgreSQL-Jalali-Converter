"""Module for retrieving and processing calendar data with Jalali dates and Iranian holidays.

This module provides functions to generate a calendar DataFrame for a specified date range, 
including both Gregorian and Jalali dates, along with functionalities to retrieve Iranian 
holiday information."""

import jalali_pandas
import pandas as pd
from loguru import logger


def _generate_base_dataframe(start_date: str, end_date: str) -> pd.DataFrame:
    date_df = pd.DataFrame({"date": pd.date_range(start_date, end_date, freq="D")})
    date_df["date"] = date_df["date"].dt.strftime("%Y-%m-%d")
    return date_df


def _generate_dates_with_slashes(
    df: pd.DataFrame, gregorian_date_column: str
) -> pd.Series:
    gregorian_date_series = df[gregorian_date_column]
    return gregorian_date_series.str.replace("-", "/")


def _convert_gregorian_to_jalali(
    df: pd.DataFrame, gregorian_date_column: str
) -> pd.Series:
    gregorian_date_series = df[gregorian_date_column]
    return (
        pd.to_datetime(gregorian_date_series)
        .jalali.to_jalali()
        .apply(lambda x: x.strftime("%Y-%m-%d"))
    )


def _expand_jalali_dates(df: pd.DataFrame, jalali_date_column: str) -> pd.DataFrame:
    jdate_df = pd.DataFrame()
    jalali_date_series = df[jalali_date_column].jalali.parse_jalali("%Y-%m-%d")

    jdate_df["jyear_number"] = jalali_date_series.jalali.year
    jdate_df["jmonth_number"] = jalali_date_series.jalali.month
    jdate_df["jday_number"] = jalali_date_series.jalali.day
    jdate_df["jweekday_number"] = jalali_date_series.jalali.weekday
    jdate_df["jweek_number"] = jalali_date_series.apply(lambda x: x.isocalendar()[1])
    jdate_df["jquarter_number"] = jalali_date_series.jalali.quarter
    return jdate_df


def _map_quarter_number_to_name(quarter_number: int) -> str:
    quarter_mapper_dict = {
        1: "بهار",
        2: "تابستان",
        3: "پاییز",
        4: "زمستان",
    }
    return quarter_mapper_dict.get(quarter_number, "Unknown")


def _map_month_number_to_name(month_number: int) -> str:
    month_mapper_dict = {
        1: "فروردین",
        2: "اردیبهشت",
        3: "خرداد",
        4: "تیر",
        5: "مرداد",
        6: "شهریور",
        7: "مهر",
        8: "آبان",
        9: "آذر",
        10: "دی",
        11: "بهمن",
        12: "اسفند",
    }
    return month_mapper_dict.get(month_number, "Unknown")


def _map_weekday_number_to_name(weekday_number: int) -> str:
    weekday_mapper_dict = {
        0: "شنبه",
        1: "یک‌شنبه",
        2: "دوشنبه",
        3: "سه‌شنبه",
        4: "چهارشنبه",
        5: "پنج‌شنبه",
        6: "جمعه",
    }
    return weekday_mapper_dict.get(weekday_number, "Unknown")


def _get_holiday_info_per_day(date_with_slashes: str, timeout: int = 5) -> pd.Series:
    import json

    import requests

    url = f"https://holidayapi.ir/gregorian/{date_with_slashes}"
    res = requests.get(url, timeout=timeout)
    data = json.loads(res.content)

    is_holiday = False
    holiday_events = []

    for event in data["events"]:
        if event["is_holiday"]:
            is_holiday = True
            holiday_events.append(event["description"])

    logger.debug(f"[🔍] Checked holiday status for {date_with_slashes}.")

    return pd.Series(
        {"is_holiday": is_holiday, "holiday_events": ", ".join(holiday_events)}
    )


def _get_holiday_info(df: pd.DataFrame, gregorian_date_column: str) -> pd.DataFrame:
    date_df = df.copy()
    date_df["date_with_slash"] = _generate_dates_with_slashes(
        df=df, gregorian_date_column=gregorian_date_column
    )
    return date_df["date_with_slash"].apply(_get_holiday_info_per_day)


def get_calendar_with_jalali_and_holidays(
    start_date: str, end_date: str
) -> pd.DataFrame:
    basic_calendar_df = _generate_base_dataframe(
        start_date=start_date, end_date=end_date
    )

    basic_calendar_df["jdate"] = _convert_gregorian_to_jalali(
        df=basic_calendar_df, gregorian_date_column=basic_calendar_df.columns[0]
    )
    basic_calendar_df = pd.concat(
        [
            basic_calendar_df,
            _expand_jalali_dates(df=basic_calendar_df, jalali_date_column="jdate"),
        ],
        axis=1,
    )

    basic_calendar_df["jquarter_name"] = basic_calendar_df["jquarter_number"].apply(
        lambda x: _map_quarter_number_to_name(quarter_number=x)
    )
    basic_calendar_df["jmonth_name"] = basic_calendar_df["jmonth_number"].apply(
        lambda x: _map_month_number_to_name(month_number=x)
    )
    basic_calendar_df["jweekday_name"] = basic_calendar_df["jweekday_number"].apply(
        lambda x: _map_weekday_number_to_name(weekday_number=x)
    )

    basic_calendar_df = pd.concat(
        [
            basic_calendar_df,
            _get_holiday_info(df=basic_calendar_df, gregorian_date_column="date"),
        ],
        axis=1,
    )
    logger.info(
        "[✅] Successfully added the date range (%s - %s) to the database.",
        start_date,
        end_date,
    )
    return basic_calendar_df
