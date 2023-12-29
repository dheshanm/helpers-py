#!/usr/bin/env python

import logging
import time
from functools import lru_cache
from pathlib import Path

import gspread

from helpers.config import config

# Silence gspread logging
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logging.getLogger("google.auth.transport.requests").setLevel(logging.ERROR)
logging.getLogger("urllib3.util.retry").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


def get_cell_notation(row_idx: int, col_idx: int) -> str:
    """
    Returns the cell notation for a given row and column index.

    Example:
        get_cell_notation(1, 1) -> 'A1'
        get_cell_notation(1, 28) -> 'AB1'

    Args:
        row_idx (int): The row index.
        col_idx (int): The column index.

    Returns:
        str: The cell notation.
    """

    col_letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    col = ""

    col_temp = col_idx
    while col_temp > 0:
        col_temp, remainder = divmod(col_temp - 1, 26)
        col = col_letters[remainder] + col

    row = str(row_idx)

    return f"{col}{row}"


def get_spreadsheet(config_file: Path) -> gspread.Spreadsheet:
    """
    Returns a Google Sheet object.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        gspread.Spreadsheet: A Google Sheet object.
    """

    config_params = config(config_file, "sheets")
    service_account_file = config_params["service_account_file"]
    sheet_id = config_params["sheet_id"]

    service_account_key = gspread.service_account(filename=Path(service_account_file))
    sheet = service_account_key.open_by_key(sheet_id)

    return sheet


def get_worksheet(config_file: Path, sheet_name: str) -> gspread.Worksheet:
    """
    Returns a Google Sheet worksheet object.

    Args:
        config_file (Path): The path to the configuration file.
        sheet_name (str): The name of the worksheet.

    Returns:
        gspread.Worksheet: A Google Sheet worksheet object.
    """
    sheet = get_spreadsheet(config_file)
    worksheet = sheet.worksheet(sheet_name)

    return worksheet


@lru_cache()
def get_row_idx(
    sheet: gspread.Worksheet, col: int, value: str, logger: logging.Logger
) -> int:
    """
    Get the row index of a specific value in a given column of a worksheet.

    Args:
        sheet (gspread.Worksheet): The worksheet to search in.
        col (int): The column number to search in.
        value (str): The value to search for.

    Returns:
        int: The row index of the first occurrence of the value in the column.

    Raises:
        ValueError: If the value is not found in the column.
    """

    def _get_col_vals():
        return sheet.col_values(col)

    cells = api_rate_limit(logger, _get_col_vals)()

    if value not in cells:
        raise ValueError(f"{value} not found in column {col}")

    idx = cells.index(value) + 1

    return idx


def update_cell(
    worksheet: gspread.Worksheet,
    row_idx: int,
    col_idx: int,
    value: str,
    logger: logging.Logger,
) -> None:
    """
    Update the value of a cell in a worksheet.

    Args:
        worksheet (gspread.Worksheet): The worksheet to update.
        row_idx (int): The row index of the cell to update.
        col_idx (int): The column index of the cell to update.
        value (str): The value to update the cell with.
    """

    def _update_cell():
        worksheet.update_cell(row_idx, col_idx, value)

    api_rate_limit(logger, _update_cell)()


def update_note(
    worksheet: gspread.Worksheet,
    row_idx: int,
    col_idx: int,
    note: str,
    logger: logging.Logger,
) -> None:
    """
    Update the note of a cell in a worksheet.

    Args:
        worksheet (gspread.Worksheet): The worksheet to update.
        row_idx (int): The row index of the cell to update.
        col_idx (int): The column index of the cell to update.
        note (str): The value to update the cell with.
    """

    def _update_note():
        worksheet.update_note(get_cell_notation(row_idx, col_idx), note)

    api_rate_limit(logger, _update_note)()


def api_rate_limit(logger: logging.Logger, func):
    """
    Decorator to handle API rate limit errors.
    """

    sleep_time = 30

    def wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    logger.warning(
                        f"API rate limit reached. Sleeping for {sleep_time} seconds."
                    )
                    time.sleep(sleep_time)
                    logger.debug("Retrying...")
                else:
                    raise e

    return wrapper
