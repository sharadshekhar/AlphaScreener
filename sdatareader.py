import csv
import logging
import os
import sys

import numpy as np
import pandas as pd
from tqdm import tqdm


class SDataReader:
    def __init__(
        self,
        stock_list_path=None,
        data_path=None,
        fundamental_data_path=None,
        logger_name=None,
    ):
        if stock_list_path is None:
            stock_list_path = "../data/stock_list/All_stocks.csv"
        if data_path is None:
            data_path = "../data/data_poly/"
        if fundamental_data_path is None:
            fundamental_data_path = "../data/data_alt/"

        self.stock_list_path = stock_list_path
        self.data_path = data_path
        self.fundamental_data_path = fundamental_data_path

        if logger_name is None:
            logger_name = "sdatareader"
        self.ulogger = self._create_logger(logger_name)

        self._ticker_info = {}
        self._data = {}
        self._loaded_indexes = set()
        self._loaded_sector_etfs = set()
        self._universe = None
        self._universe_source = None
        self._original_universe = None
        self._fundamental_data = {}

        self._load_ticker_info()

    def _create_logger(self, logger_name):
        current_directory = os.getcwd()
        log_dir = os.path.join(current_directory, "logs")
        os.makedirs(log_dir, exist_ok=True)
        filename = os.path.join(log_dir, f"{logger_name}.log")

        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            file_handler = logging.FileHandler(filename, mode="w")
            formatter = logging.Formatter(
                "%(levelname)s | %(name)s | %(funcName)s | %(message)s"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        return logger

    def _load_ticker_info(self):
        if not os.path.exists(self.stock_list_path):
            self.ulogger.error(f"Stock list file not found: {self.stock_list_path}")
            return

        data = self._read_file_to_list(self.stock_list_path)
        for line in data:
            tokens = line.strip().split(",")
            if len(tokens) >= 4:
                ticker = tokens[0]
                company_name = tokens[1] if len(tokens) > 1 else ""
                index = tokens[2]
                sector_etf = tokens[3]
                sector_name = tokens[4] if len(tokens) > 4 else ""

                self._ticker_info[ticker] = {
                    "index": index,
                    "sector_etf": sector_etf,
                    "sector_name": sector_name,
                    "company_name": company_name,
                }

        self.ulogger.info(f"Loaded ticker info for {len(self._ticker_info)} tickers")

    def _read_file_to_list(self, file_path):
        data = list()
        if os.path.isfile(file_path):
            try:
                with open(file_path, "r") as f:
                    data = f.readlines()
            except IOError as e:
                self.ulogger.error(f"Error reading file {file_path}: {e}")
        return data

    def _get_tickers_for_index(self, index):
        tickers = []
        for ticker, info in self._ticker_info.items():
            if info["index"] == index:
                tickers.append(ticker)
        return tickers

    def _get_tickers_for_sector_etf(self, sector_etf):
        tickers = []
        for ticker, info in self._ticker_info.items():
            if info["sector_etf"] == sector_etf:
                tickers.append(ticker)
        return tickers

    def _get_all_tickers(self):
        return list(self._ticker_info.keys())

    def _get_ticker_data(self, ticker):
        file_path = os.path.join(self.data_path, f"{ticker}.csv")

        if not os.path.exists(file_path):
            self.ulogger.error(f"Data file for ticker {ticker} not found: {file_path}")
            return pd.DataFrame()

        data_dict_list = []
        line_num = 0
        header_line = ""

        try:
            with open(file_path, "r") as csv_file:
                file_data = csv.reader(csv_file, delimiter=",", quotechar='"')
                for line in file_data:
                    if line_num == 0:
                        if "Last" in line[0]:
                            pass
                        else:
                            header_line = line
                            line_num = 1
                    else:
                        col_counter = 0
                        line_dict = {}
                        for element in line:
                            if col_counter < len(header_line):
                                col_key = header_line[col_counter].capitalize()
                                line_dict[col_key] = element
                            col_counter += 1
                        data_dict_list.append(line_dict)
        except Exception as e:
            self.ulogger.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()

        try:
            df_data = pd.DataFrame(data_dict_list)
            return df_data
        except Exception as e:
            self.ulogger.error(f"Error creating DataFrame for {ticker}: {e}")
            return pd.DataFrame()

    def _preprocess_ohclv(self, ticker, df_data):
        if df_data.empty:
            return df_data

        try:
            for col in ["Open", "High", "Close", "Low"]:
                if col in df_data.columns:
                    df_data[col] = pd.to_numeric(df_data[col], errors="coerce").astype("float64")
            
            if "Volume" in df_data.columns:
                df_data["Volume"] = pd.to_numeric(df_data["Volume"], errors="coerce").astype("Int64")

            if "Close" in df_data.columns:
                df_data["pct_return"] = df_data["Close"].pct_change() * 100

            if "Date" in df_data.columns:
                df_data.set_index("Date", inplace=True)
                df_data.index = pd.to_datetime(df_data.index, errors="coerce")

            return df_data
        except Exception as e:
            self.ulogger.error(f"Error preprocessing data for {ticker}: {e}")
            return pd.DataFrame()

    def _filter_data(self, df_data, filter_start, filter_end):
        if df_data.empty:
            return df_data

        df_filtered = df_data.copy()

        if filter_start is not None:
            try:
                start_dt = pd.to_datetime(filter_start)
                df_filtered = df_filtered[df_filtered.index >= start_dt]
            except Exception as e:
                self.ulogger.error(f"Error filtering by start date: {e}")

        if filter_end is not None:
            try:
                end_dt = pd.to_datetime(filter_end)
                df_filtered = df_filtered[df_filtered.index <= end_dt]
            except Exception as e:
                self.ulogger.error(f"Error filtering by end date: {e}")

        return df_filtered

    def _load_single_ticker(self, ticker, filter_start, filter_end, min_data_length):
        if ticker in self._data:
            return self._data[ticker]

        df_raw = self._get_ticker_data(ticker)
        if df_raw.empty:
            return pd.DataFrame()

        df_processed = self._preprocess_ohclv(ticker, df_raw)
        if df_processed.empty:
            return pd.DataFrame()

        df_filtered = self._filter_data(df_processed, filter_start, filter_end)

        if min_data_length is not None:
            if df_filtered.shape[0] < min_data_length:
                self.ulogger.warning(
                    f"{ticker} has {df_filtered.shape[0]} rows, less than required {min_data_length}"
                )
                return pd.DataFrame()

        self._data[ticker] = df_filtered

        if ticker in self._ticker_info:
            self._loaded_indexes.add(self._ticker_info[ticker]["index"])
            self._loaded_sector_etfs.add(self._ticker_info[ticker]["sector_etf"])

        return df_processed

    def load_data(
        self,
        ticker=None,
        index=None,
        sector_etf=None,
        test_mode=False,
        filter_start=None,
        filter_end=None,
        min_data_length=None,
    ):
        if self._universe is None:
            self._universe = set(self._ticker_info.keys())
            self._universe_source = "all"
            self._original_universe = set(self._ticker_info.keys())
            self.ulogger.info(f"Universe auto-set to all {len(self._universe)} tickers")
        
        target_tickers = []

        if ticker is not None:
            if isinstance(ticker, str):
                target_tickers = [ticker]
            else:
                target_tickers = list(ticker)
        elif index is not None:
            target_tickers = self._get_tickers_for_index(index)
        elif sector_etf is not None:
            target_tickers = self._get_tickers_for_sector_etf(sector_etf)
        else:
            if test_mode:
                target_tickers = list(self._universe)[:10]
            else:
                target_tickers = list(self._universe)

        missing_tickers = [t for t in target_tickers if t not in self._data]

        if missing_tickers:
            self.ulogger.info(f"Loading data for {len(missing_tickers)} tickers")
            for t in tqdm(missing_tickers, desc="Loading tickers"):
                self._load_single_ticker(t, filter_start, filter_end, min_data_length)

        result_data = {
            t: self._data[t]
            for t in target_tickers
            if t in self._data and not self._data[t].empty
        }

        return result_data

    def get_indexes(self):
        return list(self._loaded_indexes)

    def get_sector_etfs(self):
        return list(self._loaded_sector_etfs)

    def get_ticker_info(self, ticker):
        return self._ticker_info.get(ticker, {})

    def get_loaded_tickers(self):
        return list(self._data.keys())

    def load_fundamental_data(self, ticker=None, index=None, sector_etf=None):
        if self._universe is None:
            self._universe = set(self._ticker_info.keys())
            self._universe_source = "all"
            self._original_universe = set(self._ticker_info.keys())
            self.ulogger.info(f"Universe auto-set to all {len(self._universe)} tickers")

        target_tickers = []

        if ticker is not None:
            if isinstance(ticker, str):
                target_tickers = [ticker]
            else:
                target_tickers = list(ticker)
        elif index is not None:
            target_tickers = self._get_tickers_for_index(index)
        elif sector_etf is not None:
            target_tickers = self._get_tickers_for_sector_etf(sector_etf)
        else:
            target_tickers = list(self._universe)

        fundamental_file = os.path.join(self.fundamental_data_path, "fundamental_data.csv")

        if not os.path.exists(fundamental_file):
            self.ulogger.error(f"Fundamental data file not found: {fundamental_file}")
            return {}

        all_fundamental_data = self._read_fundamental_file(fundamental_file)

        result_data = {}
        for t in target_tickers:
            if t in all_fundamental_data:
                self._fundamental_data[t] = all_fundamental_data[t]
                result_data[t] = all_fundamental_data[t]

        self.ulogger.info(f"Loaded fundamental data for {len(result_data)} tickers")
        return result_data

    def _read_fundamental_file(self, file_path):
        result = {}
        try:
            with open(file_path, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get("symbol", "").strip()
                    if not symbol:
                        continue
                    cleaned_row = {}
                    for key, value in row.items():
                        if key == "symbol":
                            continue
                        if value is None or value == "" or value.strip() == "":
                            cleaned_row[key] = None
                        elif value.strip() == "NotFound":
                            cleaned_row[key] = None
                        else:
                            try:
                                cleaned_row[key] = float(value)
                            except ValueError:
                                cleaned_row[key] = value.strip() if value else None
                    result[symbol] = cleaned_row
        except Exception as e:
            self.ulogger.error(f"Error reading fundamental data file: {e}")
        return result

    def get_fundamental_data(self, ticker=None):
        if ticker is not None:
            return self._fundamental_data.get(ticker, {})
        return self._fundamental_data

    def load_universe(self):
        self._universe = set(self._ticker_info.keys())
        self._universe_source = "all"
        self._original_universe = set(self._ticker_info.keys())
        self.ulogger.info(f"Universe loaded with {len(self._universe)} tickers")

    def set_universe(self, ticker_list):
        valid_tickers = []
        invalid_tickers = []

        for ticker in ticker_list:
            if ticker in self._ticker_info:
                valid_tickers.append(ticker)
            else:
                invalid_tickers.append(ticker)

        if invalid_tickers:
            warning_msg = f"Tickers not found in All_stocks.csv, skipping: {invalid_tickers}"
            print(f"WARNING: {warning_msg}")
            self.ulogger.warning(warning_msg)

        if valid_tickers:
            self._universe = set(valid_tickers)
            self._universe_source = "custom"
            self._original_universe = set(self._ticker_info.keys())
            self.ulogger.info(f"Universe set with {len(self._universe)} valid tickers")
        else:
            self._universe = None
            self._universe_source = None
            self.ulogger.warning("No valid tickers provided, universe cleared")

    def get_universe(self):
        if self._universe is not None:
            return list(self._universe)
        return list(self._ticker_info.keys())

    def clear_universe(self):
        self._universe = None
        self._universe_source = None
        self.ulogger.info("Universe cleared, will load all tickers")

    def get_universe_indexes(self):
        universe = self.get_universe()
        indexes = set()
        for ticker in universe:
            if ticker in self._ticker_info:
                indexes.add(self._ticker_info[ticker]["index"])
        return sorted(list(indexes))

    def get_universe_sector_etfs(self):
        universe = self.get_universe()
        sector_etfs = set()
        for ticker in universe:
            if ticker in self._ticker_info:
                sector_etfs.add(self._ticker_info[ticker]["sector_etf"])
        return sorted(list(sector_etfs))

    def filter_universe(self, index=None, sector_etf=None):
        if self._universe is None:
            self.load_universe()

        filtered_tickers = set()

        if index is not None:
            if isinstance(index, str):
                indexes = [index]
            else:
                indexes = list(index)
            for idx in indexes:
                for ticker in self._universe:
                    if ticker in self._ticker_info and self._ticker_info[ticker]["index"] == idx:
                        filtered_tickers.add(ticker)
        else:
            filtered_tickers = self._universe.copy()

        if sector_etf is not None:
            sector_filtered = set()
            if isinstance(sector_etf, str):
                sector_etfs = [sector_etf]
            else:
                sector_etfs = list(sector_etf)
            for sec in sector_etfs:
                for ticker in filtered_tickers:
                    if ticker in self._ticker_info and self._ticker_info[ticker]["sector_etf"] == sec:
                        sector_filtered.add(ticker)
            filtered_tickers = sector_filtered

        if filtered_tickers:
            self._universe = filtered_tickers
            self._universe_source = "filter"
            self.ulogger.info(f"Universe filtered to {len(self._universe)} tickers")
        else:
            self.ulogger.warning("Filter resulted in empty universe, keeping previous universe")

    def reset_universe(self):
        if self._original_universe is not None:
            self._universe = self._original_universe.copy()
            self._universe_source = "all"
            self.ulogger.info(f"Universe reset to original {len(self._universe)} tickers")
        else:
            self.load_universe()


if __name__ == "__main__":
    reader = SDataReader()

    all_data = reader.load_data(filter_start="2022-01-01")
    print(f"Loaded {len(all_data)} tickers")

    print(f"Indexes: {reader.get_indexes()}")
    print(f"Sectors: {reader.get_sector_etfs()}")

    comm_data = reader.load_data(sector_etf="XLK", filter_start="2022-01-01")
    print(comm_data.keys())
    test_ticker = list(comm_data.keys())[5]
    print(comm_data[test_ticker].head(10))

#    print(f"Sector ETFs: {reader.get_sector_etfs()}")

#    xlf_data = reader.load_data(sector_etf="XLF")
#    print(f"XLF sector has {len(xlf_data)} tickers")
