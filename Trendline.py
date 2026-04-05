import os
import sys

from numpy._typing import _128Bit

sys.path.append("../common")
import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import Utils
from numba.core.cgutils import Loop
from stockUtils import stockUtils as su
from tqdm import tqdm


class Trendline:
    def __init__(self, logger_name=None):
        if logger_name is not None:
            self.tlogger = logging.getLogger(logger_name)
        else:
            logger_name = self.__class__.__name__
            self.tlogger = Utils.create_default_logger(logger_name)

    def check_trend_line(self, support, pivot, init_slope, np_data):
        """
        Earlier in fit_trendline_high_low, we computed a slope, intercept of line whcih fits close price
        For support, this function computes a line with following parameters
            1. Line should pass throoug lowest price
            2. slope of the line will come from optimize_slope function
        it then checks , that all prices are still above the line , else the line is invalid
        For resistance, this function does the same thing but checks if all prices are below the line
        Parameters
        ----------
        support : bool
            Whether the trend line is supposed to support or resist the price.
        pivot : int
            The index of the pivot point in the np_data array.
        init_slope : float
            The initial slope of the trend line.
        np_data : numpy array
            The array of prices to check against the trend line.

        Returns
        -------
        float
            The sum of the squared differences between the trend line and the np_data array.
            If the trend line is not valid, returns -1.0.
        """
        # using equation c= y -mx find the intecept of line with new slope
        # and passing through highest or lowest point np_data , depending on support
        # or resistance
        intercept = -init_slope * pivot + np_data[pivot]
        # commpute the y coordinate of this line using the new intecept and slope
        line_vals = init_slope * np.arange(len(np_data)) + intercept
        # compute difference between new line and np_data
        diffs = line_vals - np_data

        # Check to see if the line is valid, return -1 if it is not valid.
        # if support then all values in diffs should be less than 0 as support line
        # always below all close data. if thats not true then we have invalid line
        if support and diffs.max() > 1e-5:
            return -1.0
        # if resistance then all diffs value should be +ve as resistance line is always
        # above close data excpet for tangent point
        elif not support and diffs.min() < -1e-5:
            return -1.0

        # Squared sum of diffs between data and line
        # used a qunatitative measure of line fit for gradient descent
        err = (diffs**2.0).sum()
        return err

    def optimize_slope(self, support, pivot, init_slope, np_data):

        # we get starting delta slope from gradiest of line fitting
        # high and low point of np_close curve . Sort of arbitary
        delta_slope = (np_data.max() - np_data.min()) / len(np_data)

        # Optmization

        opt_step = 1.0
        min_step = 0.0001
        curr_step = opt_step  # current step

        best_slope = init_slope
        # get the error between intial assumption of line and data
        best_err = self.check_trend_line(support, pivot, init_slope, np_data)
        # since square of errors is being returned this wont fail
        assert best_err >= 0.0  # Shouldn't ever fail with initial slope

        get_derivative = True
        derivative = None
        count = 0
        # iterate till gradient descent step size is less than min step
        while curr_step > min_step:
            if get_derivative:
                # Numerical differentiation, increase slope by very small amount
                # to see if error increases/decreases.
                # Gives us the direction to change slope.
                slope_change = best_slope + delta_slope * min_step
                test_err = self.check_trend_line(support, pivot, slope_change, np_data)
                # did error increase or decrease
                derivative = test_err - best_err

                # If increasing by a small amount fails,
                # try decreasing by a small amount
                # if test error increased
                if test_err < 0.0:
                    slope_change = best_slope - delta_slope * min_step
                    test_err = self.check_trend_line(
                        support, pivot, slope_change, np_data
                    )
                    derivative = test_err - best_err

                if test_err < 0:
                    raise Exception("Derivative failed")
                else:
                    pass
                    # print("Intial derivative successful")
                # print("Info :Intial derivative succeeded")
                get_derivative = False

            # print(f"Derivative {count} : {derivative} best_slope = {best_slope} step :  {curr_step} error_tar={best_err}  current_error = {test_err}")
            count = count + 1
            if derivative > 0.0:
                test_slope = best_slope - delta_slope * curr_step
            else:
                test_slope = best_slope + delta_slope * curr_step

            test_err = self.check_trend_line(support, pivot, test_slope, np_data)
            # the if block code determined with min_slope adjustment that its possible to
            # reduce the overall error. Now we do bisection to find point of least error
            # if with current error we get an invalid line or error increrases then we need
            # to reduce our step size and start again
            if test_err < 0.0 or test_err >= best_err:
                opt_step = curr_step
                curr_step = curr_step / 2.0

            # if the slope adjusment resulted in error reduction , then we have found a new
            # best_slope. This will our starting point for next adjustment , and the new error
            # is the new error benchmark to beat. We revert step size back to 1 .
            else:
                best_slope = test_slope
                best_err = test_err
                curr_step = opt_step
                get_derivative = True
                # print(f"opt_point found={best_slope},{best_err},{curr_step}")
        return (best_slope, -best_slope * pivot + np_data[pivot])

    def fit_trendlines_high_low(self, np_close):
        # x will contain values from 0 to len(np_close)-1
        x = np.arange(len(np_close))
        # Assume a line with X coordinates 0,1,2,...len(np_close-1)
        # and Y-coordinates as value of np_close
        # if the line can be represenset y =mx+c
        # then coeffs[0] give the m value and coeffs[1] will c
        coefs = np.polyfit(x, np_close, 1)
        # print(coefs)

        # figure out y coordinate value lines derived from polyfit
        line_points = coefs[0] * x + coefs[1]
        # upper pivot is the index value of np_close value for which the distance
        # between polyfit line and np_close data is maximum +ve value
        uppr_pivot = (np_close - line_points).argmax()
        # lowe pivot is the index value of np_close value for which the distance
        # between polyfit line and np_close data is minimum -ve value (largest absolute)
        lower_pivot = (np_close - line_points).argmin()
        # print(line_points,uppr_pivot,lower_pivot)

        # use the line with fitted slope and lower_pivot intercept and then optimize
        # slope, such that line still touches lowest point on np_close but the overall
        # distance of line points and np_close is smallest
        support_coeffs = self.optimize_slope(True, lower_pivot, coefs[0], np_close)
        # print(support_coeffs)
        #  use the line with fitted slope and upper_pivot intercept and then optimize
        # slope, such that line still touches highest point on np_close but the overall
        # distance of line points and np_close is smallest
        resist_coeffs = self.optimize_slope(False, uppr_pivot, coefs[0], np_close)

        return (support_coeffs, resist_coeffs)

    def compute_trendlines(self, df_data, lookback, lookback_offset):
        self.df_data = df_data
        self.lookback = lookback
        if self.df_data.shape[0] < self.lookback + lookback_offset + 1:
            self.tlogger.error(
                f"Length of data {self.df_data.shape[0]} is less than lookback {self.lookback}"
            )
            return None
        data_dict = dict()
        end_index = self.df_data.shape[0] - lookback_offset + 1
        start_index = self.df_data.shape[0] - lookback - lookback_offset + 1
        candles = self.df_data.iloc[start_index:end_index]
        # print(f"start_index = {start_index}")
        # print(f"end_index = {end_index}")
        # print(f"data:")
        # print(f"{candles}")
        latest_close_price = df_data.iloc[-1]["Close"]
        np_high = candles["High"].to_numpy()
        np_low = candles["Low"].to_numpy()
        np_close = candles["Close"].to_numpy()
        support_coeffs, resist_coeffs = self.fit_trendlines_high_low(np_close)
        support_price = round(
            (support_coeffs[0] * self.lookback + support_coeffs[1]), 2
        )
        resist_price = round((resist_coeffs[0] * self.lookback + resist_coeffs[1]), 2)
        data_dict["Close"] = latest_close_price
        data_dict["Support"] = support_price
        data_dict["Resist"] = resist_price
        return data_dict

    def compute_historical_trendlines(self, df_data, lookback, lookback_offset):
        # self.df_data = df_data
        # self.lookback = lookback
        data_dict = dict()
        np_close = df_data["Close"].to_numpy()
        # print(np_close.size)
        np_support = np.zeros(np_close.size)
        np_resist = np.zeros(np_close.size)
        if df_data.shape[0] < lookback + lookback_offset + 1:
            self.tlogger.info(
                f"Data length of {df_data.shape[0]} is less than minimum required to compute trendlines"
            )
        for i in range(lookback + lookback_offset - 1, df_data.shape[0]):
            # for i in range(lookback-1,lookback):
            self.tlogger.info(f"Process tick number {i} of {df_data.shape[0]}")
            start_index = i - lookback - lookback_offset + 1
            end_index = i - lookback_offset + 1
            candles = df_data.iloc[start_index:end_index]
            np_close = candles["Close"].to_numpy()
            print(np_close)
            support_coeffs, resist_coeffs = self.fit_trendlines_high_low(np_close)
            # self.tlogger.info(f"Support coeffs {support_coeffs} Resist coeffs {resist_coeffs}")
            support_price = round((support_coeffs[0] * lookback + support_coeffs[1]), 2)
            resist_price = round((resist_coeffs[0] * lookback + resist_coeffs[1]), 2)
            np_support[i] = support_price
            np_resist[i] = resist_price
        df_data["Support"] = np_support
        df_data["Resist"] = np_resist
        return df_data
        # data_dict['Close'] = latest_close_price
        # data_dict['Support'] = support_price
        # data_dict['Resist'] = resist_price
        # yield data_dict

    def get_support_resist(self, df_data, lookback_length, lookback_offset):
        data_len = df_data.shape[0]
        if data_len > lookback_length + lookback_offset + 1:
            candles = df_data.iloc[
                data_len - lookback_length - lookback_offset - 1 : data_len
                - lookback_offset
            ]
            np_close = candles["Close"].to_numpy()
            support_coeffs, resist_coeffs = self.fit_trendlines_high_low(np_close)
            support_price = round(
                (support_coeffs[0] * lookback_length + support_coeffs[1]), 2
            )
            resist_price = round(
                (resist_coeffs[0] * lookback_length + resist_coeffs[1]), 2
            )
            return (support_price, resist_price)


if __name__ == "__main__":
    logger_name = "tLineLogger"
    tLineLogger = Utils.create_default_logger(logger_name)
    suObj = su(logger_name)
    ticker_dict = suObj.get_stock_list_new(_test_mode=0)
    # ticker_list = list(ticker_dict.keys())
    ticker_list = ["CSCO"]
    # print(ticker_list)
    df_data_dict = suObj.read_ticker_data(ticker_list, _filter_start_date="2008-01-01")
    # print(df_data_dict.keys())
    resist_dict = dict()
    support_dict = dict()
    # df_data = data_dict[ticker_list[0]]
    for ticker in tqdm(df_data_dict.keys(), desc="Computing trendlines"):
        # for ticker in df_data_dict.keys():
        tLineLogger.info(f"Computing trendlines for {ticker}")
        df_data = df_data_dict[ticker]
        tlineBot = Trendline(logger_name)
        df_data = tlineBot.compute_historical_trendlines(df_data=df_data, lookback=20)
        # print(f"Computing trendlines for {ticker}")
        # if(data_dict['Close'] < data_dict['Support']):
        # tLineLogger.info(f"{ticker} is beloe support price at {data_dict['Support']}")
        #    support_dict[ticker] = data_dict['Support']
        # elif(data_dict['Close'] > data_dict['Resist']):
        # tLineLogger.info(f"{ticker} is above resist price at {data_dict['Resist']}")
        #    resist_dict[ticker] = data_dict['Resist']
        # print(df_data.head(20))
        # print(df_data.tail(20))
    # tLineLogger.info(f"Stocks breaking out of resistance level")
    # for ticker in resist_dict.keys():
    #    print(f"{ticker} : {resist_dict[ticker]}")

    # print("\n\n")
    # tLineLogger.info(f"Stocks breaking below support level")
    # for ticker in support_dict.keys():
    #    print(f"{ticker} : {support_dict[ticker]}")
