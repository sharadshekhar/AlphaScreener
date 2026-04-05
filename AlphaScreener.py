import sys 
import os 
import logging
import argparse
import pickle
from datetime import datetime, timedelta
sys.path.append("../common/")
from sdatareader import SDataReader
from Trendline import Trendline
from encrypt_pickle import load_encrypted_pickle
import pandas as pd
import pandas_ta as ta
import numpy as np

CACHE_FILE = os.path.join(os.path.dirname(__file__), "data_cache.pkl.enc")

class AlphaScreener:
    def __init__(self,lookback=20, status_lookback=60, decrypt_key=None):
        if not decrypt_key:
            raise ValueError(
                "Decryption key is required to access the data.\n"
                "Please enter the decryption key in the sidebar to run the screener."
            )
        
        if not os.path.exists(CACHE_FILE):
            raise FileNotFoundError(
                f"Data cache file not found: {CACHE_FILE}\n\n"
                "Please ensure the encrypted pickle file exists before running the screener."
            )
        
        self.decrypt_key = decrypt_key
        self.reader = SDataReader()
        self.tline = Trendline()
        self.data = self._get_data()
        self.lookback = lookback
        self.status_lookback = status_lookback
        self.spy_df = self._prepare_benchmark()
        self._chart_data = {}
        self._support_resist = {}

    def _get_data(self):
        """
        Read stock data from encrypted pickle cache file.

        Returns
        -------
        None
        """
        try:
            df_data_dict = load_encrypted_pickle(CACHE_FILE, self.decrypt_key)
        except ValueError as e:
            raise ValueError(
                "Incorrect decryption key. Please check your key and try again.\n"
                "If you don't have the correct key, contact the administrator."
            )
        
        for ticker, df in df_data_dict.items():
            df['3_day_pct_return'] = df['Close'].pct_change(periods=3)
            df['5_day_pct_return'] = df['Close'].pct_change(periods=5)
        
        return df_data_dict

    def _prepare_benchmark(self):
        """Prepares SPY as the master calendar and return series."""
        if 'SPY' not in self.data:
            raise ValueError("SPY must be in the data dictionary as a benchmark.")
        
        spy = self.data['SPY'].copy()
        spy.index = pd.to_datetime(spy.index, errors='coerce')
        spy = spy[spy.index.notna()]
        spy = spy[spy.index >= '2024-01-01']
        spy = spy[spy.index <= datetime.now()]
        spy = spy.sort_index()
        spy['spy_ret'] = spy['Close'].pct_change()
        return spy

    def align_ticker_data(self, ticker_df):
        """Fixes missing dates by reindexing to the SPY master calendar."""
        ticker_df.index = pd.to_datetime(ticker_df.index, errors='coerce')
        ticker_df = ticker_df[ticker_df.index.notna()]
        ticker_df = ticker_df[ticker_df.index >= '2024-01-01']
        ticker_df = ticker_df[ticker_df.index <= datetime.now()]
        ticker_df = ticker_df.sort_index()

        # 1. Reindex to SPY dates (Master Calendar)
        aligned = ticker_df.reindex(self.spy_df.index)

        # 2. Handle Gaps: Prices stay flat, Volume goes to zero
        aligned['Close'] = aligned['Close'].ffill()
        aligned['Volume'] = aligned['Volume'].fillna(0)
        
        # 3. Calculate Returns and Alpha
        aligned['stock_ret'] = aligned['Close'].pct_change().fillna(0)
        aligned = aligned.join(self.spy_df['spy_ret'], how='left')
        aligned['alpha'] = aligned['stock_ret'] - aligned['spy_ret']

        # 4. Drop leading NaNs (dates before the stock existed)
        return aligned.dropna(subset=['Close'])

    def _compute_daily_status(self, df_aligned):
        """Compute rolling status for each day using rolling windows."""
        df = df_aligned.copy()
        sma20 = df['Close'].rolling(window=20).mean()
        extension_pct = ((df['Close'] - sma20) / sma20) * 100
        df['extension_pct'] = extension_pct

        batting_avg_5d = df['alpha'].rolling(window=5).apply(lambda x: np.sum(x > 0), raw=True)
        df['batting_avg_5d'] = batting_avg_5d

        true_range = pd.concat([
            df['High'] - df['Low'],
            abs(df['High'] - df['Close'].shift(1)),
            abs(df['Low'] - df['Close'].shift(1))
        ], axis=1).max(axis=1)
        df['atr'] = true_range.rolling(window=14).mean()
        atr_avg_10d = df['atr'].rolling(window=10).mean()
        df['is_contracting'] = df['atr'] < (0.70 * atr_avg_10d)

        status = pd.Series(index=df.index, dtype=object)
        status[:] = "PULLBACK"
        
        mask_extended = extension_pct > 8
        mask_breakout = (extension_pct < 3) & (batting_avg_5d >= 4)
        mask_coiling = df['is_contracting']
        
        status[mask_coiling] = "COILING"
        status[mask_breakout] = "FRESH BREAKOUT"
        status[mask_extended] = "EXTENDED (AVOID)"

        df['status'] = status
        return df

    def _analyze_status_history(self, df_with_status, lookback_days=60):
        """Analyze status transitions over the lookback period.
        
        Returns:
            current_status, previous_status, days_since_change, days_in_prev_status
        """
        cutoff_idx = len(df_with_status) - lookback_days
        if cutoff_idx < 0:
            cutoff_idx = 0
        recent = df_with_status.iloc[cutoff_idx:].copy()
        recent = recent.dropna(subset=['status'])

        if recent.empty:
            return "PULLBACK", None, 0, 0

        current_status = recent['status'].iloc[-1]

        status_changes = []
        prev = None
        for i, (idx, row) in enumerate(recent.iterrows()):
            s = row['status']
            if s != prev:
                status_changes.append((i, idx, s))
                prev = s

        if len(status_changes) < 2:
            return current_status, None, len(recent) - 1, 0

        last_change_idx = status_changes[-1][0]
        prev_change_idx = status_changes[-2][0]
        previous_status = status_changes[-2][2]

        days_since_change = len(recent) - 1 - last_change_idx
        days_in_prev_status = last_change_idx - prev_change_idx

        return current_status, previous_status, days_since_change, days_in_prev_status
    def run_scan(self):
        results = []

        for ticker, df in self.data.items():
            if ticker in ['SPY', 'QQQ'] or len(df) < self.lookback + 5:
                continue

            try:
                # --- Step 1: Data Alignment ---
                df_aligned = self.align_ticker_data(df)
                
                # --- Step 2: Z-Score (Standardized Alpha) ---
                alpha_mean = df_aligned['alpha'].rolling(window=self.lookback).mean()
                alpha_std = df_aligned['alpha'].rolling(window=self.lookback).std()
                
                df_aligned['z_score'] = np.where(alpha_std > 0, 
                                               (df_aligned['alpha'] - alpha_mean) / alpha_std, 
                                               0)

                # --- Step 3: Extract Last 5 Days ---
                last_5 = df_aligned.tail(5)
                
                batting_avg = np.sum(last_5['alpha'] > 0)
                avg_z_5d = last_5['z_score'].mean()
                
                current_vol = df_aligned['Volume'].iloc[-1]
                avg_vol_5d = df_aligned['Volume'].rolling(5).mean().iloc[-1]
                rvol = current_vol / avg_vol_5d if avg_vol_5d > 0 else 0

                sma20 = df_aligned['Close'].rolling(20).mean().iloc[-1]
                current_price = df_aligned['Close'].iloc[-1]
                extension_pct = ((current_price - sma20) / sma20) * 100

                true_range = pd.concat([
                    df_aligned['High'] - df_aligned['Low'],
                    abs(df_aligned['High'] - df_aligned['Close'].shift(1)),
                    abs(df_aligned['Low'] - df_aligned['Close'].shift(1))
                ], axis=1).max(axis=1)
                atr_14 = true_range.rolling(window=14).mean().iloc[-1]
                atr_avg_10d = true_range.rolling(window=14).mean().rolling(window=10).mean().iloc[-1]
                is_contracting = atr_14 < (0.70 * atr_avg_10d) if atr_avg_10d > 0 else False

                vibe_status = "PULLBACK"
                if extension_pct > 8:
                    vibe_status = "EXTENDED (AVOID)"
                elif extension_pct < 3 and batting_avg >= 4:
                    vibe_status = "FRESH BREAKOUT"
                elif is_contracting:
                    vibe_status = "COILING"

                df_with_status = self._compute_daily_status(df_aligned)
                current_status, prev_status, days_since_change, days_in_prev = self._analyze_status_history(
                    df_with_status, lookback_days=self.status_lookback
                )

                if prev_status and prev_status != current_status:
                    status_display = f"{current_status}/{prev_status} ({days_since_change}d/{days_in_prev}d)"
                else:
                    status_display = f"{current_status} ({days_since_change}d)"

                info = self.reader.get_ticker_info(ticker)

                sr = self.tline.get_support_resist(df_aligned, lookback_length=20, lookback_offset=1)
                if sr:
                    support_price, resist_price = sr
                    self._support_resist[ticker] = {'support': support_price, 'resist': resist_price}
                else:
                    support_price, resist_price = None, None
                    self._support_resist[ticker] = {'support': None, 'resist': None}

                if resist_price and support_price:
                    if current_price > resist_price:
                        price_zone = "ABOVE RESIST"
                    elif current_price < support_price:
                        price_zone = "BELOW SUPPORT"
                    else:
                        price_zone = "IN RANGE"
                else:
                    price_zone = "N/A"

                results.append({
                    'Ticker': ticker,
                    'Company': info.get('company_name', ''),
                    'Sector': info.get('sector_etf', ''),
                    'Price': round(df_aligned['Close'].iloc[-1], 2),
                    'Status': status_display,
                    'Price Zone': price_zone,
                    'Support': round(support_price, 2) if support_price else None,
                    'Resist': round(resist_price, 2) if resist_price else None,
                    'Ext_20MA_%': round(extension_pct, 2),
                    'Batting_Avg': int(batting_avg),
                    'Z_Score_5D': round(avg_z_5d, 2),
                    'RVOL': round(rvol, 2)
                })

            except Exception as e:
                print(f"Skipping {ticker} due to error: {e}")
                continue

        final_df = pd.DataFrame(results)
        
        final_df = final_df.sort_values(
            by=['Batting_Avg', 'Z_Score_5D', 'RVOL'], 
            ascending=[False, False, False]
        ).reset_index(drop=True)

        return final_df

    def run_recovery_scan(self):
        from RecoveryScreener import RecoveryScreener
        rs = RecoveryScreener()
        results = []

        for ticker, df in self.data.items():
            if ticker in ['SPY', 'QQQ'] or len(df) < 80:
                continue

            try:
                df_aligned = self.align_ticker_data(df)
                alpha_series = df_aligned['alpha']

                metrics = rs.analyze_stock(df_aligned, alpha_series=alpha_series)
                if metrics is None:
                    continue

                info = self.reader.get_ticker_info(ticker)

                result = {
                    'Ticker': ticker,
                    'Company': info.get('company_name', ''),
                    'Sector': info.get('sector_etf', ''),
                    'Price': metrics['current_price'],
                    'Status': metrics['status'],
                    'Recovery_Score': metrics['recovery_score'],
                    'Prior_High': metrics['prior_high'],
                    'Crash_Low': metrics['crash_low'],
                    'Max_Drawdown_%': metrics['max_drawdown_pct'],
                    'Recovery_%': metrics['recovery_pct'],
                    'Upside_to_High_%': metrics['upside_to_high_pct'],
                    'Days_Since_Low': metrics['days_since_low'],
                    'RSI': metrics['rsi'],
                    'RSI_Trending': 'YES' if metrics['rsi_trending_up'] else 'NO',
                    'MACD_Pos': 'YES' if metrics['macd_positive'] else 'NO',
                    'MACD_Rising': 'YES' if metrics['macd_rising'] else 'NO',
                    'Higher_Lows': metrics['higher_lows_count'],
                    'SMA20>SMA50': 'YES' if metrics['sma20_above_sma50'] else 'NO',
                    'Vol_Trend': metrics['volume_trend_ratio'],
                    'RVOL': metrics['rvol'],
                    'Batting_Avg': metrics['batting_avg_5d'],
                    'Z_Score_5D': metrics['z_score_5d'],
                    'Fib_Level': metrics['fib_level'],
                    'MM_Target_50': metrics['measured_move_50'],
                    'MM_Target_100': metrics['measured_move_100'],
                }
                results.append(result)

            except Exception as e:
                print(f"Skipping {ticker} recovery scan: {e}")
                continue

        final_df = pd.DataFrame(results)
        if final_df.empty:
            return final_df

        final_df = final_df.sort_values(
            by=['Recovery_Score', 'Upside_to_High_%', 'Batting_Avg'],
            ascending=[False, False, False]
        ).reset_index(drop=True)

        self._recovery_results = final_df
        return final_df

    def get_chart_data(self, ticker, days=365):
        if ticker not in self.data:
            reader = SDataReader()
            df_dict = reader.load_data(
                ticker=[ticker],
                filter_start=(datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
                min_data_length=1
            )
            if ticker not in df_dict:
                return pd.DataFrame()
            df = df_dict[ticker].copy()
        else:
            df = self.data[ticker].copy()

        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[df.index.notna()]
        df = df[df.index >= '2024-01-01']
        df = df.sort_index()
        
        if df.empty:
            return pd.DataFrame()
            
        cutoff = df.index[-1] - timedelta(days=days)
        df = df[df.index >= cutoff]
        df = df[df.index <= datetime.now()]

        if not df.empty and 'Close' in df.columns:
            df['SMA_20'] = df['Close'].rolling(window=20).mean()
            df['SMA_50'] = df['Close'].rolling(window=50).mean()
            df['Volume_MA'] = df['Volume'].rolling(window=20).mean()

        return df

    def get_status_markers(self, ticker, days=365):
        if ticker not in self.data:
            return []

        df = self.data[ticker].copy()
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = df[df.index.notna()]
        df = df[df.index >= '2024-01-01']
        df_aligned = self.align_ticker_data(df)
        df_with_status = self._compute_daily_status(df_aligned)

        cutoff = df_with_status.index[-1] - timedelta(days=days)
        df_with_status = df_with_status[df_with_status.index >= cutoff]

        markers = []
        prev_status = None
        for idx, row in df_with_status.iterrows():
            s = row['status']
            if pd.notna(s) and s != prev_status and prev_status is not None:
                markers.append({
                    'date': idx,
                    'price': row['Close'],
                    'from_status': prev_status,
                    'to_status': s
                })
            if pd.notna(s):
                prev_status = s

        return markers

    def get_all_chart_data(self, tickers, days=365):
        for t in tickers:
            self.get_chart_data(t, days)

    def get_screener_instance(self):
        return self


if __name__ == "__main__":
    screener = AlphaScreener(lookback=20)
    top_stocks = screener.run_scan()
    print(top_stocks.head(20))