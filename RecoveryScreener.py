import numpy as np
import pandas as pd


class RecoveryScreener:
    """
    Identifies stocks that crashed hard, reversed into a confirmed uptrend,
    and still have significant room to run before reclaiming prior highs.
    """

    def __init__(self, min_drawdown_pct=30, max_drawdown_pct=70,
                 min_recovery_pct=15, max_recovery_pct=60,
                 min_days_since_low=10, max_days_since_low=80,
                 min_upside_to_high=30, min_rvol_recovery=1.2,
                 min_batting_avg=3, min_rsi=50, max_rsi=72):
        self.min_drawdown_pct = min_drawdown_pct
        self.max_drawdown_pct = max_drawdown_pct
        self.min_recovery_pct = min_recovery_pct
        self.max_recovery_pct = max_recovery_pct
        self.min_days_since_low = min_days_since_low
        self.max_days_since_low = max_days_since_low
        self.min_upside_to_high = min_upside_to_high
        self.min_rvol_recovery = min_rvol_recovery
        self.min_batting_avg = min_batting_avg
        self.min_rsi = min_rsi
        self.max_rsi = max_rsi

    def find_prior_high_and_low(self, close):
        """
        Find the most significant prior high and the subsequent crash low.
        Returns: (prior_high, prior_high_idx, crash_low, crash_low_idx, max_drawdown_pct)
        """
        rolling_max = close.cummax()
        drawdown = (close - rolling_max) / rolling_max * 100

        max_dd_idx = drawdown.idxmin()
        max_drawdown_pct = drawdown.loc[max_dd_idx]

        peak_before_dd = close.loc[:max_dd_idx].idxmax()
        prior_high = close.loc[peak_before_dd]
        prior_high_idx = close.index.get_loc(peak_before_dd)

        crash_low = close.loc[max_dd_idx]
        crash_low_idx = close.index.get_loc(max_dd_idx)

        return prior_high, prior_high_idx, crash_low, crash_low_idx, max_drawdown_pct

    def count_higher_lows(self, close, low, num_lows=3, window=5):
        """
        Check if the stock is forming higher lows after the crash.
        Returns: (count_of_higher_lows, list_of_low_prices)
        """
        lows = []
        for i in range(len(low) - window, -1, -1):
            chunk = low.iloc[i:i + window]
            local_low = chunk.min()
            if len(lows) == 0 or local_low < lows[-1] * 0.98:
                lows.append(local_low)
            if len(lows) >= num_lows + 1:
                break

        lows = list(reversed(lows))
        higher_low_count = 0
        for i in range(1, len(lows)):
            if lows[i] > lows[i - 1]:
                higher_low_count += 1

        return higher_low_count, lows

    def compute_fibonacci_levels(self, high, low):
        """Fibonacci retracement levels from crash low to prior high."""
        diff = high - low
        return {
            'fib_236': low + diff * 0.236,
            'fib_382': low + diff * 0.382,
            'fib_500': low + diff * 0.500,
            'fib_618': low + diff * 0.618,
            'fib_786': low + diff * 0.786,
        }

    def compute_measured_move_target(self, prior_high, crash_low):
        """
        Measured move = decline magnitude projected from the reversal low.
        Conservative: 50% of the decline. Aggressive: 100%.
        """
        decline = prior_high - crash_low
        return {
            'measured_move_50pct': crash_low + decline * 0.50,
            'measured_move_100pct': crash_low + decline * 1.00,
        }

    def compute_rsi(self, close, period=14):
        """RSI calculation."""
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def compute_macd_histogram(self, close):
        """MACD histogram for momentum direction."""
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        histogram = macd - signal
        return histogram

    def compute_volume_trend(self, volume, period=10):
        """
        Is volume increasing on the recovery?
        Returns ratio of recent 5d avg volume to prior 10d avg volume.
        """
        if len(volume) < 15:
            return 1.0
        recent_5d = volume.iloc[-5:].mean()
        prior_10d = volume.iloc[-15:-5].mean()
        return recent_5d / prior_10d if prior_10d > 0 else 1.0

    def compute_sma_crossover_status(self, close):
        """
        Check SMA20 vs SMA50 relationship and recent crossover.
        Returns: (above_sma20, above_sma50, sma20_above_sma50, days_since_cross)
        """
        sma20 = close.rolling(20).mean()
        sma50 = close.rolling(50).mean()

        current_price = close.iloc[-1]
        above_sma20 = current_price > sma20.iloc[-1]
        above_sma50 = current_price > sma50.iloc[-1]
        sma20_above_sma50 = sma20.iloc[-1] > sma50.iloc[-1]

        days_since_cross = None
        if len(sma20) > 1 and len(sma50) > 1:
            cross_mask = (sma20.shift(1) <= sma50.shift(1)) & (sma20 > sma50)
            if cross_mask.any():
                last_cross_idx = cross_mask[::-1].idxmax()
                days_since_cross = (close.index[-1] - last_cross_idx).days

        return above_sma20, above_sma50, sma20_above_sma50, days_since_cross

    def analyze_stock(self, df, alpha_series=None):
        """
        Main analysis function. Takes a DataFrame with OHLCV columns.
        Returns a dict of all recovery metrics or None if stock doesn't qualify.
        """
        if len(df) < 60:
            return None

        close = df['Close']
        high = df['High']
        low = df['Low']
        volume = df['Volume']

        prior_high, ph_idx, crash_low, cl_idx, max_drawdown = \
            self.find_prior_high_and_low(close)

        current_price = close.iloc[-1]
        recovery_pct = ((current_price - crash_low) / crash_low) * 100
        upside_to_high = ((prior_high - current_price) / current_price) * 100
        days_since_low = (close.index[-1] - close.index[cl_idx]).days

        if max_drawdown > -self.min_drawdown_pct:
            return None
        if max_drawdown < -self.max_drawdown_pct:
            return None
        if recovery_pct < self.min_recovery_pct:
            return None
        if recovery_pct > self.max_recovery_pct:
            return None
        if upside_to_high < self.min_upside_to_high:
            return None
        if days_since_low < self.min_days_since_low:
            return None
        if days_since_low > self.max_days_since_low:
            return None

        above_sma20, above_sma50, sma20_above_sma50, days_since_cross = \
            self.compute_sma_crossover_status(close)

        if not above_sma20:
            return None

        rsi = self.compute_rsi(close)
        current_rsi = rsi.iloc[-1]
        rsi_5d_ago = rsi.iloc[-5] if len(rsi) > 5 else current_rsi
        rsi_trending = current_rsi > rsi_5d_ago

        if current_rsi < self.min_rsi or current_rsi > self.max_rsi:
            return None

        macd_hist = self.compute_macd_histogram(close)
        macd_positive = macd_hist.iloc[-1] > 0
        macd_rising = macd_hist.iloc[-1] > macd_hist.iloc[-3] if len(macd_hist) > 3 else False

        if not macd_positive:
            return None

        higher_lows, low_list = self.count_higher_lows(close, low)

        vol_trend = self.compute_volume_trend(volume)

        current_vol = volume.iloc[-1]
        avg_vol_5d = volume.iloc[-5:].mean()
        rvol = current_vol / avg_vol_5d if avg_vol_5d > 0 else 0

        batting_avg = 0
        z_score_5d = 0
        if alpha_series is not None:
            last_5_alpha = alpha_series.iloc[-5:]
            batting_avg = int((last_5_alpha > 0).sum())
            z_score_5d = last_5_alpha.mean() / last_5_alpha.std() if last_5_alpha.std() > 0 else 0

        if batting_avg < self.min_batting_avg:
            return None

        fib_levels = self.compute_fibonacci_levels(prior_high, crash_low)
        measured_moves = self.compute_measured_move_target(prior_high, crash_low)

        current_price_vs_fib = None
        for fib_name, fib_price in sorted(fib_levels.items(), key=lambda x: x[1]):
            if current_price < fib_price:
                current_price_vs_fib = fib_name
                break
        if current_price_vs_fib is None:
            current_price_vs_fib = 'above_all'

        recovery_score = self._compute_composite_score(
            max_drawdown, recovery_pct, upside_to_high,
            higher_lows, vol_trend, batting_avg,
            current_rsi, macd_positive, sma20_above_sma50,
            vol_trend > self.min_rvol_recovery
        )

        status = self._classify_recovery_status(
            recovery_pct, upside_to_high, higher_lows,
            sma20_above_sma50, days_since_cross, macd_rising
        )

        return {
            'prior_high': round(prior_high, 2),
            'crash_low': round(crash_low, 2),
            'max_drawdown_pct': round(max_drawdown, 2),
            'recovery_pct': round(recovery_pct, 2),
            'upside_to_high_pct': round(upside_to_high, 2),
            'days_since_low': days_since_low,
            'above_sma20': above_sma20,
            'above_sma50': above_sma50,
            'sma20_above_sma50': sma20_above_sma50,
            'days_since_sma_cross': days_since_cross,
            'rsi': round(current_rsi, 2),
            'rsi_trending_up': rsi_trending,
            'macd_positive': macd_positive,
            'macd_rising': macd_rising,
            'higher_lows_count': higher_lows,
            'volume_trend_ratio': round(vol_trend, 2),
            'rvol': round(rvol, 2),
            'batting_avg_5d': batting_avg,
            'z_score_5d': round(z_score_5d, 2),
            'current_price': round(current_price, 2),
            'fib_level': current_price_vs_fib,
            'fib_levels': {k: round(v, 2) for k, v in fib_levels.items()},
            'measured_move_50': round(measured_moves['measured_move_50pct'], 2),
            'measured_move_100': round(measured_moves['measured_move_100pct'], 2),
            'recovery_score': round(recovery_score, 2),
            'status': status,
        }

    def _compute_composite_score(self, max_dd, recovery_pct, upside,
                                  higher_lows, vol_trend, batting_avg,
                                  rsi, macd_pos, sma_cross, vol_confirm):
        """
        Composite score 0-100. Higher = better risk/reward.
        """
        score = 0

        dd_score = min(abs(max_dd) / 60 * 20, 20)
        score += dd_score

        room_score = min(upside / 100 * 25, 25)
        score += room_score

        recovery_stage_score = 0
        if 15 <= recovery_pct <= 40:
            recovery_stage_score = 15
        elif 40 < recovery_pct <= 60:
            recovery_stage_score = 10
        elif recovery_pct < 15:
            recovery_stage_score = 5
        score += recovery_stage_score

        score += higher_lows * 4

        score += min(vol_trend / 2 * 8, 8)

        score += batting_avg * 2

        if macd_pos:
            score += 3
        if sma_cross:
            score += 3
        if vol_confirm:
            score += 4

        return min(score, 100)

    def _classify_recovery_status(self, recovery_pct, upside, higher_lows,
                                   sma_cross, days_since_cross, macd_rising):
        """
        Classify the recovery stage.
        """
        if recovery_pct < 20 and higher_lows >= 1 and macd_rising:
            return "EARLY REVERSAL"
        elif recovery_pct < 40 and sma_cross:
            return "DEEP RECOVERY"
        elif recovery_pct < 60 and higher_lows >= 2:
            return "MID RECOVERY"
        elif upside < 30:
            return "LATE RECOVERY"
        elif recovery_pct < 25 and not macd_rising:
            return "DEAD CAT BOUNCE"
        else:
            return "MID RECOVERY"
