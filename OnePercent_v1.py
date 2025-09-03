from strategies.BaseStrategy import BaseStrategy
import numpy as np
import pandas as pd
import os

class OnePercent(BaseStrategy):
    """
    Weekly TQQQ strategy with daily timeframe execution (no lookahead):
      - Entry: go long TQQQ at Monday (or first trading day of week) open, full allocation
      - Type A: if prior daily close ≥ +8.1% or +7.0% from entry, exit today at open
      - Type B: no TP hit, prior day was last of week and week closed positive -> exit today at open
      - Type C: if prior daily close < entry price, exit today at open (break-even modeled as flat)
      - Type D: no other exit, prior day was last of week and week closed negative -> exit today at open

    No volatility-based switching. Always trades TQQQ.
    """

    def __init__(self):
        super().__init__(
            strategy_name="OnePercent",
            strategy_version="1.0",
            MacroTickers=[],
            tickers=["TQQQ", "SPY"],
        )
        self.trading_assets = ["TQQQ", "SPY"]
        self.Benchmark = "SPY"
        self.mrkt = "SPY"

        self.WARMUP = 20  # buffer for clean start

        # State
        self.in_position = False
        self.entry_price = None
        self.entry_date = None
        self.entry_week = None

        self.strategy_file = os.path.basename(__file__)

    @staticmethod
    def _is_new_week(prev_date: pd.Timestamp, curr_date: pd.Timestamp) -> bool:
        if prev_date is None:
            return True
        prev_iso = prev_date.isocalendar()
        curr_iso = curr_date.isocalendar()
        return (curr_iso.year, curr_iso.week) != (prev_iso.year, prev_iso.week)

    @staticmethod
    def _weekly_last_trading_day(date: pd.Timestamp, next_date: pd.Timestamp | None) -> bool:
        if next_date is None:
            return True
        curr_iso = date.isocalendar()
        next_iso = next_date.isocalendar()
        return (curr_iso.year, curr_iso.week) != (next_iso.year, next_iso.week)

    def run_strategy(self, data: pd.DataFrame, MacroData: pd.DataFrame):
        data = data.copy()
        data = data.ffill().fillna(0)

        for t in ["TQQQ", "SPY"]:
            for f in ["Open", "Close"]:
                if (f, t) not in data.columns:
                    raise KeyError(f"Missing column: ({f}, {t}) in input data")

        allocation = pd.DataFrame(index=data.index, columns=self.trading_assets, dtype=float)
        allocation.loc[:, self.trading_assets] = 0.0

        prev_date = None
        index_list = data.index.tolist()
        n = len(index_list)

        for i, date in enumerate(index_list):
            if i < self.WARMUP:
                prev_date = date
                continue

            allocation.iloc[i, :] = 0.0

            if self.in_position:
                # Use yesterday's close to decide
                yesterday_close = data[("Close", "TQQQ")].iloc[i-1]

                # --- Type A: Take Profit ---
                if yesterday_close >= self.entry_price * 1.081 or yesterday_close >= self.entry_price * 1.07:
                    self.in_position = False
                    self.entry_price = None
                    self.entry_date = None
                    self.entry_week = None

                # --- Type C: Break-even ---
                elif yesterday_close < self.entry_price:
                    self.in_position = False
                    self.entry_price = None
                    self.entry_date = None
                    self.entry_week = None

                else:
                    # --- Type B/D: End-of-week exit ---
                    next_date = index_list[i] if (i) < n else None  # today is considered "next day" of yesterday
                    if self._weekly_last_trading_day(index_list[i-1], next_date):
                        self.in_position = False
                        self.entry_price = None
                        self.entry_date = None
                        self.entry_week = None

            # Entry check if flat
            if not self.in_position:
                if self._is_new_week(prev_date, date):
                    self.in_position = True
                    self.entry_price = data[("Open", "TQQQ")].iloc[i]
                    self.entry_date = date
                    iso = date.isocalendar()
                    self.entry_week = (iso.year, iso.week)
                    allocation.loc[date, "TQQQ"] = 1.0
            else:
                allocation.loc[date, "TQQQ"] = 1.0

            prev_date = date

        return allocation
