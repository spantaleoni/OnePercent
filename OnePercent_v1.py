from strategies.BaseStrategy import BaseStrategy
import numpy as np
import pandas as pd
import os

class OnePercent(BaseStrategy):
    """
    Weekly TQQQ strategy with daily timeframe execution:
      - Entry: go long TQQQ at Monday (or first trading day of week) open, full allocation
      - Type A: if daily close ≥ +8.1% or +7.0% from entry, exit next day at open
      - Type B: no TP hit, week ends positive -> exit at Friday close (flat next open)
      - Type C: if daily close < entry price, exit next day at open (break-even modeled as flat)
      - Type D: no other exit, week ends negative -> exit at Friday close (flat next open)

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
        self.exit_next_open_pending = False  # used for Type A/C exits
        self.exit_eod_pending = False        # used for Type B/D exits

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

            # Handle pending exits
            if self.exit_next_open_pending:
                self.in_position = False
                self.exit_next_open_pending = False
                self.entry_price = None
                self.entry_date = None
                self.entry_week = None

            elif self.exit_eod_pending:
                self.in_position = False
                self.exit_eod_pending = False
                self.entry_price = None
                self.entry_date = None
                self.entry_week = None

            elif self.in_position:
                today_close = data[("Close", "TQQQ")].iloc[i]

                # --- Type A: Take Profit ---
                if today_close >= self.entry_price * 1.081 or today_close >= self.entry_price * 1.07:
                    self.exit_next_open_pending = True
                    allocation.loc[date, "TQQQ"] = 1.0

                # --- Type C: Break-even ---
                elif today_close < self.entry_price:
                    self.exit_next_open_pending = True
                    allocation.loc[date, "TQQQ"] = 1.0

                else:
                    # --- Type B/D: End-of-week exit ---
                    next_date = index_list[i + 1] if (i + 1) < n else None
                    if self._weekly_last_trading_day(date, next_date):
                        self.exit_eod_pending = True
                        allocation.loc[date, "TQQQ"] = 1.0
                    else:
                        allocation.loc[date, "TQQQ"] = 1.0

            else:
                if self._is_new_week(prev_date, date):
                    self.in_position = True
                    self.entry_price = data[("Open", "TQQQ")].iloc[i]
                    self.entry_date = date
                    iso = date.isocalendar()
                    self.entry_week = (iso.year, iso.week)
                    allocation.loc[date, "TQQQ"] = 1.0
                else:
                    allocation.iloc[i, :] = 0.0

            prev_date = date

        return allocation
