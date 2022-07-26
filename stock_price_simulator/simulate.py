from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal


def simulate(ticker, price_df, trading_dates=None, num_of_simulation=5):
    # Calculates the percentage change between the current and a prior price
    pct_change = price_df["Adj Close"].pct_change()

    # Assume that the daily stock returns follow a Normal Distribution
    pct_change_std = pct_change.std()

    if trading_dates is None:
        # Simulate the stock price for the following year
        start_time = price_df["Date"].max()
        end_time = start_time.replace(year=start_time.year + 1)

        start_date = start_time.strftime("%Y-%m-%d")
        end_date = end_time.strftime("%Y-%m-%d")

        trading_dates = get_trading_dates(start_date, end_date)

    ticker_price_list = []
    for simulation_id in range(num_of_simulation):
        for (idx, dt) in enumerate(trading_dates):
            if idx == 0:
                prev_price = price_df["Adj Close"].iat[-1]
            else:
                prev_price = ticker_price_list[-1]["price"]
                
            price = prev_price * (1 + np.random.normal(0, pct_change_std))
            ticker_price_list.append(
                {
                    "ticker": ticker,
                    "simulation_id": simulation_id,
                    "date": dt,
                    "price": price,
                }
            )
    simulated_price_df = pd.DataFrame(ticker_price_list)

    return simulated_price_df


def get_trading_dates(start_date, end_date):
    # Create a calendar
    nyse = mcal.get_calendar("NYSE")

    # Get NYSE trading dates
    return nyse.schedule(start_date, end_date).index.to_list()
