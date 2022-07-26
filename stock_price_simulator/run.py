from multiprocessing import Pool, cpu_count

import pandas as pd
import yfinance as yf

from simulate import simulate


def run():
    ticker_symbols = [
        "AMD",
        "AAPL",
        "AMZN",
        "META",
        "MSFT",
        "NVDA",
        "PYPL",
        "TSLA",
        "TWTR",
    ]

    ticker_price_df = {}
    for ticker in ticker_symbols:
        price_df = yf.download(
            ticker,
            start="2020-01-01",
            end="2021-12-31",
        )
        if price_df.empty:  # in case failed download
            continue
        price_df = price_df.reset_index()
        price_df.insert(0, "ticker", ticker)
        ticker_price_df[ticker] = price_df

    number_of_processes = max(1, cpu_count() - 1)
    pool = Pool(processes=number_of_processes)

    async_results = []
    for (ticker, price_df) in ticker_price_df.items():
        p_result = pool.apply_async(simulate, args=(ticker, price_df))
        async_results.append(p_result)

    pool.close()
    pool.join()

    result_dfs = [result.get() for result in async_results]
    simulated_price_df = pd.concat(result_dfs, ignore_index=True)

    return simulated_price_df


if __name__ == "__main__":
    simulated_price_df = run()

    print(simulated_price_df)
