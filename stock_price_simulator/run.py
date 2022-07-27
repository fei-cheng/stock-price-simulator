from multiprocessing import Pool, cpu_count

import pandas as pd

from stock_price_simulator.simulate import simulate
from stock_price_simulator.ticker import download_ticker_prices


def run():
    ticker_price_df = download_ticker_prices()

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
