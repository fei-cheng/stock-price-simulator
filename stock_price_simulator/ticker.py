import yfinance as yf


def download_ticker_prices():
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

    print("Download ticker data...")

    ticker_price_df = {}
    for ticker in ticker_symbols:
        print(f"{ticker}...")
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

    return ticker_price_df
