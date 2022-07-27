from pathlib import Path

import click
import pandas as pd

from stock_price_simulator.simulate import simulate


@click.command()
@click.option(
    "--ticker",
)
@click.option(
    "--filepath",
)
def cli(ticker, filepath):
    output_dir = "data/output"
    output_filename = f"Simulated_{ticker}.csv"

    price_df = pd.read_csv(filepath)
    simulated_price_df = simulate(ticker, price_df)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    simulated_price_df.to_csv(Path(output_dir) / output_filename, index=False)


if __name__ == "__main__":
    cli()
