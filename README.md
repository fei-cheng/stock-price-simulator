# Stock Price Simulator

This is an example project of:
- [Poetry](https://python-poetry.org/)
- [Python multiprocessing](https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing)
- [Azure Batch Service](https://docs.microsoft.com/en-us/azure/batch/batch-technical-overview)
- [Monte Carlo Simulation](https://de.wikipedia.org/wiki/Monte-Carlo-Simulation)

The simulation result might be not good as it is actually not the real focus of this project.

**DON'T use it as a basis for buying any stock!!!**

## Prerequisites
- Python 3.8+
- Poetry
- Azure Batch account (only if you use Azure Batch)
- Azure Storage account (only if you use Azure Batch)

**Note**: Azure accounts are not needed if you run the simulation using multiprocessing locally instead of Azure Batch.

## Installation
1. Install poetry(if it doesn't exist): `pip install poetry`
2. Install dependencies: `poetry install`

## Run

### Use multiprocessing
```
poetry run python stock_price_simulator/run.py
```

### Use Azure Batch
```
poetry run python az_batch/run.py
```

## Docker
You can also use docker to run the simulation, so that you can skip the poetry installation which is frustrating sometime.
1. Build the image:

    ```
    docker build -t stock_price_simulator .
    ```
2. Run in a container:

    ```
    docker run stock_price_simulator
    ```
