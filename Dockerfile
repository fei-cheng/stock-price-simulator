FROM python:3.9-buster

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN pip install poetry

WORKDIR /app
COPY . .

# Install python dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

CMD ["/usr/local/bin/poetry", "run", "python", "stock_price_simulator/run.py"]
