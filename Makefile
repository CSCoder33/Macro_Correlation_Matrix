PY=python

.PHONY: setup run run-levels run-returns clean

setup:
	$(PY) -m pip install --upgrade pip
	pip install -r requirements.txt

run: run-levels run-returns

run-levels:
	$(PY) -m src.pipeline_runner.main --mode levels

run-returns:
	$(PY) -m src.pipeline_runner.main --mode returns

clean:
	rm -f reports/figures/* reports/animations/* data/processed/monthly_*.csv

