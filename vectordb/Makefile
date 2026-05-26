# Setup ENV
SHELL := /bin/bash
include .env
export

unit_test:
	PYTHONPATH=./ pytest tests --doctest-modules --junitxml=junit/test-results.xml --cov=./ --cov-report=xml:coverage.xml --cov-report=html:htmlcov --cov-fail-under=70
loadtest_master:
	locust -f load_test/locust.py --config load_test/locust.conf --master
loadtest_worker:
	locust -f load_test/locust.py --worker
install-dev:
	pip install -r requirements-dev.txt
