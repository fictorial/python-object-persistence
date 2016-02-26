init:
	pip install -r requirements.txt

test:
	PYTHONPATH=$(PWD) py.test -v --cov=persistent --cov-report=term-missing tests.py
