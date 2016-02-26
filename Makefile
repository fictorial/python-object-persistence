init:
	pip install -r requirements.txt

test:
	PYTHONPATH=$(PWD) py.test -v --cov=persistent --cov-report=term-missing tests.py

gh:
	git push origin master

pypi:
	rm -f dist/python-object-persistence*.tar.gz
	python setup.py sdist
	twine upload dist/python-object-persistence*.tar.gz

.PHONY: all
