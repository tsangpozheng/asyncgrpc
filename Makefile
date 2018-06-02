



publish:
	python3 -m pip install --upgrade setuptools wheel
	python3 setup.py sdist bdist_wheel
	python3 -m pip install --upgrade twine
	twine upload --repository-url https://upload.pypi.org/legacy/ dist/*

install:
	python3 -m pip install --index-url https://test.pypi.org/simple/ asyncgrpc

venv-create:
	python3 -m pip install virtualenv
	python3 -m venv venv
	source venv/bin/activate
	python3 -m pip install --upgrade pip

venv-dump-dev:
	pip freeze > dev-requirements.txt

venv-dump:
	pipreq .

venv-restore:
	pip install -r requirements.txt
