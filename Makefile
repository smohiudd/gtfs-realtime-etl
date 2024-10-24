.PHONEY:
	install-dev
	install
	lint
	format
	diff
	deploy
	destroy

install-dev: install
	pip install -r requirements-dev.txt

install:
	pip install --upgrade pip
	pip install -r requirements.txt

lint:
	python -m ruff check
	python -m mypy .

format:
	python -m ruff check --fix
	python -m ruff check --select I --fix
	python -m ruff format

diff:
	cdk diff

STACKS="--all"
ARGS=""

deploy:
	cdk deploy --outputs-file ./gtfs-realtime-etl-cdk-outputs.json $(STACKS) $(ARGS)

destroy:
	cdk destroy $(STACKS)
