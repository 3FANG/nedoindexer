VENV=.venv
VENV_ACTIVATE=. .venv/bin/activate

setup: venv-prepare load-dependences 

venv-prepare:
	sudo apt install python3-venv
	python3 -m venv $(VENV)
	$(VENV_ACTIVATE) && pip install -U pip setuptools
	$(VENV_ACTIVATE) && pip install poetry
	
load-dependences:
	$(VENV_ACTIVATE) && poetry install
	$(VENV_ACTIVATE) && poetry shell

start:
	$(VENV_ACTIVATE) && python3 -m nedoindexer

.PHONY: venv-prepare