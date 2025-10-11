init-env:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt
.PHONY: init-env

activate-env:
	source venv/bin/activate	
.PHONY: activate-env

test:
	pytest 
.PHONY: test

test-verbose:
	pytest -v
.PHONY: test-verbose