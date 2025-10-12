init-env:
	python3 -m venv venv
	. venv/bin/activate && pip install -r requirements.txt
.PHONY: init-env

activate-env:
	@echo "To activate the virtual environment, run: source venv/bin/activate"
.PHONY: activate-env

deactivate-env:
	@echo "To deactivate the virtual environment, run: deactivate"
.PHONY: deactivate-env

test:
	pytest 
.PHONY: test

test-verbose:
	pytest -v -ss
.PHONY: test-verbose