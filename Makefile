.PHONY: setup run-local compile-semantic demo test lint synth deploy-aws destroy-aws clean

PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PIP := $(if $(wildcard .venv/bin/pip),.venv/bin/pip,pip3)
RUFF := $(if $(wildcard .venv/bin/ruff),.venv/bin/ruff,ruff)
PYTEST := $(if $(wildcard .venv/bin/pytest),.venv/bin/pytest,pytest)
STREAMLIT := $(if $(wildcard .venv/bin/streamlit),.venv/bin/streamlit,streamlit)
CDK := $(if $(wildcard node_modules/.bin/cdk),node_modules/.bin/cdk,npx cdk)

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt
	npm install --no-audit --no-fund
	cp -n .env.example .env || true
	.venv/bin/python scripts/generate_data.py

compile-semantic:
	$(PYTHON) src/semantic_compiler.py

run-local:
	$(PYTHON) scripts/generate_data.py
	$(PYTHON) src/semantic_compiler.py
	DEMO_MODE=true $(PYTHON) src/local_runner.py

demo:
	DEMO_MODE=true $(STREAMLIT) run app/executive_bi_cockpit.py

lint:
	$(RUFF) check .

synth:
	$(CDK) synth --app "$(PYTHON) infra/app.py" --quiet

test:
	$(RUFF) check .
	$(PYTEST) -v
	$(CDK) synth --app "$(PYTHON) infra/app.py" --quiet

deploy-aws:
	$(CDK) bootstrap --app "$(PYTHON) infra/app.py"
	$(CDK) deploy --all --require-approval never --app "$(PYTHON) infra/app.py"
	$(PYTHON) scripts/export_env.py

destroy-aws:
	$(CDK) destroy --all --force --app "$(PYTHON) infra/app.py"

clean:
	rm -rf cdk.out .pytest_cache .ruff_cache data/landing data/lakehouse data/warehouse
