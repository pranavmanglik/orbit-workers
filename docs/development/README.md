# Orbit Workers: development

This package declares support for `>=3.11,<3.15` in `pyproject.toml`. That declaration is distinct from evidence that hosted CI ran.

Use the checks for each language manifest present in this repository:

### Python package

```bash
python -m pip install -e '.[dev]' build
pip-audit --skip-editable --progress-spinner off
pytest
ruff check src tests
ruff format --check src tests
mypy
```

The local CPython 3.11.14, 3.12.14, 3.13.15, and 3.14.7 runs each passed 26 tests, lint,
formatting, strict type checks, dependency audit, and wheel/source builds. Hosted CI and broker-backed
deployment validation remain separate release gates.

Optional SDKs and live-service tests are described by the package README. Do not run an opt-in live test against a shared or production service unless its own documentation explicitly supports that use.

## Contribution expectations

Add focused regression coverage for behavior changes. Update the README and relevant guide when a contract, configuration option, security property, lifecycle rule, or operational assumption changes. Report the exact versions and services exercised; local checks do not imply hosted CI or release validation.
