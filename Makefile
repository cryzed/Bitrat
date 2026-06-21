.ONESHELL:
.SILENT:
SHELL := /bin/sh
.PHONY: check format lint typecheck

check: format lint typecheck

format:
	uv run ruff format bitrat/
	uv run ruff check bitrat/ --fix --unsafe-fixes

lint:
	uv run ruff check bitrat/

typecheck:
	uv run basedpyright
