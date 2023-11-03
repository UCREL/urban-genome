#!/bin/bash

# Install any dependencies listed in our pyproject.toml
poetry install

# https://spacy.io/models/en#en_core_web_lg
python -m spacy download en_core_web_lg