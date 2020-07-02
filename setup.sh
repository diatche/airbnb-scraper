#!/bin/bash

cd "$(dirname "${BASH_SOURCE[0]}")"

source ./env/bin/activate
python -m pip install --upgrade pip

# Install package manager if needed
python -m pip install pipreqs

# Install required packages
python -m pip install -e .
