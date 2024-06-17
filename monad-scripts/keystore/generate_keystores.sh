#!/bin/bash

if [[ $1 == "install" ]]; then
    echo "Installing requirements"
    python3 -m venv venv
    source venv/bin/activate
    pip3 install -r requirements.txt
    shift
fi

echo "Generating keystores"
python3 generate_keystores.py "$@"
