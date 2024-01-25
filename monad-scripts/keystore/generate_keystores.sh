#!/bin/bash

if [[ $1 == "install" ]]; then
    echo "Installing requirements"
    pip3 install -r requirements.txt
fi

echo "Generating keystores"
python3 generate_keystores.py
