#!/usr/bin/env bash

# Profiles the given python script 
# and generates a call graph profoling data for each node in the graph

if [ -z "$1" ]
  then
    echo "No argument supplied"
    echo "Usage: profile File1 [args ...]"
    exit 1
fi

python -m cProfile $@ | ~/.local/bin/gprof2dot | dot -Tsvg -o $script.svg

rm -rf result

