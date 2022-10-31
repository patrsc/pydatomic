#!/bin/bash
pdm install
export VERSION=$(pdm run python getversion.py)
pdm run pdoc -t . pydatomic -o html/$VERSION
