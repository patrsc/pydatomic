#!/bin/bash
git tag v$(pdm run python getversion.py) && git push --tags
