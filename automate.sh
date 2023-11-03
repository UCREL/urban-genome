#!/bin/bash

pushd /opt/genome.johnvidler.co.uk
docker run --rm -v ${PWD}:/docs squidfunk/mkdocs-material build
popd
