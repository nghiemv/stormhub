rm -rf build
sphinx-apidoc -o docs/source stormhub
sphinx-build -M html docs/source docs/build -c docs/source