# conftest.py — ensures this service's directory is first on sys.path
# so that pytest from the repo root imports the right main.py / normalizer.py etc.
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
