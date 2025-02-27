#!/bin/bash

# Convert Jupyter notebooks to Python scripts in their respective directories

jupyter nbconvert --to script rotation_scrape.ipynb
jupyter nbconvert --to script clip_scrape.ipynb

jupyter nbconvert --to script clip_merge2.ipynb
jupyter nbconvert --to script generate_opp.ipynb
python rotation_scrape.py
python clip_scrape.py
python clip_merge2.py
python generate_opp.py
