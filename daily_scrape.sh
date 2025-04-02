#!/bin/bash

# Convert Jupyter notebooks to Python scripts in their respective directories

jupyter nbconvert --to script rotation_scrape.ipynb
jupyter nbconvert --to script clip_scrape.ipynb

jupyter nbconvert --to script clip_merge2.ipynb
jupyter nbconvert --to script generate_opp.ipynb
jupyter nbconvert --to script clip_fix.ipynb
jupyter nbconvert --to script clip_fix2.ipynb

python rotation_scrape.py
python clip_scrape.py
python clip_merge2.py
python clip_fix.py
python async_loop.py 
python async_loop.py 
python async_loop.py 
python clip_fix2.py
python generate_opp.py
