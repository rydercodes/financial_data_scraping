import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s')

project_name= "financial_data"

list_of_files = [
    "notebooks/1-companies_details.ipynb",
    "notebooks/2-historical_data_details.ipynb",
    "notebooks/3-analyst_price_target.ipynb",
    "notebooks/4-marketcap_details.ipynb",
    "notebooks/5-load-data.ipynb",
    "src/companies_details.py",
    "src/historical_data_details.py",
    "src/analyst_price_target.py",
    "src/marketcap_details.py",
    "src/load-data.py",
    "data/test",
    "config/setting.py",
    "requirements.txt",
    "setup.py",
    "main.py"
    ]

for filepath in list_of_files:
    filepath = Path(filepath)
    if not filepath.parent.exists():
        logging.info(f"Creating directory: {filepath.parent}")
        os.makedirs(filepath.parent)
    else:
        logging.info(f"Directory already exists: {filepath.parent}")
    if not filepath.exists():
        logging.info(f"Creating file: {filepath}")
        filepath.touch()
    else:
        logging.info(f"File already exists: {filepath}")


