# config/setting.py

import os
import pandas as pd
import numpy as np
import time
from datetime import datetime
import yfinance as yf
import io
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
