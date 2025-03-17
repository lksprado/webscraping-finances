import pandas as pd 
import requests
import os 
from bs4 import BeautifulSoup as bs 
import logging 
from requests.exceptions import RequestException
import json 
import time


logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("extraction.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)




def get_data(url, timeout = 10):

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        time.sleep(1)
        return response.text
    except requests.RequestException as e:
        logger.error(f"Erro ao acessar {url}: {e}")
        return None

def parse_html(html):
    try:
        soup = bs(html, 'html.parser')
        return soup
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        return None 

def get_last_page(data):
    data_json = json.loads(data) 
    last_page = data_json.get('last_page','Not found')
    return last_page 

def save_json(data, page, path):
    data_json = json.loads(data)
    filename = f'cdb_{page}.json'
    file_path = os.path.join(path, filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data_json, f, indent=4, ensure_ascii=False)

def run_cdbs():
    page = 1
    FIRST_URL = 'https://investidor10.com.br/api/fixed-incomes/cdb?page=1'
    PATH = 'data/'
    first_data = get_data(FIRST_URL)
    if first_data:
        last_page = get_last_page(first_data)
        while page <= last_page:
            url = f'https://investidor10.com.br/api/fixed-incomes/cdb?page={page}'
            data = get_data(url)
            if data:        
                save_json(data, page, PATH)
            page += 1 

def get_data_from_html(soup_html):
    
    soup = soup_html
    cdi_section = soup.find("a", class_="index-card", href="/indices/cdi/")
    if cdi_section:
        cdi_value = cdi_section.find("strong", class_ = "variation")
        if cdi_value:
            cdi = cdi_value.text.strip()

    ipca_section = soup.find("a", class_="index-card", href="/indices/ipca/")
    if ipca_section:
        ipca_value = ipca_section.find_all("strong", class_ = "variation")
        if len(ipca_value) > 1:
            ipca = ipca_value[1].text.strip()

    kpi = {
        "cdi_yearly":cdi,
        "ipca_12":ipca
    }
    return kpi

def run_kpi():
    URL = 'https://investidor10.com.br/indices/'
    html = get_data(URL)
    parsed = parse_html(html)    
    kpis = get_data_from_html(parsed)
    kpis = pd.DataFrame(kpis, index=[0])
    kpis.to_csv('data/csv/kpis.csv',sep=";", index=False)    
        
if __name__ == "__main__":
    run_kpi()
