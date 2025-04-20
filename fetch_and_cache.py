#!/usr/bin/env python3
"""
fetch_and_cache.py - Скрипта за собирање на цени од супермаркети во Македонија

Оваа скрипта:
1. Собира локации на пазари за КАМ, Тинекс, Веро и Стокомак
2. Собира податоци за цени од секој пазар
3. Зачувува податоци во CSV и JSON формати во кеш директориуми со фолдери базирани на датум
"""

import os
import csv
import json
import time
import requests
import argparse
import logging
import io
import re
import tempfile
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import concurrent.futures

# За обработка на PDF
try:
    import PyPDF2
    import tabula
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logging.warning("PDF библиотеките не се достапни. Инсталирај PyPDF2, tabula-py и pdfplumber за обработка на PDF.")

# Поставување на логирање
logging.basicConfig(
    level=logging.DEBUG,  # Промени на DEBUG за детални логове
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Константи
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT
}
TIMEOUT = 15  # секунди
SLEEP_BETWEEN_REQUESTS = 1  # секунди

# Кеш директориуми
SCRIPT_DIR = Path(__file__).parent
ROOT_CACHE_DIR = SCRIPT_DIR.parent / "cache"
PROJECT_CACHE_DIR = SCRIPT_DIR / "cache"

def get_today_str():
    """Враќа денешен датум во формат YYYY-MM-DD"""
    return datetime.now().strftime("%Y-%m-%d")

def setup_cache_dirs():
    """Креира кеш директориуми ако не постојат"""
    today = get_today_str()
    
    ROOT_CACHE_DIR.mkdir(exist_ok=True)
    PROJECT_CACHE_DIR.mkdir(exist_ok=True)
    
    return today

def safe_request(url, headers=None, timeout=TIMEOUT, retries=3, method="get", **kwargs):
    """Извршува безбедна HTTP побара со повторувања и обработка на грешки"""
    headers = headers or HEADERS
    
    for attempt in range(retries):
        try:
            if method.lower() == "get":
                response = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            else:
                response = requests.post(url, headers=headers, timeout=timeout, **kwargs)
            
            response.raise_for_status()
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return response
        except requests.RequestException as e:
            logger.warning(f"Побарувањето не успеа (обид {attempt+1}/{retries}): {url}, {e}")
            if attempt == retries - 1:
                logger.error(f"Сите {retries} обиди не успеаја за {url}")
                raise
            time.sleep(SLEEP_BETWEEN_REQUESTS * (attempt + 1))

def fetch_tinex_markets():
    """Собира сите локации на Тинекс пазари"""
    logger.info("Собирање на Тинекс пазари...")
    url = 'http://ceni.tinex.mk/'
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    select = soup.find('select', {'name': 'org'})
    if not select:
        logger.error("Не е пронајден селектор за пазари на страницата на Тинекс")
        return []
    
    markets = [
        {
            'brand': 'Tinex',
            'id': int(opt['value']),
            'name': opt.text.strip(),
            'url': f"http://ceni.tinex.mk/?page=1&perPage=100&search=&org={opt['value']}"
        }
        for opt in select.find_all('option') if opt['value']
    ]
    logger.info(f"Пронајдени {len(markets)} Тинекс пазари")
    return markets

def fetch_kam_markets():
    """Собира сите локации на КАМ пазари"""
    logger.info("Собирање на КАМ пазари...")
    url = 'https://kam.com.mk/ceni-vo-marketi/'
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    markets = []
    for div in soup.select('.markets_wrap'):
        name_tag = div.find('h2')
        address_tag = div.find('p')
        url_tag = div.find('a')
        
        if name_tag and url_tag:
            market_id = url_tag['href'].rstrip('/').split('/')[-1]
            market = {
                'brand': 'KAM',
                'name': name_tag.text.strip(),
                'address': address_tag.text.strip() if address_tag else "",
                'url': url_tag['href'],
                'id': market_id
            }
            markets.append(market)
    
    logger.info(f"Пронајдени {len(markets)} КАМ пазари")
    return markets

def fetch_vero_markets():
    """Собира сите локации на Веро пазари"""
    logger.info("Собирање на Веро пазари...")
    url = 'https://pricelist.vero.com.mk/'
    response = safe_request(url)
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    markets = []
    for a in soup.select('a[href$=".html"]'):
        href = a.get('href', '')
        if href and href[0].isdigit():
            market = {
                'brand': 'Vero',
                'id': href.replace('.html', ''),
                'name': a.text.strip(),
                'url': f"{url}{href}"
            }
            markets.append(market)
    
    logger.info(f"Пронајдени {len(markets)} Веро пазари")
    return markets

def fetch_stokomak_markets():
    """Собира сите локации на Стокомак пазари"""
    logger.info("Собирање на Стокомак пазари...")
    url = 'https://stokomak.proverkanaceni.mk/'
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    select = soup.find('select', {'name': 'org'})
    if not select:
        logger.error("Не е пронајден селектор за пазари на страницата на Стокомак")
        return []
    
    markets = [
        {
            'brand': 'Stokomak',
            'id': int(opt['value']),
            'name': opt.text.strip(),
            'url': f"https://stokomak.proverkanaceni.mk/?page=1&perPage=100&search=&org={opt['value']}"
        }
        for opt in select.find_all('option') if opt['value']
    ]
    
    logger.info(f"Пронајдени {len(markets)} Стокомак пазари")
    return markets

def fetch_all_markets():
    """Собира сите пазари од сите брендови"""
    all_markets = []
    
    try:
        all_markets.extend(fetch_tinex_markets())
    except Exception as e:
        logger.error(f"Неуспешно собирање на Тинекс пазари: {e}")
    
    try:
        all_markets.extend(fetch_kam_markets())
    except Exception as e:
        logger.error(f"Неуспешно собирање на КАМ пазари: {e}")
    
    try:
        all_markets.extend(fetch_vero_markets())
    except Exception as e:
        logger.error(f"Неуспешно собирање на Веро пазари: {e}")
    
    try:
        all_markets.extend(fetch_stokomak_markets())
    except Exception as e:
        logger.error(f"Неуспешно собирање на Стокомак пазари: {e}")
    
    return all_markets

def fetch_tinex_prices(market):
    """Собира цени од Тинекс пазар со пагинација"""
    logger.info(f"Собирање на цени за Тинекс {market['name']} (ID: {market['id']})...")
    base_url = "http://ceni.tinex.mk/"
    products = []
    page = 1
    max_pages = 100
    
    # Проверка за вкупен број на страници на првата страница
    if page == 1:
        url = f"{base_url}?page=1&perPage=100&search=&org={market['id']}"
        response = safe_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        last_page_link = (
            soup.select_one(".pagination a:contains('Последна')") or
            soup.select_one(".pagination a:contains('Last')")
        )
        if last_page_link and 'page=' in last_page_link.get('href', ''):
            last_page_match = re.search(r'page=(\d+)', last_page_link['href'])
            if last_page_match:
                max_pages = min(int(last_page_match.group(1)), max_pages)
                logger.info(f"Детектирани {max_pages} вкупно страници за Тинекс {market['name']}")
    
    while page <= max_pages:
        url = f"{base_url}?page={page}&perPage=100&search=&org={market['id']}"
        logger.debug(f"Побарување на URL: {url}")
        
        try:
            response = safe_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = (
                soup.select("table.table tbody tr") or
                soup.select("table tbody tr") or
                soup.select("table tr")
            )
            if not rows:
                logger.warning(f"Не се пронајдени редови во табелата на страница {page} за Тинекс {market['name']}")
                logger.debug(f"HTML на страницата: {soup.prettify()[:1000]}...")
                break
            
            page_products = []
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    product = {
                        'market_id': market['id'],
                        'market_name': market['name'],
                        'brand': market['brand'],
                        'name': cells[0].text.strip(),
                        'unit': cells[1].text.strip(),
                        'price': cells[2].text.strip().replace("ден", "").strip(),
                        'date': get_today_str()
                    }
                    page_products.append(product)
            
            if not page_products:
                logger.info(f"Не се извлечени производи на страница {page} за Тинекс {market['name']}")
                break
            
            products.extend(page_products)
            logger.info(f"Собрани {len(page_products)} производи од страница {page} за Тинекс {market['name']}")
            
            total_items = None
            pagination_info = soup.select_one(".pagination-info")
            if pagination_info:
                info_text = pagination_info.text.strip()
                total_match = re.search(r'од\s+(\d+)|вкупно\s+(\d+)|total\s+(\d+)', info_text, re.IGNORECASE)
                if total_match:
                    total_items = int(total_match.group(1) or total_match.group(2) or total_match.group(3))
                    if len(products) >= total_items:
                        logger.info(f"Достигнати сите {total_items} производи за Тинекс {market['name']}")
                        break
            
            next_button = (
                soup.select_one(".pagination .page-item:not(.disabled) a[aria-label='Next']") or
                soup.select_one(".pagination a:contains('Следна')") or
                soup.select_one(".pagination a:contains('Next')") or
                soup.select_one(".pagination a[href*='page=" + str(page + 1) + "']")
            )
            if not next_button:
                logger.info(f"Достигната последна страница ({page}) за Тинекс {market['name']}")
                break
            
            page += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Грешка при собирање на страница {page} за Тинекс {market['name']}: {e}")
            break
        
        if page > max_pages:
            logger.warning(f"Достигнат максимум од {max_pages} страници за Тинекс {market['name']}")
    
    logger.info(f"Вкупно: Пронајдени {len(products)} цени за Тинекс {market['name']}")
    return products

def fetch_kam_prices(market):
    """Собира цени од КАМ пазар со подобрена поддршка за PDF"""
    logger.info(f"Собирање на цени за КАМ {market['name']}...")
    url = market['url']
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    products = []
    price_tables = soup.select(".ceni_table")
    
    for table in price_tables:
        rows = table.select("tr")
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) >= 3:
                product = {
                    'market_id': market.get('id', market['name']),
                    'market_name': market['name'],
                    'brand': market['brand'],
                    'name': cells[0].text.strip(),
                    'unit': cells[1].text.strip(),
                    'price': cells[2].text.strip().replace("ден", "").strip(),
                    'date': get_today_str()
                }
                products.append(product)
    
    if products:
        logger.info(f"Пронајдени {len(products)} цени во HTML за КАМ {market['name']}")
        return products
    
    pdf_url = None
    pdf_pattern = re.compile(r'(?:/pdf/|https?://kam\.com\.mk/pdf/)(\d+)\.pdf')
    for a in soup.find_all('a', href=True):
        href = a['href']
        match = pdf_pattern.search(href)
        if match:
            pdf_url = f"https://kam.com.mk/pdf/{match.group(1)}.pdf"
            logger.info(f"Пронајден кандидат PDF URL: {pdf_url}")
            break
    
    if not pdf_url:
        html_content = response.text
        match = pdf_pattern.search(html_content)
        if match:
            pdf_url = f"https://kam.com.mk/pdf/{match.group(1)}.pdf"
            logger.info(f"Пронајден PDF URL во HTML содржината: {pdf_url}")
    
    if pdf_url:
        logger.info(f"Обработка на PDF за КАМ {market['name']}: {pdf_url}")
        pdf_content = download_pdf(pdf_url)
        if pdf_content:
            text = extract_text_from_pdf(pdf_content)
            if text and any(keyword in text.lower() for keyword in ['цена', 'артикл', 'единица']):
                logger.info(f"PDF изгледа како ценовник, се обработува...")
                pdf_products = parse_kam_pdf_specialized(pdf_content, market)
                if pdf_products:
                    products.extend(pdf_products)
                    logger.info(f"Извлечени {len(pdf_products)} производи од PDF со специјализиран парсер")
                else:
                    pdf_products = parse_kam_pdf(pdf_content, market)
                    if pdf_products:
                        products.extend(pdf_products)
                        logger.info(f"Извлечени {len(pdf_products)} производи од PDF со стандарден парсер")
                    else:
                        pdf_products = parse_kam_pdf_fallback(pdf_content, market)
                        if pdf_products:
                            products.extend(pdf_products)
                            logger.info(f"Извлечени {len(pdf_products)} производи од PDF со резервен парсер")
                        else:
                            logger.warning(f"Сите методи за парсирање на PDF не успеаја за КАМ {market['name']}")
            else:
                logger.warning(f"PDF на {pdf_url} не изгледа како ценовник")
        else:
            logger.error(f"Неуспешно преземање на PDF за КАМ {market['name']}")
    else:
        logger.warning(f"Не е пронајден PDF ценовник за КАМ {market['name']}")
    
    logger.info(f"Вкупно: Пронајдени {len(products)} цени за КАМ {market['name']}")
    return products

def fetch_vero_prices(market):
    """Собира цени од Веро пазар со пагинација"""
    logger.info(f"Собирање на цени за Веро {market['name']} (ID: {market['id']})...")
    base_url = "https://pricelist.vero.com.mk/"
    products = []
    id_parts = market['id'].split('_')
    base_id = id_parts[0]
    page = 1
    max_pages = 100
    
    while page <= max_pages:
        page_url = f"{base_url}{base_id}_{page}.html"
        logger.debug(f"Побарување на URL: {page_url}")
        try:
            response = safe_request(page_url)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select("table tr")
            if len(rows) <= 1:
                logger.info(f"Не се пронајдени производи на страница {page} за Веро {market['name']}")
                logger.debug(f"HTML на страницата: {soup.prettify()[:1000]}...")
                break
            
            page_products = []
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    product = {
                        'market_id': market['id'],
                        'market_name': market['name'],
                        'brand': market['brand'],
                        'name': cells[0].text.strip(),
                        'unit': cells[1].text.strip(),
                        'price': cells[2].text.strip().replace("ден", "").strip(),
                        'date': get_today_str()
                    }
                    page_products.append(product)
            
            if not page_products:
                logger.info(f"Не се извлечени валидни производи на страница {page} за Веро {market['name']}")
                break
                
            products.extend(page_products)
            logger.info(f"Собрани {len(page_products)} производи од страница {page} за Веро {market['name']}")
            
            page += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"Достигнат крај на страниците за Веро {market['name']} на страница {page}")
                break
            logger.error(f"HTTP грешка при собирање на страница {page} за Веро: {e}")
            break
        except Exception as e:
            logger.error(f"Грешка при собирање на страница {page} за Веро: {e}")
            break
            
        if page > max_pages:
            logger.warning(f"Достигнат максимум од {max_pages} страници за Веро {market['name']}")
    
    logger.info(f"Вкупно: Пронајдени {len(products)} цени за Веро {market['name']}")
    return products

def fetch_stokomak_prices(market):
    """Собира цени од Стокомак пазар со пагинација"""
    logger.info(f"Собирање на цени за Стокомак {market['name']} (ID: {market['id']})...")
    base_url = "https://stokomak.proverkanaceni.mk/"
    products = []
    page = 1
    max_pages = 100
    
    # Проверка за вкупен број на страници на првата страница
    if page == 1:
        url = f"{base_url}?page=1&perPage=100&search=&org={market['id']}"
        response = safe_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        last_page_link = (
            soup.select_one(".pagination a:contains('Последна')") or
            soup.select_one(".pagination a:contains('Last')")
        )
        if last_page_link and 'page=' in last_page_link.get('href', ''):
            last_page_match = re.search(r'page=(\d+)', last_page_link['href'])
            if last_page_match:
                max_pages = min(int(last_page_match.group(1)), max_pages)
                logger.info(f"Детектирани {max_pages} вкупно страници за Стокомак {market['name']}")
    
    while page <= max_pages:
        # Забелешка: Ако search= не е потребен за Стокомак, промени во:
        # url = f"{base_url}?org={market['id']}&page={page}&perPage=100"
        url = f"{base_url}?page={page}&perPage=100&search=&org={market['id']}"
        logger.debug(f"Побарување на URL: {url}")
        
        try:
            response = safe_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = (
                soup.select("table.table tbody tr") or
                soup.select("table tbody tr") or
                soup.select("table tr")
            )
            if not rows:
                logger.warning(f"Не се пронајдени редови во табелата на страница {page} за Стокомак {market['name']}")
                logger.debug(f"HTML на страницата: {soup.prettify()[:1000]}...")
                break
            
            page_products = []
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    product = {
                        'market_id': market['id'],
                        'market_name': market['name'],
                        'brand': market['brand'],
                        'name': cells[0].text.strip(),
                        'unit': cells[1].text.strip(),
                        'price': cells[2].text.strip().replace("ден", "").strip(),
                        'date': get_today_str()
                    }
                    page_products.append(product)
            
            if not page_products:
                logger.info(f"Не се извлечени производи на страница {page} за Стокомак {market['name']}")
                break
            
            products.extend(page_products)
            logger.info(f"Собрани {len(page_products)} производи од страница {page} за Стокомак {market['name']}")
            
            total_items = None
            pagination_info = soup.select_one(".pagination-info")
            if pagination_info:
                info_text = pagination_info.text.strip()
                total_match = re.search(r'од\s+(\d+)|вкупно\s+(\d+)|total\s+(\d+)', info_text, re.IGNORECASE)
                if total_match:
                    total_items = int(total_match.group(1) or total_match.group(2) or total_match.group(3))
                    if len(products) >= total_items:
                        logger.info(f"Достигнати сите {total_items} производи за Стокомак {market['name']}")
                        break
            
            next_button = (
                soup.select_one(".pagination .page-item:not(.disabled) a[aria-label='Next']") or
                soup.select_one(".pagination a:contains('Следна')") or
                soup.select_one(".pagination a:contains('Next')") or
                soup.select_one(".pagination a[href*='page=" + str(page + 1) + "']")
            )
            if not next_button:
                logger.info(f"Достигната последна страница ({page}) за Стокомак {market['name']}")
                break
            
            page += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Грешка при собирање на страница {page} за Стокомак {market['name']}: {e}")
            break
        
        if page > max_pages:
            logger.warning(f"Достигнат максимум од {max_pages} страници за Стокомак {market['name']}")
    
    logger.info(f"Вкупно: Пронајдени {len(products)} цени за Стокомак {market['name']}")
    return products

def fetch_market_prices(market):
    """Собира цени за даден пазар врз основа на брендот"""
    brand = market['brand'].lower()
    
    try:
        if brand == 'tinex':
            return fetch_tinex_prices(market)
        elif brand == 'kam':
            return fetch_kam_prices(market)
        elif brand == 'vero':
            return fetch_vero_prices(market)
        elif brand == 'stokomak':
            return fetch_stokomak_prices(market)
        else:
            logger.warning(f"Непознат бренд: {brand}")
            return []
    except Exception as e:
        logger.error(f"Грешка при собирање на цени за {market['name']} ({brand}): {e}")
        return []

def save_to_csv(data, filename):
    """Зачувува податоци во CSV датотека"""
    if not data:
        logger.warning(f"Нема податоци за зачувување во {filename}")
        return False
    
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Зачувани {len(data)} записи во {filename}")
        return True
    except Exception as e:
        logger.error(f"Грешка при зачувување на CSV {filename}: {e}")
        return False

def save_to_json(data, filename):
    """Зачувува податоци во JSON датотека"""
    if not data:
        logger.warning(f"Нема податоци за зачувување во {filename}")
        return False
    
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Зачувани {len(data)} записи во {filename}")
        return True
    except Exception as e:
        logger.error(f"Грешка при зачувување на JSON {filename}: {e}")
        return False

def fetch_all_prices(markets, max_workers=4):
    """Собира цени за сите пазари со паралелизација"""
    all_products = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_market = {executor.submit(fetch_market_prices, market): market for market in markets}
        
        for future in concurrent.futures.as_completed(future_to_market):
            market = future_to_market[future]
            try:
                products = future.result()
                all_products.extend(products)
                logger.info(f"Завршено собирање за {market['brand']} - {market['name']}")
            except Exception as e:
                logger.error(f"Исклучок при собирање за {market['brand']} - {market['name']}: {e}")
    
    return all_products

def download_pdf(url, timeout=TIMEOUT):
    """Презема PDF датотека од URL и враќа содржина како бајтови"""
    try:
        response = safe_request(url, timeout=timeout)
        return response.content
    except Exception as e:
        logger.error(f"Грешка при преземање на PDF од {url}: {e}")
        return None

def extract_text_from_pdf(pdf_content):
    """Извлекува текст од PDF содржина користејќи PyPDF2"""
    if not PDF_SUPPORT:
        logger.error("PDF библиотеките не се достапни. Не може да се извлече текст од PDF.")
        return ""
    
    text = ""
    try:
        with io.BytesIO(pdf_content) as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            for page_num in range(len(reader.pages)):
                text += reader.pages[page_num].extract_text() + "\n"
        return text
    except Exception as e:
        logger.error(f"Грешка при извлекување на текст од PDF: {e}")
        return ""

def extract_tables_from_pdf(pdf_content):
    """Извлекува табели од PDF содржина користејќи tabula-py"""
    if not PDF_SUPPORT:
        logger.error("PDF библиотеките не се достапни. Не може да се извлечат табели од PDF.")
        return []
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(pdf_content)
        
        tables = tabula.read_pdf(temp_file_path, pages='all', multiple_tables=True)
        
        try:
            os.remove(temp_file_path)
        except:
            pass
            
        return tables
    except Exception as e:
        logger.error(f"Грешка при извлекување на табели од PDF: {e}")
        return []

def parse_kam_pdf(pdf_content, market):
    """Парсира КАМ PDF ценовник и извлекува податоци за производи"""
    if not PDF_SUPPORT:
        logger.warning("PDF библиотеките не се достапни. Не може да се парсира КАМ PDF.")
        return []
    
    products = []
    
    try:
        tables = extract_tables_from_pdf(pdf_content)
        
        if tables:
            for table in tables:
                for _, row in table.iterrows():
                    if len(row) >= 3:
                        name_idx = None
                        unit_idx = None
                        price_idx = None
                        
                        for i, val in enumerate(row):
                            if isinstance(val, str):
                                val_lower = val.lower()
                                if len(val_lower) < 2:
                                    continue
                                
                                if not name_idx and any(word in val_lower for word in ['артикл', 'производ', 'име']):
                                    name_idx = i
                                    continue
                                    
                                if not unit_idx and any(word in val_lower for word in ['единица', 'мерка', 'е.м', 'ем']):
                                    unit_idx = i
                                    continue
                                    
                                if not price_idx and any(word in val_lower for word in ['цена', 'ден', 'денари', 'ценa']):
                                    price_idx = i
                                    continue
                        
                        if name_idx is None and len(row) >= 3:
                            name_idx = 0
                            unit_idx = 1
                            price_idx = 2
                        
                        if name_idx is not None and unit_idx is not None and price_idx is not None:
                            name = str(row[name_idx]).strip()
                            unit = str(row[unit_idx]).strip()
                            price = str(row[price_idx]).strip().replace('ден', '').replace('МПЦ', '').strip()
                            
                            if (name and unit and price and 
                                not any(header in name.lower() for header in ['артикл', 'производ', 'име']) and
                                not any(header in unit.lower() for header in ['единица', 'мерка', 'е.м', 'ем']) and
                                not any(header in price.lower() for header in ['цена', 'ценa'])):
                                
                                product = {
                                    'market_id': market.get('id', market['name']),
                                    'market_name': market['name'],
                                    'brand': market['brand'],
                                    'name': name,
                                    'unit': unit,
                                    'price': price,
                                    'date': get_today_str()
                                }
                                products.append(product)
        
        if not products:
            text = extract_text_from_pdf(pdf_content)
            lines = text.split('\n')
            
            for i, line in enumerate(lines):
                if len(line.strip()) < 5:
                    continue
                
                price_match = re.search(r'(\d+[.,]?\d*)\s*(?:ден|den)', line, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).strip()
                    parts = re.split(r'\s{2,}', line.strip())
                    if len(parts) >=  personally2:
                        name = parts[0].strip()
                        unit_candidates = parts[1:-1] if len(parts) > 2 else [parts[1]]
                        unit = unit_candidates[0].strip() if unit_candidates else ""
                        
                        if (name and unit and price and 
                            not any(header in name.lower() for header in ['артикл', 'производ', 'име']) and
                            not any(header in unit.lower() for header in ['единица', 'мерка', 'е.м', 'ем'])):
                            
                            product = {
                                'market_id': market.get('id', market['name']),
                                'market_name': market['name'],
                                'brand': market['brand'],
                                'name': name,
                                'unit': unit,
                                'price': price,
                                'date': get_today_str()
                            }
                            products.append(product)
                
        return products
    except Exception as e:
        logger.error(f"Грешка при парсирање на КАМ PDF: {e}")
        return []

def parse_kam_pdf_fallback(pdf_content, market):
    """Алтернативен парсер за КАМ PDF користејќи pdfplumber"""
    if not PDF_SUPPORT:
        logger.warning("PDF библиотеките не се достапни. Не може да се парсира КАМ PDF.")
        return []
    
    products = []
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(pdf_content)
        
        with pdfplumber.open(temp_file_path) as pdf:
            all_text = ""
            for page_num in range(len(pdf.pages)):
                page = pdf.pages[page_num]
                text = page.extract_text(x_tolerance=2, y_tolerance=3)
                if text:
                    all_text += text + "\n"
            
            lines = all_text.split('\n')
            
            for line in lines:
                line = line.strip()
                if len(line) < 5:
                    continue
                
                if any(header in line.lower() for header in ['артикл', 'производ', 'име', 'цени во маркети']):
                    continue
                
                if not any(price_unit in line.lower() for price_unit in ['ден', 'den', 'мкд', 'mkd']):
                    continue
                
                price_match = re.search(r'(\d+[.,]?\d*)\s*(?:ден|den|мкд|mkd|ден\.)', line, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).strip().replace(',', '.')
                    product_part = line[:price_match.start()].strip()
                    
                    units = ['кг', 'kg', 'г', 'g', 'л', 'l', 'мл', 'ml', 'бр', 'br', 'пар', 'пак']
                    unit = ""
                    
                    unit_match = None
                    for u in units:
                        pattern = r'(\s+\d*[\.,]?\d*\s*' + re.escape(u) + r'\.?\s+|\s+\d*[\.,]?\d*\s*' + re.escape(u) + r'\.?$)'
                        match = re.search(pattern, product_part, re.IGNORECASE)
                        if match:
                            unit = match.group(0).strip()
                            name_parts = product_part.split(match.group(0))
                            product_part = name_parts[0].strip()
                            unit_match = match
                            break
                    
                    if not unit_match:
                        parts = re.split(r'\s{2,}', product_part)
                        if len(parts) >= 2:
                            if len(parts[-1]) < 10:
                                unit = parts[-1].strip()
                                product_part = ' '.join(parts[:-1]).strip()
                    
                    name = product_part.strip()
                    
                    if name and price and not name.isdigit() and len(name) > 2:
                        product = {
                            'market_id': market.get('id', market['name']),
                            'market_name': market['name'],
                            'brand': market['brand'],
                            'name': name,
                            'unit': unit,
                            'price': price,
                            'date': get_today_str()
                        }
                        products.append(product)

            if not products:
                pattern = r'([^\d]+)(?:\s+(\d*[\.,]?\d*\s*(?:кг|kg|г|g|л|l|мл|ml|бр|br|пар|пак)\.?))?\s+(\d+[.,]?\d*)\s*(?:ден|den|мкд|mkd|ден\.)'
                matches = re.findall(pattern, all_text, re.IGNORECASE)
                for match in matches:
                    name, unit, price = match
                    name = name.strip()
                    unit = unit.strip()
                    price = price.strip().replace(',', '.')
                    
                    if name and price:
                        product = {
                            'market_id': market.get('id', market['name']),
                            'market_name': market['name'],
                            'brand': market['brand'],
                            'name': name,
                            'unit': unit,
                            'price': price,
                            'date': get_today_str()
                        }
                        products.append(product)

        try:
            os.remove(temp_file_path)
        except:
            pass
        
        if products:
            logger.info(f"Успешно извлечени {len(products)} производи од КАМ PDF со резервен метод")
        else:
            logger.warning(f"Не може да се извлечат производи од КАМ PDF дури и со резервен метод")
            
        return products
                
    except Exception as e:
        logger.error(f"Грешка во резервното парсирање на PDF: {e}")
        return []

def parse_kam_pdf_specialized(pdf_content, market):
    """Специјализиран парсер за компресирани PDF-ови на КАМ"""
    if not PDF_SUPPORT:
        logger.warning("PDF библиотеките не се достапни. Не може да се парсира КАМ PDF.")
        return []
    
    products = []
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(pdf_content)
        
        all_text = ""
        with open(temp_file_path, 'rb') as f:
            try:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        all_text += page_text + "\n"
            except Exception as e:
                logger.warning(f"Грешка при извлекување на текст со PyPDF2: {e}")
        
        lines = all_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            skip_terms = ['артикл', 'производ', 'цена', 'страна', 'стр.', 'цени во маркети', 'важи до']
            if any(term in line.lower() for term in skip_terms):
                continue
                
            price_match = re.search(r'(\d+[.,]?\d*)\s*(?:ден|den|мкд|mkd|ден\.)', line, re.IGNORECASE)
            if not price_match:
                continue
                
            price = price_match.group(1).strip().replace(',', '.')
            product_part = line[:price_match.start()].strip()
            
            unit = ""
            name = product_part
            
            unit_pattern = r'(\d*[\.,]?\d*\s*(?:кг|kg|г|g|л|l|мл|ml|бр|br|пар|пак)\.?)'
            unit_match = re.search(unit_pattern, product_part, re.IGNORECASE)
            if unit_match:
                unit = unit_match.group(0).strip()
                name = product_part[:unit_match.start()].strip()
            else:
                parts = re.split(r'\s{2,}|\t', product_part)
                if len(parts) >= 2:
                    last_part = parts[-1].strip()
                    if re.search(r'\d', last_part):
                        unit = last_part
                        name = ' '.join(parts[:-1]).strip()
                    else:
                        name = product_part
            
            if name and price and not name.isdigit() and len(name) > 2:
                product = {
                    'market_id': market.get('id', market['name']),
                    'market_name': market['name'],
                    'brand': market['brand'],
                    'name': name,
                    'unit': unit,
                    'price': price,
                    'date': get_today_str()
                }
                products.append(product)
        
        try:
            os.remove(temp_file_path)
        except:
            pass
            
        return products
    
    except Exception as e:
        logger.error(f"Грешка во специјализираното парсирање на КАМ PDF: {e}")
        return []

def main(max_workers=4, brand_filter=None, market_id=None, test_mode=False):
    """Главна функција за собирање и кеширање на цени од супермаркети"""
    today = setup_cache_dirs()
    
    logger.info(f"=== Започнување на fetch_and_cache.py за {today} ===")
    if brand_filter:
        logger.info(f"Филтрирање по бренд: {brand_filter}")
    if market_id:
        logger.info(f"Филтрирање по ID на пазар: {market_id}")
    if test_mode:
        logger.info("Работи во ТЕСТ режим - ограничено собирање на податоци")
    
    markets = fetch_all_markets()
    
    if brand_filter:
        markets = [m for m in markets if m['brand'].lower() == brand_filter.lower()]
        logger.info(f"Филтрирани до {len(markets)} {brand_filter} пазари")
    
    if market_id:
        market_id_str = str(market_id)
        markets = [m for m in markets if str(m.get('id', '')) == market_id_str]
        logger.info(f"Филтрирани до пазар ID {market_id}: {len(markets)} пазари")
    
    if test_mode:
        brands = set(m['brand'] for m in markets)
        test_markets = []
        for brand in brands:
            brand_markets = [m for m in markets if m['brand'] == brand]
            if brand_markets:
                test_markets.append(brand_markets[0])
        markets = test_markets
        logger.info(f"ТЕСТ режим: Ограничено на {len(markets)} пазари (еден по бренд)")
    
    if not markets:
        logger.warning("Не се пронајдени пазари по примена на филтри!")
        return
    
    failed_markets = []
    all_products = fetch_all_prices(markets, max_workers=max_workers)
    
    for market in markets:
        market_products = [p for p in all_products if p['market_id'] == market['id']]
        if not market_products:
            failed_markets.append(f"{market['brand']} - {market['name']} (ID: {market['id']}) - Нема производи")
        elif len(market_products) < 10:
            failed_markets.append(f"{market['brand']} - {market['name']} (ID: {market['id']}) - Само {len(market_products)} производи")
    
    if failed_markets:
        logger.error(f"Проблеми со {len(failed_markets)} пазари:\n" + "\n".join(failed_markets))
    
    if not all_products:
        logger.warning("Не се собрани податоци за цени!")
        return
    
    combined_csv = ROOT_CACHE_DIR / f"{today}.csv"
    combined_json = ROOT_CACHE_DIR / f"{today}.json"
    project_combined_csv = PROJECT_CACHE_DIR / f"{today}.csv"
    project_combined_json = PROJECT_CACHE_DIR / f"{today}.json"
    
    save_to_csv(all_products, combined_csv)
    save_to_json(all_products, combined_json)
    save_to_csv(all_products, project_combined_csv)
    save_to_json(all_products, project_combined_json)
    
    for brand in set(p['brand'] for p in all_products):
        brand_products = [p for p in all_products if p['brand'] == brand]
        brand_csv = ROOT_CACHE_DIR / f"{today}-{brand.lower()}.csv"
        brand_json = ROOT_CACHE_DIR / f"{today}-{brand.lower()}.json"
        project_brand_csv = PROJECT_CACHE_DIR / f"{today}-{brand.lower()}.csv"
        project_brand_json = PROJECT_CACHE_DIR / f"{today}-{brand.lower()}.json"
        
        save_to_csv(brand_products, brand_csv)
        save_to_json(brand_products, brand_json)
        save_to_csv(brand_products, project_brand_csv)
        save_to_json(brand_products, project_brand_json)
    
    logger.info(f"=== Завршено: собрани {len(all_products)} цени од {len(markets)} пазари ===")
    if failed_markets:
        logger.info(f"Забелешка: Детектирани проблеми со {len(failed_markets)} пазари; провери ги логовите за детали")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Собира и кешира цени од супермаркети во Македонија")
    parser.add_argument("--brand", help="Филтрирај по бренд (KAM, Tinex, Vero, Stokomak)")
    parser.add_argument("--market-id", help="Филтрирај по ID на пазар")
    parser.add_argument("--workers", type=int, default=4, help="Број на работни нишки (стандардно: 4)")
    parser.add_argument("--test", action="store_true", help="Тест режим - собира само еден пазар по бренд")
    args = parser.parse_args()
    
    main(
        max_workers=args.workers, 
        brand_filter=args.brand, 
        market_id=args.market_id,
        test_mode=args.test
    )