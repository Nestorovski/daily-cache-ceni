#!/usr/bin/env python3
"""
fetch_and_cache.py - Scraper for supermarket prices in Macedonia

This script:
1. Fetches all market locations for KAM, Tinex, Vero, and Stokomak
2. Gets price data from each market
3. Stores data in CSV and JSON formats in the cache directory with date-based folders
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

# For PDF processing
try:
    import PyPDF2
    import tabula
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logging.warning("PDF support libraries not available. Install PyPDF2, tabula-py, and pdfplumber for PDF processing.")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": USER_AGENT
}
TIMEOUT = 15  # seconds
SLEEP_BETWEEN_REQUESTS = 1  # seconds

# Cache directories
SCRIPT_DIR = Path(__file__).parent
ROOT_CACHE_DIR = SCRIPT_DIR.parent / "cache"
PROJECT_CACHE_DIR = SCRIPT_DIR / "cache"

def get_today_str():
    """Returns today's date in YYYY-MM-DD format"""
    return datetime.now().strftime("%Y-%m-%d")

def setup_cache_dirs():
    """Create cache directories if they don't exist"""
    today = get_today_str()
    
    # Ensure both cache directories exist
    ROOT_CACHE_DIR.mkdir(exist_ok=True)
    PROJECT_CACHE_DIR.mkdir(exist_ok=True)
    
    return today

def safe_request(url, headers=None, timeout=TIMEOUT, retries=3, method="get", **kwargs):
    """Make a safe request with retries and error handling"""
    headers = headers or HEADERS
    
    for attempt in range(retries):
        try:
            if method.lower() == "get":
                response = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            else:
                response = requests.post(url, headers=headers, timeout=timeout, **kwargs)
            
            response.raise_for_status()
            time.sleep(SLEEP_BETWEEN_REQUESTS)  # Be nice to the servers
            return response
        except requests.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt+1}/{retries}): {url}, {e}")
            if attempt == retries - 1:
                logger.error(f"All {retries} attempts failed for {url}")
                raise
            time.sleep(SLEEP_BETWEEN_REQUESTS * (attempt + 1))  # Exponential backoff

def fetch_tinex_markets():
    """Fetch all Tinex market locations"""
    logger.info("Fetching Tinex markets...")
    url = 'http://ceni.tinex.mk/'
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    select = soup.find('select', {'name': 'org'})
    if not select:
        logger.error("Could not find market selector on Tinex page")
        return []
    
    markets = [
        {
            'brand': 'Tinex',
            'id': int(opt['value']),
            'name': opt.text.strip(),
            'url': f"http://ceni.tinex.mk/?org={opt['value']}&perPage=100"
        }
        for opt in select.find_all('option') if opt['value']
    ]
    logger.info(f"Found {len(markets)} Tinex markets")
    return markets

def fetch_kam_markets():
    """Fetch all KAM market locations"""
    logger.info("Fetching KAM markets...")
    url = 'https://kam.com.mk/ceni-vo-marketi/'
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    markets = []
    for div in soup.select('.markets_wrap'):
        name_tag = div.find('h2')
        address_tag = div.find('p')
        url_tag = div.find('a')
        
        if name_tag and url_tag:
            # Extract market_id from URL if possible
            market_id = url_tag['href'].rstrip('/').split('/')[-1]
            
            market = {
                'brand': 'KAM',
                'name': name_tag.text.strip(),
                'address': address_tag.text.strip() if address_tag else "",
                'url': url_tag['href'],
                'id': market_id  # Always set the ID
            }
            
            markets.append(market)
    
    logger.info(f"Found {len(markets)} KAM markets")
    return markets

def fetch_vero_markets():
    """Fetch all Vero market locations"""
    logger.info("Fetching Vero markets...")
    url = 'https://pricelist.vero.com.mk/'
    response = safe_request(url)
    # Fix encoding for Vero's Cyrillic characters
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    markets = []
    # Example: market links are in <a href="89_1.html">ВЕРО 1</a>
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
    
    logger.info(f"Found {len(markets)} Vero markets")
    return markets

def fetch_stokomak_markets():
    """Fetch all Stokomak market locations"""
    logger.info("Fetching Stokomak markets...")
    url = 'https://stokomak.proverkanaceni.mk/'
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    select = soup.find('select', {'name': 'org'})
    if not select:
        logger.error("Could not find market selector on Stokomak page")
        return []
    
    markets = [
        {
            'brand': 'Stokomak',
            'id': int(opt['value']),
            'name': opt.text.strip(),
            'url': f"https://stokomak.proverkanaceni.mk/?org={opt['value']}&perPage=100"
        }
        for opt in select.find_all('option') if opt['value']
    ]
    
    logger.info(f"Found {len(markets)} Stokomak markets")
    return markets

def fetch_all_markets():
    """Fetch all markets from all brands"""
    all_markets = []
    
    try:
        all_markets.extend(fetch_tinex_markets())
    except Exception as e:
        logger.error(f"Failed to fetch Tinex markets: {e}")
    
    try:
        all_markets.extend(fetch_kam_markets())
    except Exception as e:
        logger.error(f"Failed to fetch KAM markets: {e}")
    
    try:
        all_markets.extend(fetch_vero_markets())
    except Exception as e:
        logger.error(f"Failed to fetch Vero markets: {e}")
    
    try:
        all_markets.extend(fetch_stokomak_markets())
    except Exception as e:
        logger.error(f"Failed to fetch Stokomak markets: {e}")
    
    return all_markets

def fetch_tinex_prices(market):
    """Fetch prices from a Tinex market with pagination"""
    logger.info(f"Fetching Tinex prices for {market['name']} (ID: {market['id']})...")
    base_url = f"http://ceni.tinex.mk/?org={market['id']}"
    products = []
    page = 1
    max_pages = 100  # Safety limit, but we'll break earlier if we detect the last page
    
    while page <= max_pages:
        url = f"{base_url}&page={page}&perPage=100"
        response = safe_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.select("table.table tbody tr")
        if not rows:
            break  # No more products
            
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
            break  # No products found on this page
            
        products.extend(page_products)
        logger.info(f"Fetched {len(page_products)} products from page {page} for Tinex {market['name']}")
        
        # Check if we need to fetch more pages
        pagination_info = soup.select_one(".pagination-info")
        if pagination_info:
            info_text = pagination_info.text.strip()
            total_match = re.search(r'од\s+(\d+)', info_text)
            if total_match:
                total_items = int(total_match.group(1))
                if len(products) >= total_items:
                    logger.info(f"Reached all {total_items} products for Tinex {market['name']}")
                    break
        
        # Check if there's a "next" button - if not, we're on the last page
        next_button = soup.select_one(".pagination .page-item:not(.disabled) a[aria-label='Next']")
        if not next_button:
            logger.info(f"Reached last page ({page}) for Tinex {market['name']}")
            break
            
        # Move to next page
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)  # Be polite
        
        if page > max_pages:
            logger.warning(f"Reached maximum page limit ({max_pages}) for Tinex {market['name']}")
            
    logger.info(f"Total: Found {len(products)} prices for Tinex {market['name']}")
    return products

def fetch_kam_prices(market):
    """Fetch prices from a KAM market with improved PDF support"""
    logger.info(f"Fetching KAM prices for {market['name']}...")
    url = market['url']
    response = safe_request(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # KAM offers PDFs, but let's try to parse the HTML table if available first
    products = []
    price_tables = soup.select(".ceni_table")
    
    for table in price_tables:
        rows = table.select("tr")
        # Skip header row
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
    
    # If products already found in HTML, don't bother with PDF
    if products:
        logger.info(f"Found {len(products)} prices in HTML for KAM {market['name']}")
        return products
    
    # Enhanced debugging: Print all links found on the page
    pdf_url = None
    all_links = []
    for a in soup.find_all('a'):
        href = a.get('href')
        if href:
            all_links.append(href)
            # Debug: print any PDF links we find
            if '.pdf' in href:
                logger.info(f"Found PDF link: {href}")
    
    logger.info(f"Found {len(all_links)} total links on KAM page")
    
    # Use multiple patterns to try to find the correct PDF format
    # Pattern 1: Classic /pdf/123.pdf format
    pdf_pattern1 = re.compile(r'/pdf/\d+\.pdf')
    # Pattern 2: Also match pdf/123.pdf without leading slash
    pdf_pattern2 = re.compile(r'pdf/\d+\.pdf')
    # Pattern 3: Full URL format
    pdf_pattern3 = re.compile(r'https?://kam\.com\.mk/pdf/\d+\.pdf')
    
    # Try all patterns
    pdf_links = []
    for link in all_links:
        if pdf_pattern1.search(link) or pdf_pattern2.search(link) or pdf_pattern3.search(link):
            pdf_links.append(link)
            logger.info(f"Matched PDF pattern: {link}")
    
    if pdf_links:
        pdf_url = pdf_links[0]
        logger.info(f"Found PDF URL with /pdf/id.pdf format in page: {pdf_url}")
    else:
        # Extract any numbers from the market URL and try those
        url_numbers = re.findall(r'/(\d+)/?$', market['url'])
        if url_numbers:
            num = url_numbers[0]
            test_url = f"https://kam.com.mk/pdf/{num}.pdf"
            logger.info(f"Trying KAM PDF URL based on URL number: {test_url}")
            
            try:
                test_response = requests.head(test_url, headers=HEADERS, timeout=TIMEOUT/2)
                if test_response.status_code == 200:
                    pdf_url = test_url
                    logger.info(f"Found working KAM PDF URL: {pdf_url}")
                else:
                    logger.info(f"URL {test_url} returned status code {test_response.status_code}")
            except Exception as e:
                logger.debug(f"Error checking URL {test_url}: {e}")
        
        # Try to find numeric pattern in the HTML content directly
        if not pdf_url:
            html_content = response.text
            # Try to find patterns like '/pdf/123.pdf' directly in the HTML
            direct_patterns = [
                r'/pdf/(\d+)\.pdf',
                r'pdf/(\d+)\.pdf',
                r'href=[\'"](?:https?://kam\.com\.mk)?/pdf/(\d+)\.pdf[\'"]'
            ]
            
            for pattern in direct_patterns:
                matches = re.findall(pattern, html_content)
                if matches:
                    num = matches[0]
                    test_url = f"https://kam.com.mk/pdf/{num}.pdf"
                    logger.info(f"Found numeric pattern in HTML: {test_url}")
                    try:
                        test_response = requests.head(test_url, headers=HEADERS, timeout=TIMEOUT/2)
                        if test_response.status_code == 200:
                            pdf_url = test_url
                            logger.info(f"Pattern-matched URL works: {pdf_url}")
                            break
                        else:
                            logger.info(f"Pattern-matched URL returned status code {test_response.status_code}")
                    except Exception as e:
                        logger.debug(f"Error checking URL {test_url}: {e}")
                        
        # Hardcoded attempt - just to confirm this works
        if not pdf_url:
            test_url = "https://kam.com.mk/pdf/99.pdf"
            logger.info(f"Trying hardcoded test URL: {test_url}")
            try:
                test_response = requests.head(test_url, headers=HEADERS, timeout=TIMEOUT/2)
                if test_response.status_code == 200:
                    pdf_url = test_url
                    logger.info(f"Hardcoded test URL works: {pdf_url}")
                else:
                    logger.info(f"Hardcoded test URL returned status code {test_response.status_code}")
            except Exception as e:
                logger.debug(f"Error checking hardcoded URL: {e}")
    
    # Make sure URL is absolute
    if pdf_url and not pdf_url.startswith(('http://', 'https://')):
        if pdf_url.startswith('/'):
            pdf_url = f"https://kam.com.mk{pdf_url}"
        else:
            pdf_url = f"https://kam.com.mk/{pdf_url}"
    
    # If we still don't have a working PDF URL, fall back to any PDF links on the page
    if not pdf_url:
        logger.warning(f"Could not find a PDF URL in the /pdf/id.pdf format, falling back to any PDF")
        pdf_links = soup.select("a[href$='.pdf']")
        if pdf_links:
            pdf_url = pdf_links[0]['href']
            # Make sure URL is absolute
            if not pdf_url.startswith(('http://', 'https://')):
                if pdf_url.startswith('/'):
                    pdf_url = f"https://kam.com.mk{pdf_url}"
                else:
                    pdf_url = f"https://kam.com.mk/{pdf_url}"
            logger.info(f"Using fallback PDF URL for KAM {market['name']}: {pdf_url}")
    
    # Process the PDF if we found a URL
    if pdf_url:
        logger.info(f"KAM {market['name']} has PDF price list: {pdf_url}")
        
        if PDF_SUPPORT:
            pdf_content = download_pdf(pdf_url)
            if pdf_content:
                logger.info(f"Successfully downloaded PDF for KAM {market['name']}, parsing...")
                
                # Try parsing with all three methods in order
                pdf_products = parse_kam_pdf_specialized(pdf_content, market)
                if pdf_products:
                    products.extend(pdf_products)
                    logger.info(f"Extracted {len(pdf_products)} products from PDF for KAM {market['name']} using specialized parser")
                else:
                    pdf_products = parse_kam_pdf(pdf_content, market)
                    if pdf_products:
                        products.extend(pdf_products)
                        logger.info(f"Extracted {len(pdf_products)} products from PDF for KAM {market['name']} using standard parser")
                    else:
                        logger.warning(f"First two PDF parsing methods failed for KAM {market['name']}, trying fallback...")
                        pdf_products = parse_kam_pdf_fallback(pdf_content, market)
                        if pdf_products:
                            products.extend(pdf_products)
                            logger.info(f"Fallback method: Extracted {len(pdf_products)} products from PDF for KAM {market['name']}")
                        else:
                            logger.warning(f"All PDF parsing methods failed for KAM {market['name']}")
            else:
                logger.error(f"Failed to download PDF for KAM {market['name']}")
        else:
            logger.warning(f"PDF support libraries not available. Skipping PDF parsing for KAM {market['name']}")
    else:
        logger.info(f"No PDF price list found for KAM {market['name']}")
    
    logger.info(f"Total: Found {len(products)} prices for KAM {market['name']}")
    return products

def fetch_vero_prices(market):
    """Fetch prices from a Vero market with pagination"""
    logger.info(f"Fetching Vero prices for {market['name']} (ID: {market['id']})...")
    base_url = f"https://pricelist.vero.com.mk/"
    products = []
    
    # Vero uses a pattern with page numbers in the URL like 89_1.html, 89_2.html, etc.
    # Extract the base part (e.g., "89") and start with page 1
    id_parts = market['id'].split('_')
    base_id = id_parts[0]
    page = 1
    max_pages = 100  # Safety limit
    
    while page <= max_pages:
        # Construct URL for this page
        page_url = f"{base_url}{base_id}_{page}.html"
        
        try:
            response = safe_request(page_url)
            # Force UTF-8 encoding to fix Cyrillic characters
            response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select("table tr")
            if len(rows) <= 1:  # Only header row or no rows at all
                # We've gone beyond the last page
                break
                
            # Skip header row
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
                # No products found on this page, we've reached the end
                break
                
            products.extend(page_products)
            logger.info(f"Fetched {len(page_products)} products from page {page} for Vero {market['name']}")
            
            # Move to the next page
            page += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)  # Be polite
            
            if page > max_pages:
                logger.warning(f"Reached maximum page limit ({max_pages}) for Vero {market['name']}")
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # No more pages
                logger.info(f"Reached end of pages for Vero {market['name']} at page {page}")
                break
            else:
                logger.error(f"HTTP error fetching Vero page {page}: {e}")
                break
        except Exception as e:
            logger.error(f"Error fetching Vero page {page}: {e}")
            break
            
    logger.info(f"Total: Found {len(products)} prices for Vero {market['name']}")
    return products

def fetch_stokomak_prices(market):
    """Fetch prices from a Stokomak market with pagination"""
    logger.info(f"Fetching Stokomak prices for {market['name']} (ID: {market['id']})...")
    base_url = f"https://stokomak.proverkanaceni.mk/"
    products = []
    page = 1
    max_pages = 100  # Safety limit, but we'll break earlier if we detect the last page
    
    while page <= max_pages:
        url = f"{base_url}?org={market['id']}&page={page}&perPage=100"
        response = safe_request(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.select("table.table tbody tr")
        if not rows:
            break  # No more products
            
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
            break  # No products found on this page
            
        products.extend(page_products)
        logger.info(f"Fetched {len(page_products)} products from page {page} for Stokomak {market['name']}")
        
        # Check if we need to fetch more pages - look for pagination info
        pagination_info = soup.select_one(".pagination-info")
        if pagination_info:
            info_text = pagination_info.text.strip()
            total_match = re.search(r'од\s+(\d+)', info_text)
            if total_match:
                total_items = int(total_match.group(1))
                if len(products) >= total_items:
                    logger.info(f"Reached all {total_items} products for Stokomak {market['name']}")
                    break
        
        # Check if there's a "next" button - if not, we're on the last page
        next_button = soup.select_one(".pagination .page-item:not(.disabled) a.page-link[aria-label='Next']")
        if not next_button:
            logger.info(f"Reached last page ({page}) for Stokomak {market['name']}")
            break
        
        # Move to next page
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)  # Be polite
        
        if page > max_pages:
            logger.warning(f"Reached maximum page limit ({max_pages}) for Stokomak {market['name']}")
            
    logger.info(f"Total: Found {len(products)} prices for Stokomak {market['name']}")
    return products

def fetch_market_prices(market):
    """Fetch prices for a given market based on its brand"""
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
            logger.warning(f"Unknown brand: {brand}")
            return []
    except Exception as e:
        logger.error(f"Error fetching prices for {market['name']} ({brand}): {e}")
        return []

def save_to_csv(data, filename):
    """Save data to CSV file"""
    if not data:
        logger.warning(f"No data to save to {filename}")
        return False
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Write CSV
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Saved {len(data)} records to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving CSV {filename}: {e}")
        return False

def save_to_json(data, filename):
    """Save data to JSON file"""
    if not data:
        logger.warning(f"No data to save to {filename}")
        return False
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Write JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(data)} records to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving JSON {filename}: {e}")
        return False

def fetch_all_prices(markets, max_workers=4):
    """Fetch prices for all markets, with optional parallelization"""
    all_products = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create dictionary of futures to markets for tracking
        future_to_market = {executor.submit(fetch_market_prices, market): market for market in markets}
        
        for future in concurrent.futures.as_completed(future_to_market):
            market = future_to_market[future]
            try:
                products = future.result()
                all_products.extend(products)
                logger.info(f"Completed fetching {market['brand']} - {market['name']}")
            except Exception as e:
                logger.error(f"Exception fetching {market['brand']} - {market['name']}: {e}")
    
    return all_products

def download_pdf(url, timeout=TIMEOUT):
    """Download a PDF file from URL and return its content as bytes"""
    try:
        response = safe_request(url, timeout=timeout)
        return response.content
    except Exception as e:
        logger.error(f"Error downloading PDF from {url}: {e}")
        return None

def extract_text_from_pdf(pdf_content):
    """Extract text from PDF content using PyPDF2"""
    if not PDF_SUPPORT:
        logger.error("PDF support libraries not available. Cannot extract text from PDF.")
        return ""
    
    text = ""
    try:
        with io.BytesIO(pdf_content) as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            for page_num in range(len(reader.pages)):
                text += reader.pages[page_num].extract_text() + "\n"
        return text
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {e}")
        return ""

def extract_tables_from_pdf(pdf_content):
    """Extract tables from PDF content using tabula-py"""
    if not PDF_SUPPORT:
        logger.error("PDF support libraries not available. Cannot extract tables from PDF.")
        return []
    
    try:
        # Save PDF content to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(pdf_content)
        
        # Extract tables using tabula
        tables = tabula.read_pdf(temp_file_path, pages='all', multiple_tables=True)
        
        # Clean up
        try:
            os.remove(temp_file_path)
        except:
            pass
            
        return tables
    except Exception as e:
        logger.error(f"Error extracting tables from PDF: {e}")
        return []

def parse_kam_pdf(pdf_content, market):
    """
    Parse KAM PDF price list and extract product data
    Returns list of product dictionaries
    """
    if not PDF_SUPPORT:
        logger.warning("PDF support libraries not available. Cannot parse KAM PDF.")
        return []
    
    products = []
    
    try:
        # First try tabula for structured table extraction
        tables = extract_tables_from_pdf(pdf_content)
        
        if tables:
            for table in tables:
                # Clean up table - typical column names might be 'Артикл', 'Единица мерка', 'Цена'
                # Convert DataFrame to list of dictionaries
                for _, row in table.iterrows():
                    if len(row) >= 3:
                        # Try to find the column indices for name, unit and price
                        name_idx = None
                        unit_idx = None
                        price_idx = None
                        
                        # Attempt to identify columns by content
                        for i, val in enumerate(row):
                            if isinstance(val, str):
                                val_lower = val.lower()
                                # Skip empty or very short values
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
                        
                        # If we couldn't identify columns by name, use position
                        if name_idx is None and len(row) >= 3:
                            name_idx = 0
                            unit_idx = 1
                            price_idx = 2
                        
                        # Extract values if all columns were identified
                        if name_idx is not None and unit_idx is not None and price_idx is not None:
                            name = str(row[name_idx]).strip()
                            unit = str(row[unit_idx]).strip()
                            price = str(row[price_idx]).strip().replace('ден', '').replace('МПЦ', '').strip()
                            
                            # Skip header rows or rows with missing data
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
        
        # If table extraction didn't work well, try text extraction and parsing
        if not products:
            text = extract_text_from_pdf(pdf_content)
            lines = text.split('\n')
            
            # Pattern: product name, followed by unit, followed by price
            for i, line in enumerate(lines):
                # Skip short lines
                if len(line.strip()) < 5:
                    continue
                
                # Try to find a pattern that matches product, unit, price
                # First check if this line has a price pattern
                price_match = re.search(r'(\d+[.,]?\d*)\s*(?:ден|den)', line, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).strip()
                    # Extract what could be the name and unit from this line
                    parts = re.split(r'\s{2,}', line.strip())
                    if len(parts) >= 2:
                        name = parts[0].strip()
                        # Look for unit between name and price
                        unit_candidates = parts[1:-1] if len(parts) > 2 else [parts[1]]
                        unit = unit_candidates[0].strip() if unit_candidates else ""
                        
                        # Skip header rows
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
        logger.error(f"Error parsing KAM PDF: {e}")
        return []

def parse_kam_pdf_fallback(pdf_content, market):
    """
    Alternative PDF parser for KAM using pdfplumber
    This provides a fallback in case tabula doesn't work well with KAM's PDF format
    """
    if not PDF_SUPPORT:
        logger.warning("PDF support libraries not available. Cannot parse KAM PDF.")
        return []
    
    products = []
    
    try:
        # Create a temporary file for pdfplumber
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(pdf_content)
        
        # Use pdfplumber to extract data
        with pdfplumber.open(temp_file_path) as pdf:
            # First approach: Extract just raw text and parse line by line since KAM PDFs 
            # often have formatting that confuses table extraction
            all_text = ""
            for page_num in range(len(pdf.pages)):
                page = pdf.pages[page_num]
                text = page.extract_text(x_tolerance=2, y_tolerance=3)
                if text:
                    all_text += text + "\n"
            
            # Split into lines for line-by-line processing
            lines = all_text.split('\n')
            
            # KAM PDFs typically have product name at start of line and price at end
            # Sometimes each line has name, unit, price separated by spaces or tabs
            for line in lines:
                line = line.strip()
                # Skip short or empty lines
                if len(line) < 5:
                    continue
                
                # Skip header lines
                if any(header in line.lower() for header in ['артикл', 'производ', 'име', 'цени во маркети']):
                    continue
                
                # Skip lines that don't contain any price indicators
                if not any(price_unit in line.lower() for price_unit in ['ден', 'den', 'мкд', 'mkd']):
                    continue
                
                # Check for price pattern at end of line: digits followed by "ден" or similar
                price_match = re.search(r'(\d+[.,]?\d*)\s*(?:ден|den|мкд|mkd|ден\.)', line, re.IGNORECASE)
                if price_match:
                    price = price_match.group(1).strip().replace(',', '.')
                    
                    # Remove the price part from the line
                    product_part = line[:price_match.start()].strip()
                    
                    # Try to separate name and unit
                    # Common units in Macedonian markets
                    units = ['кг', 'kg', 'г', 'g', 'л', 'l', 'мл', 'ml', 'бр', 'br', 'пар', 'пак']
                    unit = ""
                    
                    # Look for a unit indicator
                    unit_match = None
                    for u in units:
                        # Look for the unit with surrounding spaces or at the end of text
                        pattern = r'(\s+\d*[\.,]?\d*\s*' + re.escape(u) + r'\.?\s+|\s+\d*[\.,]?\d*\s*' + re.escape(u) + r'\.?$)'
                        match = re.search(pattern, product_part, re.IGNORECASE)
                        if match:
                            unit = match.group(0).strip()
                            # Split at the unit to get clean name and unit
                            name_parts = product_part.split(match.group(0))
                            product_part = name_parts[0].strip()
                            unit_match = match
                            break
                    
                    # If we couldn't find a standard unit, try to split by multiple spaces
                    if not unit_match:
                        parts = re.split(r'\s{2,}', product_part)
                        if len(parts) >= 2:
                            # Assume the last part before price might be the unit
                            if len(parts[-1]) < 10:  # Units are usually short
                                unit = parts[-1].strip()
                                product_part = ' '.join(parts[:-1]).strip()
                    
                    # Clean up the name
                    name = product_part.strip()
                    
                    # Only add if we have a valid-looking product
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

            # If we got no products, try a simpler pattern-based approach with regular expressions
            if not products:
                # Common pattern: Product name, possibly followed by unit, followed by price
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

        # Clean up temp file
        try:
            os.remove(temp_file_path)
        except:
            pass
        
        # Log a message about extraction success/failure
        if products:
            logger.info(f"Successfully extracted {len(products)} products from KAM PDF using fallback method")
        else:
            logger.warning(f"Could not extract products from KAM PDF even with fallback method")
            
        return products
                
    except Exception as e:
        logger.error(f"Error in PDF fallback parsing: {e}")
        return []

# Add a third method specifically for handling KAM's compressed PDFs
def parse_kam_pdf_specialized(pdf_content, market):
    """
    Specialized parser for KAM's compressed PDFs that have a unique format.
    This targets the specific format seen in the kammkletok14-20.040.2025_compressed.pdf file.
    """
    if not PDF_SUPPORT:
        logger.warning("PDF support libraries not available. Cannot parse KAM PDF.")
        return []
    
    products = []
    
    try:
        # Create a temp file for PyPDF2
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file_path = temp_file.name
            temp_file.write(pdf_content)
        
        # First try with PyPDF2 which sometimes handles compressed PDFs better
        all_text = ""
        with open(temp_file_path, 'rb') as f:
            try:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        all_text += page_text + "\n"
            except Exception as e:
                logger.warning(f"Error extracting text with PyPDF2: {e}")
        
        # Try to find patterns that look like:
        # Product Name                      1000 ден.
        # or
        # Product Name                  1 kg    1000 ден.
        lines = all_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Skip header/footer lines
            skip_terms = ['артикл', 'производ', 'цена', 'страна', 'стр.', 'цени во маркети', 'важи до']
            if any(term in line.lower() for term in skip_terms):
                continue
                
            # Look for price at end of line
            price_match = re.search(r'(\d+[.,]?\d*)\s*(?:ден|den|мкд|mkd|ден\.)', line, re.IGNORECASE)
            if not price_match:
                continue
                
            price = price_match.group(1).strip().replace(',', '.')
            
            # Get the part before the price
            product_part = line[:price_match.start()].strip()
            
            # Try to find a unit
            unit = ""
            name = product_part
            
            # Check for common units with space
            unit_pattern = r'(\d*[\.,]?\d*\s*(?:кг|kg|г|g|л|l|мл|ml|бр|br|пар|пак)\.?)'
            unit_match = re.search(unit_pattern, product_part, re.IGNORECASE)
            if unit_match:
                unit = unit_match.group(0).strip()
                # Name is everything before the unit
                name = product_part[:unit_match.start()].strip()
            else:
                # Try to split by multiple spaces or tabs
                parts = re.split(r'\s{2,}|\t', product_part)
                if len(parts) >= 2:
                    # Last part might be the unit, but check if it looks like one
                    last_part = parts[-1].strip()
                    if re.search(r'\d', last_part):  # If it contains digits, likely a unit measurement
                        unit = last_part
                        name = ' '.join(parts[:-1]).strip()
                    else:
                        # All parts might be the name
                        name = product_part
            
            # Final validation
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
        
        # Clean up temp file
        try:
            os.remove(temp_file_path)
        except:
            pass
            
        return products
    
    except Exception as e:
        logger.error(f"Error in specialized KAM PDF parsing: {e}")
        return []

def main(max_workers=4, brand_filter=None, market_id=None, test_mode=False):
    """Main function to fetch and cache supermarket prices"""
    today = setup_cache_dirs()
    
    # Log the run details
    logger.info(f"=== Starting fetch_and_cache.py run for {today} ===")
    if brand_filter:
        logger.info(f"Filtering for brand: {brand_filter}")
    if market_id:
        logger.info(f"Filtering for market ID: {market_id}")
    if test_mode:
        logger.info("Running in TEST MODE - limiting data collection")
    
    # Fetch all markets
    markets = fetch_all_markets()
    
    # Apply filters if specified
    if brand_filter:
        markets = [m for m in markets if m['brand'].lower() == brand_filter.lower()]
        logger.info(f"Filtered to {len(markets)} {brand_filter} markets")
    
    if market_id:
        market_id_str = str(market_id)  # Convert to string for comparison
        markets = [m for m in markets if str(m.get('id', '')) == market_id_str]
        logger.info(f"Filtered to market ID {market_id}: {len(markets)} markets")
    
    # Limit markets in test mode - select one market per brand
    if test_mode:
        brands = set(m['brand'] for m in markets)
        test_markets = []
        for brand in brands:
            brand_markets = [m for m in markets if m['brand'] == brand]
            if brand_markets:
                test_markets.append(brand_markets[0])
        markets = test_markets
        logger.info(f"TEST MODE: Limited to {len(markets)} markets (one per brand)")
    
    if not markets:
        logger.warning("No markets found after applying filters!")
        return
    
    # Fetch prices for all markets
    all_products = fetch_all_prices(markets, max_workers=max_workers)
    
    if not all_products:
        logger.warning("No price data collected!")
        return
    
    # Create output filenames
    combined_csv = ROOT_CACHE_DIR / f"{today}.csv"
    combined_json = ROOT_CACHE_DIR / f"{today}.json"
    
    project_combined_csv = PROJECT_CACHE_DIR / f"{today}.csv"
    project_combined_json = PROJECT_CACHE_DIR / f"{today}.json"
    
    # Save all data
    save_to_csv(all_products, combined_csv)
    save_to_json(all_products, combined_json)
    
    # Also save to the project folder
    save_to_csv(all_products, project_combined_csv)
    save_to_json(all_products, project_combined_json)
    
    # Save individual brand files
    for brand in set(p['brand'] for p in all_products):
        brand_products = [p for p in all_products if p['brand'] == brand]
        brand_csv = ROOT_CACHE_DIR / f"{today}-{brand.lower()}.csv"
        brand_json = ROOT_CACHE_DIR / f"{today}-{brand.lower()}.json"
        
        save_to_csv(brand_products, brand_csv)
        save_to_json(brand_products, brand_json)
        
        # Also save to the project folder
        project_brand_csv = PROJECT_CACHE_DIR / f"{today}-{brand.lower()}.csv"
        project_brand_json = PROJECT_CACHE_DIR / f"{today}-{brand.lower()}.json"
        
        save_to_csv(brand_products, project_brand_csv)
        save_to_json(brand_products, project_brand_json)
    
    # Log completion
    logger.info(f"=== Run completed: collected {len(all_products)} prices from {len(markets)} markets ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and cache supermarket prices in Macedonia")
    parser.add_argument("--brand", help="Filter to specific brand (KAM, Tinex, Vero, Stokomak)")
    parser.add_argument("--market-id", help="Filter to specific market ID")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads (default: 4)")
    parser.add_argument("--test", action="store_true", help="Test mode - fetch only one market per brand")
    args = parser.parse_args()
    
    main(
        max_workers=args.workers, 
        brand_filter=args.brand, 
        market_id=args.market_id,
        test_mode=args.test
    )
