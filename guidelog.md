# daily-cache-ceni - Водич за автоматизација

## 1. Архитектура и чекори

1. **Мапирање на маркети:**
   - Скенирање на сите достапни маркети/локации за секој бренд (КАМ, Тинекс, Веро, Стокомак) преку scraping или API.
   - Складирање на имиња, адреси, ID и URL во JSON/CSV.
2. **Фечирање на ценовници:**
   - За КАМ: PDF download и парсирање.
   - За другите: HTML scraping со пагинација.
3. **Кеширање:**
   - Секојдневно (на 24 часа) се креира нова папка со датум (YYYY-MM-DD) во `cache/`.
   - Се чуваат CSV/JSON фајлови за секој маркет.
4. **Архива:**
   - Старите податоци се достапни за споредба по датум, маркет и бренд.
5. **Автоматизација:**
   - GitHub Actions workflow за дневно пуштање на Python скриптата и commit/push на новите податоци.
   - GitHub Pages за јавен приказ и споредба.

## 2. Пример Python структура за фечирање

```python
import requests
from bs4 import BeautifulSoup

def fetch_tinex_markets():
    url = 'http://ceni.tinex.mk/'
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    select = soup.find('select', {'name': 'org'})
    return [
        {'id': int(opt['value']), 'name': opt.text.strip()}
        for opt in select.find_all('option') if opt['value']
    ]

def fetch_kam_markets():
    url = 'https://kam.com.mk/ceni-vo-marketi/'
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    markets = []
    for div in soup.select('.markets_wrap'):
        name = div.find('h2').text.strip()
        address = div.find('p').text.strip()
        url = div.find('a')['href']
        markets.append({'name': name, 'address': address, 'url': url})
    return markets

def fetch_vero_markets():
    url = 'https://pricelist.vero.com.mk/'
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    # Example: market links are in <a href="89_1.html">ВЕРО 1</a>
    return [
        {'id': a['href'].replace('.html',''), 'name': a.text.strip(), 'url': url + a['href']}
        for a in soup.select('a[href$=".html"]') if a['href'][0].isdigit()
    ]

def fetch_stokomak_markets():
    url = 'https://stokomak.proverkanaceni.mk/'
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    select = soup.find('select', {'name': 'org'})
    return [
        {'id': int(opt['value']), 'name': opt.text.strip()}
        for opt in select.find_all('option') if opt['value']
    ]
```

## 3. Препораки

- За секој бренд користи соодветен parser (PDF или HTML).
- Секојдневно фечирај и кеширај податоци во нова датумска папка.
- Користи GitHub Actions за автоматизација.
- Прикажи податоци преку GitHub Pages со можност за споредба по датум/маркет/бренд.

## 4. Следни чекори

- Имплементирај Python скрипта за фечирање и кеширање.
- Креирај GitHub Action workflow за автоматско пуштање.
- Дефинирај JSON/CSV schema за складирање на податоците.

---

*Овој водич е основа за автоматизација и јавен приказ на цени од супермаркети.*
