# daily-cache-ceni (refined)

## Опис

Автоматско дневно собирање и кеширање на цени од супермаркети (КАМ, Тинекс, Веро, Стокомак) за јавен приказ и споредба преку GitHub Pages.

## Поддржани супермаркети и формати

| Бренд      | Локации/ID | Извор/Формат         | Пример URL за феч |
|------------|------------|----------------------|-------------------|
| КАМ        | Име, адреса, линк | HTML листа + PDF | <https://kam.com.mk/price_markets/{market_id}/> |
| Тинекс     | org ID      | HTML, пагинација     | <http://ceni.tinex.mk/?org={id}&perPage=100> |
| Веро       | HTML страници| HTML, пагинација     | <https://pricelist.vero.com.mk/{id}_1.html> |
| Стокомак   | org ID      | HTML, пагинација     | <https://stokomak.proverkanaceni.mk/?org={id}&perPage=100> |

## Пример JSON структура за мапирање на маркети

```json
[
  {
    "brand": "КАМ",
    "name": "Маџари",
    "address": "ул.Благоја Стефковски бр.12",
    "url": "https://kam.com.mk/price_markets/madzari/"
  },
  {
    "brand": "Тинекс",
    "id": 511,
    "name": "КАРПОШ-3",
    "url": "http://ceni.tinex.mk/?org=511&perPage=100"
  },
  {
    "brand": "Веро",
    "id": "89_1",
    "name": "ВЕРО 1",
    "url": "https://pricelist.vero.com.mk/89_1.html"
  },
  {
    "brand": "Стокомак",
    "id": 3,
    "name": "КАРПОШ 2",
    "url": "https://stokomak.proverkanaceni.mk/?org=3&perPage=100"
  }
]
```

## Пример Python schema за фечирање на ID/локации

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

## Како се фечираат податоците

- За секој бренд се скенираат ID/локации од изворната страна.
- Се мапираат во JSON/CSV.
- Се фечира ценовникот (HTML/PDF) за секој маркет.
- Се чува во cache/{YYYY-MM-DD}/{brand}_{market_id}.csv

## Архива и споредба

- Секојдневно се креира нова папка со датум.
- Старите податоци се достапни за споредба преку HTML интерфејс.

## Автоматизација

- GitHub Actions workflow за дневно фечирање и пуштање на нови податоци.
- GitHub Pages за јавен приказ.
 Стар README содржината е преместена и прочистена во оваа табела и пример структура
