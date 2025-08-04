import requests
from bs4 import BeautifulSoup

url = "https://web.archive.org/web/20240521112004/https://www.thetimes.com/"
res = requests.get(url)
soup = BeautifulSoup(res.content, 'html.parser')
encabezados = soup.find_all(['h1', 'h2', 'h3'])

print(f"Total encabezados encontrados: {len(encabezados)}")
for h in encabezados[:10]:
    print(h.get_text(strip=True))
