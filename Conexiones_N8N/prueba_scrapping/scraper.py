def extraer_titulares(snapshot_url, fecha_str):
    titulares = []
    page = requests.get(snapshot_url, timeout=SNAPSHOT_TIMEOUT)
    soup = BeautifulSoup(page.content, 'html.parser')
    for t in soup.find_all(['h1', 'h2', 'h3']):
        clases = " ".join(t.get('class', []))
        texto = t.get_text(strip=True)
        if texto and (any(c in clases for c in [
            'gs-c-promo-heading__title',
            'lx-stream-post__header-title',
            'ssrcss-6arcww-PromoHeadline'
        ]) or len(texto.split()) > 3):
            titulares.append({
                "fecha": fecha_str,
                "titular": texto,
                "url_archivo": snapshot_url,
                "fuente": "BBC",
                "idioma": "en"
            })
    return titulares
