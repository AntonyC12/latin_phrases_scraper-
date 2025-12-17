import scrapy
from latin_phrases_scraper.items import LatinPhraseItem
import re

class WikipediaLatinSpider(scrapy.Spider):
    name = "wikipedia_latin"  # Nombre único para correr el spider
    allowed_domains = ["en.wikipedia.org"]
    
    # ¡AQUÍ ES DONDE PONDRÁS LA URL CORRECTA!
    # Este spider está listo para scrapear la tabla de frases.
    start_urls = ["https://en.wikipedia.org/wiki/List_of_Latin_phrases_(full)"]

    def parse(self, response):
        """
        Método principal que Scrapy llama para procesar la respuesta.
        Aquí definimos las reglas para extraer los datos de la página HTML.
        """
        # Buscamos TODAS las tablas en la página. Este selector es genérico.
        # Para una página específica, podríamos afinar el selector (ej: 'table.wikitable').
        tables = response.css('table.wikitable')
        
        for table in tables:
            # Iteramos por cada fila (<tr>) de la tabla, saltando el encabezado
            rows = table.css('tr')[1:]  # [1:] omite la primera fila (headers)
            
            for row in rows:
                # Creamos un ítem para llenar con nuestros datos
                item = LatinPhraseItem()
                
                # Extraemos el texto de las celdas (<td>). Usamos .get() para el primero y .getall() para posibles notas.
                # Estos selectores CSS son el patrón a seguir para otras páginas.
                cells = row.css('td')
                
                if len(cells) >= 2:  # Aseguramos que haya al menos 2 celdas (latín y traducción)
                    item['latin_phrase'] = cells[0].css('::text').get("").strip()
                    item['translation'] = cells[1].css('::text').get("").strip()
                    # Unimos el texto de todas las celdas restantes como "notas"
                    item['notes'] = " ".join([cell.css('::text').get("") for cell in cells[2:]]).strip()
                    
                    # Pasamos el ítem a nuestro pipeline para procesarlo
                    yield item
