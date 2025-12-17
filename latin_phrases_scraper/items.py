# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class LatinPhraseItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # Esto es como un modelo o contrato.
    latin_phrase = scrapy.Field()        # La frase en latín
    translation = scrapy.Field()         # La traducción al inglés
    notes = scrapy.Field()               # Las notas explicativas (si existen)
