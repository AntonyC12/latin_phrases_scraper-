import pandas as pd
from collections import Counter
import re
import spacy

class LatinPhrasesPipeline:
    """Pipeline para procesar, analizar y exportar los datos scrapeados."""
    
    def __init__(self):
        # Lista para acumular todos los ítems
        self.phrases = []
        
        # Cargar modelo de spaCy para inglés
        print("[INFO] Cargando modelo de spaCy para inglés...")
        self.nlp_en = spacy.load("en_core_web_sm")
        
        # Lista mejorada de stopwords en latín (se ampliará dinámicamente)
        self.latin_stopwords = {'et', 'in', 'non', 'est', 'ad', 'cum', 'ex', 'de', 'ut', 'sed', 
                                'si', 'quod', 'a', 'ab', 'per', 'sine', 'pro', 'ante', 'post', 
                                'inter', 'sub', 'super', 'contra', 'apud'}
    
    def process_item(self, item, spider):
        """Método llamado por Scrapy para cada ítem. Lo guardamos en la lista."""
        self.phrases.append(dict(item))
        return item
    
    def close_spider(self, spider):
        """Método llamado cuando el spider termina. Aquí hacemos el análisis final."""
        print(f"\n[INFO] Spider finalizado. Se recolectaron {len(self.phrases)} frases.")
        
        if not self.phrases:
            print("[WARNING] No se recolectaron datos.")
            return
        
        # 1. Crear DataFrame y guardar en Excel
        df = pd.DataFrame(self.phrases)
        output_file = "latin_phrases_analysis.xlsx"
        df.to_excel(output_file, index=False)
        print(f"[SUCCESS] Datos guardados en '{output_file}'")
        
        # 2. ANÁLISIS DE LATÍN
        all_latin_text = " ".join(df['latin_phrase'].dropna().astype(str))
        
        # Extraer palabras latinas (palabras de 3+ letras)
        latin_words = re.findall(r'\b[a-z]{3,}\b', all_latin_text.lower())
        
        # Filtrar stopwords dinámicamente: añadir palabras de 1-2 ocurrencias
        word_counts = Counter(latin_words)
        rare_words = {word for word, count in word_counts.items() if count <= 2}
        self.latin_stopwords.update(rare_words)
        
        filtered_latin = [w for w in latin_words if w not in self.latin_stopwords]
        latin_word_freq = Counter(filtered_latin).most_common(20)
        
        # Identificar verbos latinos por sufijos comunes (sin lista predefinida)
        latin_verbs = [word for word in filtered_latin 
                      if any(word.endswith(suffix) for suffix in ['are', 'ere', 'ire', 'avi', 'ivi', 'atus', 'itus'])]
        latin_verb_freq = Counter(latin_verbs).most_common(10) if latin_verbs else []
        
        # 3. ANÁLISIS DE INGLÉS CON SPACY
        all_english_text = " ".join(df['translation'].dropna().astype(str))
        doc_en = self.nlp_en(all_english_text)
        
        # Filtrar tokens: palabras alfabéticas, no stopwords
        english_tokens = [token for token in doc_en if token.is_alpha and not token.is_stop]
        
        # Frecuencia de palabras (lematizadas)
        english_words = [token.lemma_.lower() for token in english_tokens]
        english_word_freq = Counter(english_words).most_common(20)
        
        # Frecuencia de VERBOS (usando POS tagging de spaCy)
        english_verbs = [token.lemma_.lower() for token in english_tokens if token.pos_ == "VERB"]
        english_verb_freq = Counter(english_verbs).most_common(10) if english_verbs else []
        
        # 4. GENERACIÓN DE FRASES EN ESPAÑOL
        top_latin_words = [word for word, _ in latin_word_freq[:5]]
        top_english_verbs = [verb for verb, _ in english_verb_freq[:3]] if english_verb_freq else ['be', 'have', 'do']
        
        # Traducción básica sin diccionario predefinido (solo los más comunes)
        verb_translation_map = {
            'be': 'es', 'have': 'tiene', 'do': 'hace', 'say': 'dice',
            'make': 'hace', 'take': 'toma', 'see': 've', 'come': 'viene',
            'use': 'usa', 'find': 'encuentra', 'give': 'da', 'know': 'sabe',
            'get': 'obtiene', 'go': 'va', 'think': 'piensa', 'look': 'mira'
        }
        
        top_spanish_verbs = [verb_translation_map.get(verb, verb) for verb in top_english_verbs]
        
        # Plantillas de frases 100% en español
        templates_es = [
            f"El concepto latino '{top_latin_words[0]}' {top_spanish_verbs[0]} fundamental en la filosofía antigua.",
            f"La expresión '{top_latin_words[1]}' nos {top_spanish_verbs[1]} reflexionar sobre el tiempo.",
            f"Mediante '{top_latin_words[2]}' se {top_spanish_verbs[2]} entender el conocimiento clásico.",
            f"El principio de '{top_latin_words[3]}' {top_spanish_verbs[0]} la base de la jurisprudencia romana.",
            f"La búsqueda de '{top_latin_words[4]}' {top_spanish_verbs[1]} en el centro de la retórica antigua."
        ]
        
        # 5. GUARDAR RESULTADOS COMPLETOS
        with open("analisis_frecuencias.txt", "w", encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("ANÁLISIS COMPLETO DE FRECUENCIAS\n")
            f.write("="*60 + "\n\n")
            
            f.write("TOP 20 PALABRAS EN LATÍN (excluyendo stopwords):\n")
            for word, freq in latin_word_freq:
                f.write(f"  {word}: {freq}\n")
            
            f.write("\nTOP 10 VERBOS EN LATÍN (identificados por sufijos):\n")
            if latin_verb_freq:
                for verb, freq in latin_verb_freq:
                    f.write(f"  {verb}: {freq}\n")
            else:
                f.write("  No se identificaron verbos claros por sufijos\n")
            
            f.write("\nTOP 20 PALABRAS EN INGLÉS (lematizadas, sin stopwords):\n")
            for word, freq in english_word_freq:
                f.write(f"  {word}: {freq}\n")
            
            f.write("\nTOP 10 VERBOS EN INGLÉS (lematizados):\n")
            if english_verb_freq:
                for verb, freq in english_verb_freq:
                    f.write(f"  {verb}: {freq}\n")
            else:
                f.write("  No se identificaron verbos en inglés\n")
            
            f.write("\n" + "="*60 + "\n")
            f.write("5 FRASES GENERADAS EN ESPAÑOL\n")
            f.write("="*60 + "\n")
            for i, frase in enumerate(templates_es, 1):
                f.write(f"{i}. {frase}\n")
            
            # Resumen ejecutivo
            f.write("\n" + "="*60 + "\n")
            f.write("RESUMEN EJECUTIVO\n")
            f.write("="*60 + "\n")
            f.write(f"Palabras latín más usadas: {top_latin_words}\n")
            f.write(f"Verbos inglés más usados: {top_english_verbs}\n")
            f.write(f"Verbos traducidos al español: {top_spanish_verbs}\n")
        
        print("[SUCCESS] Análisis completado y guardado en 'analisis_frecuencias.txt'")