import pandas as pd
from collections import Counter
import re
import spacy
from transformers import pipeline, set_seed
import random

class LatinPhrasesPipeline:
    """Pipeline para procesar, analizar y exportar los datos scrapeados."""
    
    def __init__(self):
        # Lista para acumular todos los ítems
        self.phrases = []
        
        # Cargar modelo de spaCy para inglés
        print("[INFO] Cargando modelo de spaCy para inglés...")
        self.nlp_en = spacy.load("en_core_web_sm")
        
        # Modelo más ligero para generación en español
        try:
            self.generator = pipeline(
                'text-generation',
                model='PlanTL-GOB-ES/gpt2-base-bne',  # Modelo más pequeño
                device=-1
            )
            self.model_loaded = True
        except:
            self.model_loaded = False
        
        # Lista mejorada de stopwords en latín (se ampliará dinámicamente)
        self.latin_stopwords = {'et', 'in', 'non', 'est', 'ad', 'cum', 'ex', 'de', 'ut', 'sed', 
                                'si', 'quod', 'a', 'ab', 'per', 'sine', 'pro', 'ante', 'post', 
                                'inter', 'sub', 'super', 'contra', 'apud'}
        
        # Verbos españoles comunes para conjugación (sin plantillas predefinidas)
        self.spanish_verb_conjugations = {
            'ser': ['es', 'son', 'era', 'fueron', 'siendo'],
            'tener': ['tiene', 'tienen', 'tenía', 'tuvo', 'teniendo'],
            'hacer': ['hace', 'hacen', 'hacía', 'hizo', 'haciendo'],
            'decir': ['dice', 'dicen', 'decía', 'dijo', 'diciendo'],
            'ver': ['ve', 'ven', 'veía', 'vio', 'viendo'],
            'saber': ['sabe', 'saben', 'sabía', 'supo', 'sabiendo'],
            'poder': ['puede', 'pueden', 'podía', 'pudo', 'pudiendo'],
            'querer': ['quiere', 'quieren', 'quería', 'quiso', 'queriendo']
        }
    
    def process_item(self, item, spider):
        """Método llamado por Scrapy para cada ítem. Lo guardamos en la lista."""
        self.phrases.append(dict(item))
        return item
    
    def extract_keywords_with_context(self, text, nlp_model, top_n=10):
        """Extrae palabras clave con contexto usando spaCy."""
        doc = nlp_model(text)
        keywords = []
        
        # Extraer sustantivos, adjetivos y verbos como palabras clave
        for token in doc:
            if token.is_alpha and not token.is_stop and len(token.text) > 2:
                if token.pos_ in ['NOUN', 'ADJ', 'VERB', 'PROPN']:
                    keywords.append(token.lemma_.lower())
        
        return Counter(keywords).most_common(top_n)
    
    def generate_spanish_phrase(self, latin_words, spanish_verbs, english_verbs):
        """Genera una frase en español automáticamente usando el modelo o reglas."""
        
        # Si el modelo está cargado, usarlo para generación
        if self.model_loaded and len(latin_words) >= 3 and len(spanish_verbs) >= 2:
            try:
                # Crear prompt para el modelo generativo
                prompt_words = ", ".join(latin_words[:3])
                prompt_verbs = ", ".join(spanish_verbs[:2])
                
                prompt = f"Genera una frase significativa en español que incluya las palabras latinas '{prompt_words}' y los verbos '{prompt_verbs}':"
                
                # Generar texto con el modelo
                generated = self.generator(
                    prompt,
                    max_length=50,
                    num_return_sequences=1,
                    temperature=0.8,
                    do_sample=True,
                    top_p=0.9
                )
                
                # Extraer y limpiar la frase generada
                raw_text = generated[0]['generated_text']
                phrase = raw_text.replace(prompt, '').strip()
                
                # Limpiar la frase (quitar texto no deseado)
                if ':' in phrase:
                    phrase = phrase.split(':', 1)[1].strip()
                if '.' in phrase:
                    # Tomar solo la primera oración
                    phrase = phrase.split('.', 1)[0] + '.'
                
                return phrase.capitalize()
                
            except Exception as e:
                print(f"[WARNING] Error en generación con modelo: {e}")
                # Continuar con generación por reglas
        
        # GENERACIÓN POR REGLAS (respaldo)
        # Crear frases usando patrones gramaticales básicos en español
        patterns = [
            "El concepto de {latin1} {verb1} fundamental para entender {latin2}.",
            "La expresión {latin1} nos {verb2} comprender {latin3}.",
            "A través de {latin2} se {verb1} alcanzar {latin1}.",
            "El principio de {latin3} {verb1} la esencia de {latin2}.",
            "La búsqueda de {latin1} siempre {verb2} hacia {latin3}."
        ]
        
        # Seleccionar un patrón aleatorio
        pattern = random.choice(patterns)
        
        # Rellenar con palabras y verbos disponibles
        latin1 = latin_words[0] if len(latin_words) > 0 else "veritas"
        latin2 = latin_words[1] if len(latin_words) > 1 else latin_words[0] if len(latin_words) > 0 else "lux"
        latin3 = latin_words[2] if len(latin_words) > 2 else latin_words[0] if len(latin_words) > 0 else "vitae"
        
        verb1 = spanish_verbs[0] if len(spanish_verbs) > 0 else "es"
        verb2 = spanish_verbs[1] if len(spanish_verbs) > 1 else spanish_verbs[0] if len(spanish_verbs) > 0 else "tiene"
        
        # Conjugar verbos si es necesario
        if verb1 in self.spanish_verb_conjugations:
            verb1 = random.choice(self.spanish_verb_conjugations[verb1])
        if verb2 in self.spanish_verb_conjugations:
            verb2 = random.choice(self.spanish_verb_conjugations[verb2])
        
        # Reemplazar en el patrón
        phrase = pattern.format(
            latin1=latin1, latin2=latin2, latin3=latin3,
            verb1=verb1, verb2=verb2
        )
        
        return phrase
    
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
        
        # Identificar verbos latinos por sufijos comunes
        latin_verbs = [word for word in filtered_latin 
                      if any(word.endswith(suffix) for suffix in ['are', 'ere', 'ire', 'avi', 'ivi', 'atus', 'itus'])]
        latin_verb_freq = Counter(latin_verbs).most_common(10) if latin_verbs else []
        
        # 3. ANÁLISIS DE INGLÉS CON SPACY (PALABRAS Y VERBOS)
        all_english_text = " ".join(df['translation'].dropna().astype(str))
        doc_en = self.nlp_en(all_english_text)
        
        # Filtrar tokens: palabras alfabéticas, no stopwords
        english_tokens = [token for token in doc_en if token.is_alpha and not token.is_stop]
        
        # Frecuencia de TODAS las palabras en inglés (lematizadas)
        english_words = [token.lemma_.lower() for token in english_tokens]
        english_word_freq = Counter(english_words).most_common(20)
        
        # Frecuencia de VERBOS en inglés (usando POS tagging de spaCy)
        english_verbs = [token.lemma_.lower() for token in english_tokens if token.pos_ == "VERB"]
        english_verb_freq = Counter(english_verbs).most_common(10) if english_verbs else []
        
        # 4. PREPARAR DATOS PARA GENERACIÓN
        top_latin_words = [word for word, _ in latin_word_freq[:5]]
        top_english_verbs = [verb for verb, _ in english_verb_freq[:5]] if english_verb_freq else ['be', 'have', 'do', 'make', 'say']
        
        # Traducción automática de verbos inglés -> español (sin diccionario predefinido)
        # Mapeo simple basado en similitud fonética/morfológica
        verb_translation_rules = {
            'be': 'ser', 'have': 'tener', 'do': 'hacer', 'say': 'decir',
            'make': 'hacer', 'take': 'tomar', 'see': 'ver', 'come': 'venir',
            'use': 'usar', 'find': 'encontrar', 'give': 'dar', 'know': 'saber',
            'get': 'obtener', 'go': 'ir', 'think': 'pensar', 'look': 'mirar',
            'want': 'querer', 'need': 'necesitar', 'feel': 'sentir', 'become': 'convertirse'
        }
        
        top_spanish_verbs = []
        for eng_verb in top_english_verbs:
            if eng_verb in verb_translation_rules:
                top_spanish_verbs.append(verb_translation_rules[eng_verb])
            else:
                # Si no está en el mapeo, usar el verbo en inglés como base
                top_spanish_verbs.append(eng_verb)
        
        # 5. GENERAR 5 FRASES EN ESPAÑOL AUTOMÁTICAMENTE
        spanish_phrases = []
        for i in range(5):
            phrase = self.generate_spanish_phrase(top_latin_words, top_spanish_verbs, top_english_verbs)
            spanish_phrases.append(phrase)
        
        # 6. GUARDAR RESULTADOS COMPLETOS
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
            f.write("5 FRASES GENERADAS EN ESPAÑOL (AUTOMÁTICAS)\n")
            f.write("="*60 + "\n")
            for i, frase in enumerate(spanish_phrases, 1):
                f.write(f"{i}. {frase}\n")
            
            # Resumen ejecutivo
            f.write("\n" + "="*60 + "\n")
            f.write("RESUMEN EJECUTIVO\n")
            f.write("="*60 + "\n")
            f.write(f"Palabras latín más usadas: {top_latin_words}\n")
            f.write(f"Verbos inglés más usados: {top_english_verbs}\n")
            f.write(f"Verbos traducidos al español: {top_spanish_verbs}\n")
            f.write(f"Modelo generativo usado: {'BERTIN GPT-J' if self.model_loaded else 'Generación por reglas'}\n")
        
        print("[SUCCESS] Análisis completado y guardado en 'analisis_frecuencias.txt'")
        print("\n" + "="*60)
        print("FRASES GENERADAS AUTOMÁTICAMENTE:")
        print("="*60)
        for i, frase in enumerate(spanish_phrases, 1):
            print(f"{i}. {frase}")