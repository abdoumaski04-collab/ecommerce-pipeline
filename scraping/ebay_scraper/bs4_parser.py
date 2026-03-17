"""
bs4_parser.py — Parser HTML avancé avec BeautifulSoup
======================================================

ÉTAPE 2 DU PIPELINE : Extraction des données complexes
-------------------------------------------------------

RÔLE DE CE FICHIER :
    Scrapy extrait les données simples (titre, prix, condition).
    Ce module extrait les données COMPLEXES que les sélecteurs CSS
    de Scrapy ne peuvent pas atteindre proprement :
        - Spécifications techniques (CPU, RAM, stockage, écran...)
        - Variantes de prix (128GB vs 256GB vs 512GB)
        - Détails approfondis du vendeur
        - Texte de description nettoyé

PRINCIPE FONDAMENTAL :
    Ce module ne fait AUCUNE requête HTTP.
    Il reçoit le HTML déjà téléchargé par Scrapy (response.text)
    et le reparse avec BeautifulSoup pour en extraire plus.
    Zéro surcharge réseau, zéro requête supplémentaire.

INTÉGRATION DANS LE SPIDER :
    # Dans parse_product() de ebay_spider.py :
    bs_parser = EbayBeautifulSoupParser(response.text)
    bs_data   = bs_parser.extract_all()
    yield { ...données_scrapy..., **bs_data }

POURQUOI UN FICHIER SÉPARÉ ?
    Principe de Séparation des Responsabilités (SRP) :
    - ebay_spider.py = navigation + requêtes HTTP
    - bs4_parser.py  = parsing HTML complexe
    Si eBay change son HTML → on modifie uniquement bs4_parser.py
    sans risquer de casser la logique de navigation du spider.
"""

import re
import json
from bs4 import BeautifulSoup


class EbayBeautifulSoupParser:
    """
    Parser BeautifulSoup pour les pages produit eBay.

    UTILISATION :
        parser  = EbayBeautifulSoupParser(html_string)
        données = parser.extract_all()

    RÉSULTAT :
        Un dictionnaire plat avec tous les champs extraits,
        prêt à être mergé dans l'item Scrapy avec **bs_data.
    """

    # ------------------------------------------------------------------
    # INITIALISATION
    # ------------------------------------------------------------------

    def __init__(self, html: str):
        """
        Initialise le parser avec le HTML brut de la page.

        PARAMÈTRES :
            html : le contenu HTML complet de la page produit
                   (= response.text dans Scrapy)

        POURQUOI "html.parser" ET PAS "lxml" ?
            "html.parser" est le parser Python natif.
            Pas besoin d'installer une dépendance C (lxml).
            Légèrement plus lent mais suffisant pour notre usage.
            Si performance devient critique → remplacer par "lxml".
        """
        self.soup = BeautifulSoup(html, "html.parser")

    # ------------------------------------------------------------------
    # MÉTHODE PRINCIPALE
    # ------------------------------------------------------------------

    def extract_all(self) -> dict:
        """
        Appelle toutes les méthodes d'extraction et retourne un dict complet.

        POURQUOI UNE MÉTHODE "extract_all" ?
            Le spider n'a besoin de faire qu'UN SEUL appel :
                bs_data = parser.extract_all()
            Au lieu d'appeler chaque méthode séparément.
            Si on ajoute une nouvelle méthode d'extraction plus tard,
            on l'ajoute ici et le spider n'a pas besoin de changer.

        GESTION DES ERREURS :
            Chaque méthode est appelée dans un try/except.
            Si une extraction échoue (HTML inattendu), on retourne
            des valeurs None pour cette section et on continue.
            Un échec partiel ne doit jamais faire crasher le spider.
        """
        result = {}

        for method_name, method in [
            ("tech_specs",       self.extract_tech_specs),
            ("price_variants",   self.extract_price_variants),
            ("seller_details",   self.extract_seller_details),
            ("item_specifics",   self.extract_item_specifics),
            ("description",      self.extract_description_text),
        ]:
            try:
                data = method()
                result.update(data)
            except Exception as e:
                # On ne crash pas le spider pour une extraction qui échoue
                # On log juste l'erreur et on continue avec les autres sections
                print(f"[BS4Parser] Erreur dans {method_name}: {e}")
                continue

        return result

    # ------------------------------------------------------------------
    # MÉTHODE 1 : Spécifications techniques
    # ------------------------------------------------------------------

    def extract_tech_specs(self) -> dict:
        """
        Extrait les spécifications techniques du produit.

        DONNÉES EXTRAITES (exemples pour un laptop) :
            spec_processor → "Intel Core i7-1355U"
            spec_ram       → "16 GB"
            spec_storage   → "512 GB SSD"
            spec_screen    → "15.6 in"
            spec_os        → "Windows 11 Home"
            spec_brand     → "Dell"

        POURQUOI BEAUTIFULSOUP EST MEILLEUR ICI :
            Les specs sont dans des divs imbriquées sur 4-5 niveaux.
            BeautifulSoup's get_text() traverse tout l'arbre d'un coup
            peu importe la profondeur, là où Scrapy CSS devient fragile.

        STRUCTURE HTML TYPIQUE SUR EBAY :
            <div class="ux-layout-section-evo__col">
              <div class="ux-labels-values__labels">
                <span><span>Processor</span></span>   ← 2 niveaux
              </div>
              <div class="ux-labels-values__values">
                <span><span>Intel Core i7</span></span>
              </div>
            </div>

        MOTS-CLÉS RECHERCHÉS :
            On filtre uniquement les specs pertinentes pour l'analyse.
            "brand" et "model" → pour identifier le produit
            "processor/cpu"    → spec principale laptops/phones
            "ram/memory"       → variable de régression importante
            "storage/ssd/hdd"  → variable de régression importante
            "screen/display"   → taille écran → impact sur prix
            "battery"          → autonomie → impact sur prix phones
            "os/operating"     → Windows vs macOS → impact sur prix
        """
        specs = {}

        # Chercher toutes les sections qui contiennent des specs
        # re.compile() permet de chercher une classe qui CONTIENT ce pattern
        # (eBay a plusieurs noms de classes selon le layout de la page)
        spec_sections = self.soup.find_all(
            "div",
            class_=re.compile(
                r"ux-layout-section|x-about-this-item|section-specs|"
                r"ux-layout-section-evo"
            )
        )

        # Mots-clés des specs qui nous intéressent pour l'analyse
        RELEVANT_SPECS = {
            "processor": "spec_processor",
            "cpu":       "spec_processor",
            "ram":       "spec_ram",
            "memory":    "spec_ram",
            "storage":   "spec_storage",
            "ssd":       "spec_storage",
            "hdd":       "spec_storage",
            "screen":    "spec_screen_size",
            "display":   "spec_screen_size",
            "battery":   "spec_battery",
            "os":        "spec_os",
            "operating": "spec_os",
            "brand":     "spec_brand",
            "model":     "spec_model",
            "graphics":  "spec_gpu",
            "gpu":       "spec_gpu",
            "weight":    "spec_weight",
            "color":     "spec_color",
            "colour":    "spec_color",
            "network":   "spec_network",
            "connectivity": "spec_connectivity",
        }

        for section in spec_sections:
            # Trouver les lignes de specs dans cette section
            rows = section.find_all(
                "div",
                class_=re.compile(r"ux-labels-values")
            )

            for row in rows:
                # Extraire le label (ex: "Processor")
                label_el = row.find(
                    class_=re.compile(r"ux-labels-values__labels")
                )
                # Extraire la valeur (ex: "Intel Core i7-1355U")
                value_el = row.find(
                    class_=re.compile(r"ux-labels-values__values")
                )

                if not label_el or not value_el:
                    continue

                # get_text() = extrait tout le texte de l'élément ET ses enfants
                # strip=True = supprime les espaces en début/fin
                # separator=" " = met un espace entre les textes des enfants
                label = label_el.get_text(strip=True).lower()
                value = value_el.get_text(separator=" ", strip=True)

                if not label or not value:
                    continue

                # Chercher si ce label correspond à une spec pertinente
                for keyword, field_name in RELEVANT_SPECS.items():
                    if keyword in label:
                        # N'écraser que si le champ n'existe pas encore
                        # (on garde la première occurrence trouvée)
                        if field_name not in specs:
                            specs[field_name] = value
                        break

        # Toujours retourner les champs principaux, même si None
        # Pour que le CSV ait des colonnes cohérentes sur tous les produits
        defaults = {
            "spec_processor":  None,
            "spec_ram":        None,
            "spec_storage":    None,
            "spec_screen_size": None,
            "spec_brand":      None,
            "spec_os":         None,
        }
        # Merge : les valeurs trouvées remplacent les None par défaut
        return {**defaults, **specs}

    # ------------------------------------------------------------------
    # MÉTHODE 2 : Variantes de prix
    # ------------------------------------------------------------------

    def extract_price_variants(self) -> dict:
        """
        Extrait les variantes de prix (tailles, couleurs, configurations).

        POURQUOI C'EST IMPORTANT POUR L'ANALYSE :
            Un iPhone peut être vendu en 3 configurations :
                128GB → $699  |  256GB → $799  |  512GB → $999
            Si on scrape seulement le prix par défaut (128GB = $699),
            on rate les 2 autres prix du MÊME produit.
            Avec les variantes, on capture toute la gamme de prix.

            Cela enrichit la régression :
            prix ~ stockage + RAM + ... (specs extraites de GSMArena)

        DONNÉES RETOURNÉES :
            price_variants      → JSON string des variantes
                                  ex: [{"type":"Storage","value":"256GB","price":799.0}]
            variants_count      → nombre de variantes (int)
            variant_price_min   → prix le plus bas parmi les variantes
            variant_price_max   → prix le plus haut parmi les variantes
            variant_price_range → écart max-min (mesure de la gamme de prix)

        STRUCTURE HTML EBAY :
            <div class="x-msku__select-box">
                <select>
                    <option value="128GB" data-price="699.00">128GB</option>
                    <option value="256GB" data-price="799.00">256GB</option>
                </select>
            </div>
        """
        all_variants = []

        # Chercher tous les sélecteurs de variantes (dropdown)
        select_boxes = self.soup.find_all(
            "div",
            class_=re.compile(r"x-msku__select-box|variation-select")
        )

        for box in select_boxes:
            select = box.find("select")
            if not select:
                continue

            # Trouver le nom de ce sélecteur (ex: "Storage", "Color")
            label_el = box.find_previous(
                class_=re.compile(r"x-msku__label|variation-label")
            )
            variant_type = label_el.get_text(strip=True) if label_el else "variant"

            # Parcourir toutes les options du dropdown
            for option in select.find_all("option"):
                text = option.get_text(strip=True)

                # Ignorer les placeholders
                if not text or text.lower() in ("select", "choose", "--", "- select -"):
                    continue

                # Le prix de cette variante est souvent dans un attribut data-*
                price_attr = (
                    option.get("data-price")
                    or option.get("data-selectbox-price")
                    or option.get("data-raw-price")
                )

                all_variants.append({
                    "type":  variant_type,
                    "value": text,
                    "price": float(price_attr) if price_attr else None,
                })

        # Chercher aussi les variantes sous forme de boutons radio
        # (eBay utilise parfois des boutons au lieu d'un dropdown)
        radio_sections = self.soup.find_all(
            "div",
            class_=re.compile(r"x-msku__radio-row|msku-sel-label")
        )
        for section in radio_sections:
            labels = section.find_all(
                class_=re.compile(r"x-msku__radio-label|cbx-lbl")
            )
            for label in labels:
                text = label.get_text(strip=True)
                if text:
                    all_variants.append({
                        "type":  "option",
                        "value": text,
                        "price": None,  # pas de prix direct sur les boutons radio
                    })

        # Construire le résultat
        if all_variants:
            prices = [v["price"] for v in all_variants if v["price"] is not None]
            return {
                "price_variants":      json.dumps(all_variants),
                "variants_count":      len(all_variants),
                "variant_price_min":   round(min(prices), 2) if prices else None,
                "variant_price_max":   round(max(prices), 2) if prices else None,
                "variant_price_range": round(max(prices) - min(prices), 2) if prices else None,
            }
        else:
            return {
                "price_variants":      None,
                "variants_count":      0,
                "variant_price_min":   None,
                "variant_price_max":   None,
                "variant_price_range": None,
            }

    # ------------------------------------------------------------------
    # MÉTHODE 3 : Détails approfondis du vendeur
    # ------------------------------------------------------------------

    def extract_seller_details(self) -> dict:
        """
        Extrait les informations détaillées du vendeur.

        DONNÉES BASIQUES déjà extraites par Scrapy :
            seller_name  → nom du vendeur
            seller_score → score affiché

        DONNÉES APPROFONDIES extraites ici par BeautifulSoup :
            seller_total_sales  → nombre total de ventes (ex: 15234)
            seller_positive_pct → % de feedback positif (ex: 99.8)
            seller_top_rated    → badge "Top Rated Seller" (bool)
            seller_country      → pays du vendeur

        POURQUOI CES DONNÉES POUR L'ANALYSE ?
            Question statistique intéressante :
            "Les Top Rated Sellers vendent-ils plus cher ?"
            "Le nombre de ventes du vendeur influence-t-il le prix ?"
            Ces variables enrichissent la régression et donnent
            des insights business concrets.

        COMMENT ON EXTRAIT LE NOMBRE DE VENTES ?
            eBay écrit "15,234 sold" ou "15.2K items sold" dans le HTML.
            On utilise une regex pour extraire ce nombre et le convertir
            en entier propre. C'est impossible avec un sélecteur CSS seul.
        """
        result = {
            "seller_total_sales":   None,
            "seller_positive_pct":  None,
            "seller_top_rated":     False,
            "seller_country":       None,
        }

        # Localiser le bloc vendeur sur la page
        seller_section = (
            self.soup.find("div", class_=re.compile(r"x-sellercard-atf"))
            or self.soup.find("div", class_=re.compile(r"mbg-wrapper"))
            or self.soup.find("div", attrs={"data-testid": re.compile(r"seller")})
        )

        if not seller_section:
            return result

        # Tout le texte du bloc vendeur en une fois
        full_text = seller_section.get_text(separator=" ")

        # --- Nombre total de ventes ---
        # Patterns possibles : "15,234 sold", "15.2K items sold", "15k sold"
        sales_match = re.search(
            r"([\d,]+\.?\d*)\s*[kK]?\s*(?:items?\s*)?sold",
            full_text
        )
        if sales_match:
            raw = sales_match.group(1).replace(",", "")
            # Gérer les "k" (milliers) : "15.2k" → 15200
            if "k" in sales_match.group(0).lower():
                result["seller_total_sales"] = int(float(raw) * 1000)
            else:
                try:
                    result["seller_total_sales"] = int(float(raw))
                except ValueError:
                    pass

        # --- Pourcentage de feedback positif ---
        # Pattern : "99.8% positive feedback"
        feedback_match = re.search(
            r"([\d.]+)%\s*positive",
            full_text,
            re.IGNORECASE
        )
        if feedback_match:
            result["seller_positive_pct"] = float(feedback_match.group(1))

        # --- Badge Top Rated Seller ---
        # Si l'élément avec ce badge existe → True, sinon → False
        top_rated = seller_section.find(
            class_=re.compile(r"top-rated|TopRated|trs-badge|top_rated")
        )
        result["seller_top_rated"] = top_rated is not None

        # --- Pays du vendeur ---
        location_el = seller_section.find(
            class_=re.compile(r"seller-location|mbg-loc|x-sellercard-atf__info__about-seller")
        )
        if location_el:
            result["seller_country"] = location_el.get_text(strip=True)

        return result

    # ------------------------------------------------------------------
    # MÉTHODE 4 : Item Specifics eBay
    # ------------------------------------------------------------------

    def extract_item_specifics(self) -> dict:
        """
        Extrait la section "Item Specifics" standardisée d'eBay.

        QU'EST-CE QUE "ITEM SPECIFICS" ?
            eBay oblige les vendeurs à remplir des champs standardisés
            pour décrire leurs produits : marque, modèle, couleur, etc.
            Ces données sont plus fiables que les specs techniques
            car eBay les contrôle et les standardise.

        DIFFÉRENCE AVEC extract_tech_specs() :
            tech_specs     → section "About this product" (données techniques)
            item_specifics → section "Item specifics" (données commerciales)
            Les deux peuvent coexister sur la même page.

        DONNÉES TYPIQUES :
            info_brand        → "Apple"
            info_model        → "iPhone 14"
            info_color        → "Midnight Black"
            info_storage      → "256 GB"
            info_connectivity → "5G"
            info_mpn          → numéro de pièce fabricant

        POURQUOI C'EST UTILE ?
            Ces données sont très propres et standardisées par eBay.
            Elles complètent les specs techniques qui peuvent être
            incomplètes ou mal formatées selon le vendeur.
        """
        specifics = {}

        # Chercher la section "Item specifics" par son titre
        # On cherche un tag texte contenant "item specifics"
        header = self.soup.find(
            lambda tag: (
                tag.name in ("h2", "h3", "h4", "span", "div")
                and "item specifics" in tag.get_text(strip=True).lower()
            )
        )

        if not header:
            return {}

        # Remonter au parent section qui contient les données
        section = header.find_parent(
            "div",
            class_=re.compile(r"ux-layout-section|section-specifics")
        )
        if not section:
            # Essayer de prendre le parent direct
            section = header.parent

        if not section:
            return {}

        # Extraire toutes les paires label/valeur
        # Strategy A : div avec labels et values séparés
        rows = section.find_all(
            "div",
            class_=re.compile(r"ux-labels-values$|x-about-this-item")
        )

        for row in rows:
            # Labels en gras (BOLD)
            label_els = row.find_all(
                "span",
                class_=re.compile(r"BOLD|ux-textspans--BOLD")
            )
            # Valeurs (non-gras)
            value_els = row.find_all(
                "span",
                class_=lambda c: c and "ux-textspans" in c and "BOLD" not in c
            )

            for label_el, value_el in zip(label_els, value_els):
                k = label_el.get_text(strip=True)
                v = value_el.get_text(strip=True)

                if k and v:
                    # Nettoyer la clé : "Storage Capacity" → "info_storage_capacity"
                    clean_key = "info_" + re.sub(r"[^a-z0-9]+", "_", k.lower()).strip("_")
                    specifics[clean_key] = v

        # Strategy B : tableau HTML (certains vendeurs utilisent des <table>)
        if not specifics:
            tables = section.find_all("table")
            for table in tables:
                for row in table.find_all("tr"):
                    cells = row.find_all(["th", "td"])
                    if len(cells) >= 2:
                        k = cells[0].get_text(strip=True)
                        v = cells[1].get_text(strip=True)
                        if k and v:
                            clean_key = "info_" + re.sub(r"[^a-z0-9]+", "_", k.lower()).strip("_")
                            specifics[clean_key] = v

        return specifics

    # ------------------------------------------------------------------
    # MÉTHODE 5 : Description textuelle nettoyée
    # ------------------------------------------------------------------

    def extract_description_text(self) -> dict:
        """
        Extrait et nettoie la description textuelle du produit.

        POURQUOI EXTRAIRE LA DESCRIPTION ?
            1. Détecter des mots-clés qui expliquent un prix anormal :
               "scratches", "cracked screen", "dead pixels" → prix bas
               "brand new sealed", "never opened" → prix plus haut que Used
            2. Longueur de description : les vendeurs sérieux écrivent plus
               → corrélation avec seller_top_rated et prix ?
            3. Analyse de sentiment basique sur la qualité perçue

        DÉFI TECHNIQUE :
            La description eBay est souvent dans un <iframe> séparé.
            L'iframe charge une autre URL — Scrapy ne l'a pas téléchargée.
            Quand l'iframe n'est pas disponible, on cherche la description
            dans les divs alternatives de la page principale.

        NETTOYAGE :
            get_text() extrait TOUT le texte de l'arbre HTML, en traversant
            tous les enfants quelle que soit la profondeur.
            On nettoie ensuite les espaces multiples et on tronque à 500 chars
            pour ne pas surcharger le CSV et Bigtable.
        """
        desc_el = (
            self.soup.find("div", attrs={"id": "desc_div"})
            or self.soup.find("div", class_=re.compile(r"item-description|d-item-description"))
            or self.soup.find(
                "div",
                attrs={"data-testid": re.compile(r"description|ux-layout-section.*desc")}
            )
        )

        if not desc_el:
            return {
                "description_text":   None,
                "description_length": 0,
                "has_warning_keywords": False,
            }

        # Extraire tout le texte (separator=" " évite les mots collés)
        raw_text   = desc_el.get_text(separator=" ", strip=True)
        clean_text = re.sub(r"\s+", " ", raw_text).strip()

        # Détecter des mots-clés négatifs qui pourraient expliquer un prix bas
        # Utile pour l'analyse : "les produits avec mentions de défauts
        # sont-ils vendus significativement moins cher ?" (t-test)
        WARNING_KEYWORDS = [
            "scratch", "crack", "broken", "damaged", "repair",
            "parts only", "for parts", "dead pixel", "screen issue",
            "dent", "chip", "worn", "stain", "missing",
        ]
        has_warning = any(
            kw in clean_text.lower() for kw in WARNING_KEYWORDS
        )

        return {
            # Tronqué à 500 chars — suffisant pour les mots-clés,
            # pas trop lourd pour le CSV et Bigtable
            "description_text":     clean_text[:500] if clean_text else None,
            # Longueur TOTALE avant troncature (variable pour régression)
            "description_length":   len(clean_text),
            # Bool : est-ce que la description mentionne des défauts ?
            "has_warning_keywords": has_warning,
        }