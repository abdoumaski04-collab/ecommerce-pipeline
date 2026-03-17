"""
ebay_spider.py — Spider principal eBay
=======================================

ÉTAPE 1 DU PIPELINE : Collecte des données brutes sur eBay
-----------------------------------------------------------

CE QUE FAIT CE FICHIER :
    Ce spider visite automatiquement eBay, cherche des produits
    électroniques (smartphones, laptops, tablets), navigue les pages
    de résultats, visite chaque page produit, et extrait toutes les
    données utiles pour l'analyse statistique.

FLUX D'EXÉCUTION :
    start_requests()
        → génère les URLs de recherche eBay (une par catégorie)
    parse_search_results()
        → extrait les liens vers chaque produit sur la page
        → génère la requête vers la page suivante (pagination)
    parse_product()
        → visite chaque page produit
        → extrait : titre, prix, condition, vendeur, rating, reviews, timestamp
        → retourne un item dict propre

COMMANDES :
    # Depuis le dossier scraping/
    scrapy crawl ebay_electronics
    scrapy crawl ebay_electronics -a category=laptops -a max_pages=3
    scrapy crawl ebay_electronics -o data/output.csv
"""

import scrapy
import re
from datetime import datetime
from urllib.parse import urlencode

from ebay_scraper.bs4_parser import EbayBeautifulSoupParser


class EbayElectronicsSpider(scrapy.Spider):
    """
    Spider principal pour scraper les produits électroniques sur eBay.

    POURQUOI HÉRITER DE scrapy.Spider ?
    scrapy.Spider est la classe de base de Scrapy. Elle fournit
    automatiquement : la gestion des requêtes HTTP, les retries,
    la gestion des délais, le logging, et l'export des données.
    On hérite de cette classe pour bénéficier de tout ça gratuitement
    et ne coder que la logique spécifique à eBay.
    """

    # ------------------------------------------------------------------
    # IDENTITÉ DU SPIDER
    # ------------------------------------------------------------------

    # Identifiant unique utilisé dans la commande : scrapy crawl ebay_electronics
    name = "ebay_electronics"

    # Sécurité : le spider ne peut PAS sortir de ebay.com par accident
    # Si un lien pointe vers un autre domaine, Scrapy l'ignore automatiquement
    allowed_domains = ["ebay.com"]

    # ------------------------------------------------------------------
    # CATÉGORIES CIBLES
    # ------------------------------------------------------------------
    # Dictionnaire des catégories à scraper.
    #
    # keyword     : mot-clé de recherche eBay
    # category_id : ID interne eBay de la catégorie
    #
    # POURQUOI DES IDs DE CATÉGORIE ?
    # Sans category_id, chercher "laptop" retourne aussi des housses
    # de laptop, des autocollants, des chargeurs... L'ID restreint
    # les résultats aux vrais laptops uniquement.
    #
    # COMMENT TROUVER UN category_id ?
    # Va sur eBay → filtre par catégorie → regarde l'URL
    # Tu verras : ?_sacat=177  → 177 est le category_id des laptops

    CATEGORIES = {
        "smartphones": {
            "keyword":     "smartphone",
            "category_id": "9355",    # Cell Phones & Smartphones
            "label":       "Smartphones",
        },
        "laptops": {
            "keyword":     "laptop",
            "category_id": "177",     # Laptops & Netbooks
            "label":       "Laptops",
        },
        "tablets": {
            "keyword":     "tablet",
            "category_id": "171485",  # Tablets & eBook Readers
            "label":       "Tablets",
        },
    }

    # URL de base pour toutes les recherches eBay
    BASE_URL = "https://www.ebay.com/sch/i.html"

    # ------------------------------------------------------------------
    # CONFIGURATION DU COMPORTEMENT
    # ------------------------------------------------------------------
    # custom_settings : paramètres qui s'appliquent UNIQUEMENT à ce spider
    # (ils remplacent settings.py pour ce spider seulement)

    custom_settings = {

        # ---- POLITESSE -----------------------------------------------
        # Attendre 2 secondes entre chaque requête HTTP.
        # Sans ça, eBay détecte le robot et bloque l'IP.
        # Règle d'or : traite le serveur avec respect.
        "DOWNLOAD_DELAY": 2,

        # Varier le délai entre 1s et 3s de façon aléatoire.
        # Un délai EXACTEMENT fixe (2.000s, 2.000s, 2.000s...)
        # est un signal clair de robot. Un humain est irrégulier.
        "RANDOMIZE_DOWNLOAD_DELAY": True,

        # Jamais plus d'une requête en parallèle.
        # On ne veut pas surcharger les serveurs d'eBay.
        "CONCURRENT_REQUESTS": 1,

        # Toujours respecter le fichier robots.txt d'eBay.
        # robots.txt = la "charte" du site pour les robots.
        "ROBOTSTXT_OBEY": True,

        # Désactiver les cookies pour ne pas créer de session traçable.
        # Chaque requête est indépendante → moins détectable.
        "COOKIES_ENABLED": False,

        # Si une page échoue (timeout, erreur réseau), réessayer 3 fois.
        # Évite de perdre des données à cause d'un problème temporaire.
        "RETRY_TIMES": 3,

        # ---- USER AGENT ----------------------------------------------
        # Le User-Agent est la "signature" envoyée au serveur.
        # Scrapy envoie par défaut "Scrapy/2.x" → bloqué immédiatement.
        # On se fait passer pour un vrai navigateur Chrome.
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),

        # ---- AUTOTHROTTLE -------------------------------------------
        # Fonctionnalité intelligente : Scrapy mesure le temps de réponse
        # d'eBay et ralentit automatiquement si le serveur est lent.
        # C'est le comportement d'un ingénieur responsable.
        "AUTOTHROTTLE_ENABLED":             True,
        "AUTOTHROTTLE_START_DELAY":         1,
        "AUTOTHROTTLE_MAX_DELAY":           10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY":  1.0,

        # ---- EXPORT AUTOMATIQUE -------------------------------------
        # Scrapy sauvegarde automatiquement les données dans ces fichiers.
        # %(time)s = timestamp du run → chaque run crée un nouveau fichier
        # On génère JSON (pour NiFi) ET CSV (pour pandas/Bigtable loader)
        "FEEDS": {
            "data/ebay_%(time)s.json": {
                "format":   "json",
                "encoding": "utf8",
                "indent":   2,
            },
            "data/ebay_%(time)s.csv": {
                "format": "csv",
            },
        },

        # ---- PIPELINES ----------------------------------------------
        # Ordre d'exécution : 300 (nettoyage) avant 400 (déduplication)
        "ITEM_PIPELINES": {
            "ebay_scraper.pipelines.PriceCleaningPipeline":    300,
            "ebay_scraper.pipelines.DuplicateFilterPipeline":  400,
        },

        # ---- LOGS ---------------------------------------------------
        "LOG_LEVEL": "INFO",
        "LOG_FILE":  "logs/ebay_spider.log",
    }

    # ------------------------------------------------------------------
    # CONSTRUCTEUR
    # ------------------------------------------------------------------

    def __init__(self, category=None, max_pages=5, *args, **kwargs):
        """
        Initialise le spider avec les paramètres passés en ligne de commande.

        PARAMÈTRES :
            category  : scraper une seule catégorie (ex: -a category=laptops)
                        si None → scrape toutes les catégories
            max_pages : nombre de pages de résultats par catégorie (défaut: 5)
                        5 pages × 48 produits = ~240 produits par catégorie

        EXEMPLE D'UTILISATION :
            scrapy crawl ebay_electronics -a category=laptops -a max_pages=10
        """
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)

        if category and category in self.CATEGORIES:
            self.targets = {category: self.CATEGORIES[category]}
            self.logger.info(f"Catégorie unique : {category}")
        else:
            self.targets = self.CATEGORIES
            self.logger.info(f"Toutes catégories : {list(self.CATEGORIES.keys())}")

        # Compteurs pour le rapport final
        self.stats_scraped   = 0
        self.stats_dropped   = 0
        self.run_started_at  = datetime.utcnow().isoformat()

    # ------------------------------------------------------------------
    # ÉTAPE 1A : Générer les URLs de départ
    # ------------------------------------------------------------------

    def start_requests(self):
        """
        Point d'entrée du spider — génère les premières requêtes.

        EXPLICATION :
        Cette méthode est appelée automatiquement par Scrapy au démarrage.
        Elle génère une requête HTTP pour la page de recherche eBay
        de chaque catégorie cible.

        C'est l'équivalent de taper "laptop" dans la barre de recherche
        eBay et d'appuyer sur Entrée — mais pour chaque catégorie.

        PARAMÈTRES URL EBAY :
            _nkw    = mot-clé de recherche (ex: "laptop")
            _sacat  = ID de la catégorie (restreint aux vrais produits)
            _sop=12 = trier par "Best Match" (les plus pertinents en premier)
            LH_BIN=1= "Buy It Now" uniquement — on exclut les enchères car
                      les enchères n'ont pas de prix final avant la fin.
                      Pour l'analyse de prix, on veut des prix fixes.
            LH_ItemCondition=3 = New only pour avoir des prix comparables
                                 (on peut enlever pour avoir New + Used)
            _pgn    = numéro de page (commence à 1)
        """
        for cat_name, cat_info in self.targets.items():
            params = {
                "_nkw":    cat_info["keyword"],
                "_sacat":  cat_info["category_id"],
                "_sop":    "12",
                "LH_BIN":  "1",
                "_pgn":    "1",
            }

            url = f"{self.BASE_URL}?{urlencode(params)}"
            self.logger.info(f"Démarrage scraping [{cat_info['label']}] → {url}")

            # scrapy.Request = une requête HTTP.
            # callback = fonction appelée quand la réponse arrive.
            # meta     = données transportées vers la prochaine fonction.
            #            Scrapy ne retient rien entre les requêtes —
            #            meta est notre "mémoire" d'une page à l'autre.
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results,
                meta={
                    "category":     cat_name,
                    "category_label": cat_info["label"],
                    "page":         1,
                    "base_params":  params,
                },
                headers=self._get_headers(),
                errback=self._handle_error,
            )

    # ------------------------------------------------------------------
    # ÉTAPE 1B : Parser la page de résultats de recherche
    # ------------------------------------------------------------------

    def parse_search_results(self, response):
        """
        Traite une page de résultats eBay (liste de ~48 produits).

        EXPLICATION :
        Quand tu cherches "laptop" sur eBay, tu vois une grille de produits.
        Chaque produit est une carte HTML avec un lien vers la page détaillée.
        Cette méthode :
        1. Extrait le lien vers chaque produit de la page
        2. Génère une requête vers la page de chaque produit
        3. Si on n'a pas atteint max_pages, génère la requête page suivante

        SÉLECTEURS CSS UTILISÉS :
        "li.s-item"           = chaque carte produit dans la liste
        "a.s-item__link"      = le lien principal de chaque carte
        "::attr(href)"        = extrait l'attribut href (l'URL) du lien
        """
        category       = response.meta["category"]
        category_label = response.meta["category_label"]
        page           = response.meta["page"]

        self.logger.info(
            f"[{category_label}] Page {page}/{self.max_pages} "
            f"— {response.url[:80]}"
        )

        # Sélectionner toutes les cartes produit de la page
        listings = response.css("li.s-item")

        if not listings:
            self.logger.warning(
                f"Aucun produit trouvé — [{category_label}] page {page}. "
                f"eBay a peut-être changé son HTML."
            )
            return

        queued = 0
        for listing in listings:

            # eBay insère toujours une fausse première carte "Shop on eBay"
            # Elle a la classe LIGHT_HIGHLIGHT — on la détecte et on la saute
            if listing.css("span.LIGHT_HIGHLIGHT"):
                continue

            # Extraire l'URL de la page produit
            item_url = listing.css("a.s-item__link::attr(href)").get()
            if not item_url:
                continue

            # Optionnel : extraire le prix depuis la liste pour un fallback
            # Si la page produit individuelle ne donne pas le prix, on a ça
            list_price_raw = listing.css(
                "span.s-item__price span.POSITIVE::text,"
                "span.s-item__price::text"
            ).get(default="")

            # Générer la requête vers la page produit individuelle
            yield scrapy.Request(
                url=item_url,
                callback=self.parse_product,
                meta={
                    "category":       category,
                    "category_label": category_label,
                    "list_price_raw": list_price_raw,
                },
                headers=self._get_headers(),
                errback=self._handle_error,
            )
            queued += 1

        self.logger.info(
            f"  → [{category_label}] page {page} : {queued} produits en file"
        )

        # ------ PAGINATION -------------------------------------------
        # Si on n'a pas atteint la limite de pages, aller à la page suivante.
        # On incrémente _pgn dans les paramètres URL et on génère la requête.
        if page < self.max_pages:
            next_params         = dict(response.meta["base_params"])
            next_params["_pgn"] = page + 1
            next_url            = f"{self.BASE_URL}?{urlencode(next_params)}"

            yield scrapy.Request(
                url=next_url,
                callback=self.parse_search_results,
                meta={
                    "category":       category,
                    "category_label": category_label,
                    "page":           page + 1,
                    "base_params":    next_params,
                },
                headers=self._get_headers(),
                errback=self._handle_error,
            )

    # ------------------------------------------------------------------
    # ÉTAPE 1C : Parser la page d'un produit individuel
    # ------------------------------------------------------------------

    def parse_product(self, response):
        """
        Extrait toutes les données d'une page produit eBay individuelle.

        EXPLICATION :
        Cette méthode est le cœur du spider. Elle est appelée pour chaque
        produit trouvé à l'étape précédente.

        On utilise PLUSIEURS sélecteurs CSS pour chaque champ (avec 'or').
        Pourquoi ? eBay a plusieurs layouts selon le type de produit
        (listing standard, listing pro, listing avec variantes...).
        Si le premier sélecteur ne trouve rien → on essaie le suivant.
        C'est ce qui rend le spider robuste face aux changements d'eBay.

        DONNÉES EXTRAITES :
            product_id    : ID unique eBay (depuis l'URL)
            title         : titre complet du produit
            price         : prix numérique (float)
            currency      : devise (USD, EUR, GBP...)
            condition     : état (New, Used, Refurbished...)
            seller_name   : nom du vendeur eBay
            seller_score  : score de feedback vendeur
            rating        : note produit (float, ex: 4.7)
            reviews_count : nombre d'avis (int)
            free_shipping : livraison gratuite (bool)
            item_location : pays/ville d'expédition
            scraped_at    : timestamp UTC (pour time-series Bigtable)
        """
        category       = response.meta["category"]
        category_label = response.meta["category_label"]
        list_price_raw = response.meta.get("list_price_raw", "")

        # ---- TITRE ---------------------------------------------------
        # On cherche dans 3 endroits possibles du HTML eBay.
        # ::text extrait le texte de l'élément (sans les balises HTML).
        title = (
            response.css("h1.x-item-title__mainTitle span::text").get()
            or response.css("h1.it-ttl span[itemprop='name']::text").get()
            or response.css("h1[itemprop='name']::text").get()
            or ""
        ).strip()

        # ---- PRIX ----------------------------------------------------
        # Le prix peut être à plusieurs endroits selon le type de listing.
        # En dernier recours, on utilise le prix extrait de la liste.
        price_raw = (
            response.css("div.x-price-primary span.ux-textspans::text").get()
            or response.css("span.x-price-approx__price::text").get()
            or response.css("[itemprop='price']::attr(content)").get()
            or list_price_raw
            or ""
        ).strip()

        currency, price = self._parse_price(price_raw)

        # ---- CONDITION -----------------------------------------------
        # "New", "Used", "Seller Refurbished", "For parts or not working"...
        # Variable cruciale pour l'analyse : un iPhone neuf vs occasion
        # n'a pas du tout le même prix.
        condition = (
            response.css("div.x-item-condition-text span.ux-textspans::text").get()
            or response.css("[itemprop='itemCondition']::text").get()
            or response.css("span.condText::text").get()
            or ""
        ).strip()

        # ---- VENDEUR -------------------------------------------------
        # Nom du vendeur + son score de feedback.
        # Utile pour la régression : est-ce que les Top Sellers
        # vendent plus cher que les vendeurs ordinaires ?
        seller_name = (
            response.css("span.mbg-nw::text").get()
            or response.css(
                "span[data-testid='ux-seller-section__item--seller-name']::text"
            ).get()
            or ""
        ).strip()

        seller_score = (
            response.css(
                "span[data-testid='ux-seller-section__item--seller-score']::text"
            ).get()
            or response.css("span.mbg-l a::text").get()
            or ""
        ).strip()

        # ---- RATING & REVIEWS ----------------------------------------
        # Note moyenne du produit et nombre total d'avis.
        # Variables importantes pour la régression :
        # prix ~ rating + reviews_count
        rating_raw = (
            response.css("span.reviews-star-rating::attr(title)").get()
            or ""
        )
        rating = self._parse_float(rating_raw)

        reviews_raw = (
            response.css(
                "span[data-testid='reviews-accordion-header-count']::text"
            ).get()
            or response.css("a.reviews-link span::text").get()
            or "0"
        )
        reviews_count = self._parse_int(reviews_raw)

        # ---- LIVRAISON -----------------------------------------------
        # "Free shipping" influence le prix perçu.
        # Un produit à $300 avec livraison gratuite vs $280 + $20 livraison
        # → même prix final mais impact sur le comportement d'achat.
        shipping_raw = (
            response.css(
                "div.ux-labels-values--shipping span.ux-textspans::text"
            ).get()
            or ""
        ).strip()
        free_shipping = "free" in shipping_raw.lower()

        # ---- LOCALISATION --------------------------------------------
        # Pays/ville d'expédition. Les produits venant de Chine sont
        # souvent moins chers → variable utile dans l'analyse géographique.
        item_location = (
            response.css("span.ux-textspans--SECONDARY::text").get()
            or ""
        ).strip()

        # ---- BEAUTIFULSOUP : données complexes -----------------------
        # On passe le HTML complet au parser BeautifulSoup pour extraire
        # les specs techniques, variantes de prix, détails vendeur.
        # Pas de requête supplémentaire — on réutilise le HTML déjà téléchargé.
        bs_parser = EbayBeautifulSoupParser(response.text)
        bs_data   = bs_parser.extract_all()

        # ---- CONSTRUCTION DE L'ITEM FINAL ----------------------------
        # On fusionne les données Scrapy (basiques) + BeautifulSoup (complexes).
        # {**dict1, **dict2} = merge de deux dictionnaires Python.
        #
        # LES 3 CHAMPS TEMPORELS SONT CRITIQUES :
        # scraped_at   : timestamp précis → row key dans Bigtable
        # scraped_date : date seule → partitionnement et filtres dbt
        # scraped_hour : heure seule → analyse intra-journalière
        #                (les prix sont-ils différents le matin vs le soir ?)
        self.stats_scraped += 1

        yield {
            # Identité du produit
            "product_id":    self._extract_item_id(response.url),
            "title":         title,
            "category":      category,
            "category_label": category_label,
            "url":           response.url,
            "source":        "ebay",        # important pour la comparaison eBay vs Amazon

            # Prix
            "price":         price,         # float  : 299.99
            "currency":      currency,       # str    : "USD"
            "price_raw":     price_raw,      # str    : "$299.99" (gardé pour debug)

            # Caractéristiques produit
            "condition":     condition,
            "free_shipping": free_shipping,  # bool
            "shipping_info": shipping_raw,
            "item_location": item_location,

            # Vendeur
            "seller_name":   seller_name,
            "seller_score":  seller_score,

            # Popularité
            "rating":        rating,         # float ou None
            "reviews_count": reviews_count,  # int

            # Horodatage — INDISPENSABLE pour Bigtable (time-series)
            # Row key Bigtable = product_id#scraped_at
            "scraped_at":    datetime.utcnow().isoformat(),          # "2026-03-14T10:32:11"
            "scraped_date":  datetime.utcnow().strftime("%Y-%m-%d"), # "2026-03-14"
            "scraped_hour":  datetime.utcnow().hour,                  # 10

            # Données enrichies par BeautifulSoup (specs, variantes...)
            **bs_data,
        }

    # ------------------------------------------------------------------
    # RAPPORT DE FIN DE RUN
    # ------------------------------------------------------------------

    def closed(self, reason):
        """
        Appelée automatiquement quand le spider se termine.
        Affiche un résumé propre dans les logs.

        POURQUOI C'EST IMPORTANT :
        En production, on veut savoir combien de produits ont été
        collectés, combien ont été rejetés, et pourquoi le spider
        s'est arrêté (fin normale ou erreur).
        """
        self.logger.info(
            f"\n{'='*50}\n"
            f"SPIDER TERMINÉ — raison : {reason}\n"
            f"Démarré à   : {self.run_started_at}\n"
            f"Terminé à   : {datetime.utcnow().isoformat()}\n"
            f"Produits collectés : {self.stats_scraped}\n"
            f"Catégories  : {list(self.targets.keys())}\n"
            f"{'='*50}"
        )

    # ------------------------------------------------------------------
    # GESTION DES ERREURS
    # ------------------------------------------------------------------

    def _handle_error(self, failure):
        """
        Appelée si une requête échoue définitivement (après les retries).

        En production on loggue l'erreur et on continue.
        On ne veut pas qu'une page cassée arrête tout le spider.
        """
        self.logger.error(
            f"Requête échouée : {failure.request.url}\n"
            f"Erreur : {failure.value}"
        )
        self.stats_dropped += 1

    # ------------------------------------------------------------------
    # FONCTIONS UTILITAIRES (helpers)
    # ------------------------------------------------------------------

    def _parse_price(self, raw: str) -> tuple:
        """
        Convertit un prix brut en tuple (devise, float).

        EXEMPLES :
            "$1,299.99" → ("USD", 1299.99)
            "€899.00"   → ("EUR", 899.00)
            "£450.00"   → ("GBP", 450.00)
            ""          → ("USD", None)

        POURQUOI SÉPARER DEVISE ET MONTANT ?
        Pour filtrer par devise dans dbt et convertir en USD
        dans l'analyse si on compare plusieurs marchés.
        """
        if not raw:
            return "USD", None

        # Détection de la devise par le symbole monétaire
        currency = "USD"
        if "€"    in raw: currency = "EUR"
        elif "£"  in raw: currency = "GBP"
        elif "CA $" in raw or "C $" in raw: currency = "CAD"

        # Supprimer tout sauf les chiffres et le point décimal
        # re.sub(pattern, remplacement, texte)
        # [^\d.] = tout ce qui n'est pas un chiffre ou un point
        cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
        try:
            return currency, round(float(cleaned), 2)
        except ValueError:
            return currency, None

    def _parse_float(self, s: str) -> float | None:
        """
        Extrait le premier float d'une chaîne de texte.
        Exemple : "4.7 out of 5 stars" → 4.7
        """
        match = re.search(r"[\d.]+", str(s))
        return round(float(match.group()), 2) if match else None

    def _parse_int(self, s: str) -> int:
        """
        Extrait un entier en ignorant les virgules de milliers.
        Exemple : "1,234 product ratings" → 1234
        """
        match = re.search(r"[\d,]+", str(s))
        return int(match.group().replace(",", "")) if match else 0

    def _extract_item_id(self, url: str) -> str:
        """
        Extrait l'ID unique du produit depuis l'URL eBay.
        Exemple : "https://ebay.com/itm/234567890123?..." → "234567890123"

        Cet ID est utilisé comme partie de la row key Bigtable :
        "234567890123#2026-03-14T10:32:11"
        """
        match = re.search(r"/itm/(\d+)", url)
        return match.group(1) if match else ""

    def _get_headers(self) -> dict:
        """
        Headers HTTP additionnels pour simuler un vrai navigateur.

        Un navigateur envoie ces headers automatiquement.
        Scrapy ne les envoie pas par défaut.
        Sans eux, eBay peut détecter facilement qu'on n'est pas
        un vrai navigateur et bloquer les requêtes.

        Accept          : types de contenu qu'on accepte
        Accept-Language : langue préférée (en-US pour le marché US)
        Accept-Encoding : compression acceptée (gzip = réponses plus rapides)
        """
        return {
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language":           "en-US,en;q=0.5",
            "Accept-Encoding":           "gzip, deflate, br",
            "Connection":                "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control":             "max-age=0",
        }