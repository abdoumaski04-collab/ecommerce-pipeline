"""
pipelines.py — Pipelines de nettoyage et validation
=====================================================

ÉTAPE 3 DU PIPELINE : Contrôle qualité des données
---------------------------------------------------

RÔLE DE CE FICHIER :
    Chaque item collecté par le spider passe obligatoirement
    par ces pipelines AVANT d'être sauvegardé en CSV/JSON.
    C'est le principe "garbage in, garbage out" à l'envers :
    on garantit des données propres dès la source.

ORDRE D'EXÉCUTION (défini dans custom_settings du spider) :
    300 → PriceCleaningPipeline   : nettoie et valide chaque item
    400 → DuplicateFilterPipeline : rejette les doublons

POURQUOI CET ORDRE ?
    On nettoie D'ABORD (300) pour avoir des données normalisées,
    puis on dédoublonne (400) en se basant sur le product_id propre.
    Inverser l'ordre serait une erreur : si on dédoublonne des données
    sales, on pourrait garder le mauvais doublon.

INTÉGRATION DANS LE PIPELINE GLOBAL :
    Spider → Pipeline 300 → Pipeline 400 → CSV/JSON → Bigtable
"""

import re
import hashlib
from datetime import datetime
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


# ======================================================================
# PIPELINE 1 : Nettoyage et validation des prix
# ======================================================================

class PriceCleaningPipeline:
    """
    Nettoie, valide et enrichit chaque item avant sauvegarde.

    RÈGLES APPLIQUÉES DANS L'ORDRE :
        1. Rejeter si prix manquant
        2. Rejeter si titre manquant
        3. Normaliser la condition (New, Used, Refurbished...)
        4. Nettoyer le titre (espaces multiples, caractères spéciaux)
        5. Forcer le prix en float arrondi à 2 décimales
        6. Forcer reviews_count en int
        7. Forcer rating en float ou None
        8. Valider la devise (USD par défaut si manquante)
        9. Ajouter price_bucket (tranche de prix)
       10. Ajouter is_new_condition (bool simple pour analyse)

    POURQUOI DES RÈGLES STRICTES ?
        Si on laisse passer un item avec price=None dans Bigtable,
        la moyenne des prix sera faussée.
        Si on laisse passer "Brand New" et "New" comme deux
        conditions différentes, le GROUP BY condition dans dbt
        donnera des résultats incohérents.
        La rigueur ici = analyses statistiques fiables plus tard.
    """

    # ------------------------------------------------------------------
    # Dictionnaire de normalisation des conditions
    # ------------------------------------------------------------------
    # eBay laisse les vendeurs écrire la condition librement.
    # Résultat : des dizaines de façons d'écrire "New" ou "Used".
    # On ramène tout à 6 valeurs standardisées.
    #
    # FORMAT : "texte_à_chercher" → "Valeur standardisée"
    # On cherche si la clé est CONTENUE dans la condition brute (lower).
    # Ordre important : du plus spécifique au plus général.
    CONDITION_NORMALIZATION = {
        "certified refurb":  "Certified Refurbished",
        "manufacturer refurb": "Certified Refurbished",
        "seller refurb":     "Seller Refurbished",
        "open box":          "Open Box",
        "for parts":         "For Parts",
        "not working":       "For Parts",
        "new other":         "New Other",
        "new with":          "New",          # "New with tags", "New with box"
        "brand new":         "New",
        "new":               "New",
        "used":              "Used",
        "pre-owned":         "Used",
        "pre owned":         "Used",
        "good":              "Used",          # "Good - Refurbished" eBay grades
        "very good":         "Used",
        "acceptable":        "Used",
        "excellent":         "Used",
    }

    # ------------------------------------------------------------------
    # Méthode principale
    # ------------------------------------------------------------------

    def process_item(self, item, spider):
        """
        Appelée automatiquement par Scrapy pour chaque item collecté.

        PARAMÈTRES :
            item   : le dictionnaire de données du produit
            spider : référence au spider (pour logger)

        RETOURNE :
            L'item nettoyé si tout est valide
            Lève DropItem si l'item doit être rejeté

        ITEMADAPTER :
            ItemAdapter est un wrapper Scrapy qui permet d'accéder
            à l'item comme un dictionnaire standard, peu importe
            son type réel (dict, dataclass, Item Scrapy...).
            C'est la façon recommandée d'accéder aux items dans les pipelines.
        """
        adapter = ItemAdapter(item)

        # ---- RÈGLE 1 : Prix obligatoire ----------------------------
        # Un produit sans prix est inutile pour l'analyse de prix.
        # DropItem = Scrapy rejette cet item définitivement.
        # Il ne sera PAS sauvegardé dans le CSV/JSON.
        price = adapter.get("price")
        if price is None or price == "":
            spider.logger.debug(
                f"DROP (prix manquant) : {adapter.get('title', '')[:50]}"
            )
            raise DropItem(f"Prix manquant : {adapter.get('url', '')}")

        # ---- RÈGLE 2 : Titre obligatoire ---------------------------
        # Sans titre, on ne sait pas ce qu'on a scrappé.
        title = adapter.get("title", "").strip()
        if not title:
            raise DropItem(f"Titre manquant : {adapter.get('url', '')}")

        # ---- RÈGLE 3 : Rejeter les prix aberrants ------------------
        # Un prix de 0 ou négatif = erreur d'extraction.
        # Un prix > 50 000 = probablement une erreur de parsing.
        # On garde cette plage large pour ne pas perdre de vrais produits.
        try:
            price_float = float(price)
            if price_float <= 0 or price_float > 50_000:
                raise DropItem(
                    f"Prix aberrant ({price_float}) : {adapter.get('title', '')[:40]}"
                )
        except (TypeError, ValueError):
            raise DropItem(f"Prix non-numérique : {price}")

        # ---- RÈGLE 4 : Normaliser la condition ---------------------
        # "Brand New with tags" → "New"
        # "Pre-Owned - Good"    → "Used"
        # On cherche les clés du dictionnaire dans la condition brute.
        cond_raw  = (adapter.get("condition") or "").lower().strip()
        condition = "Unknown"   # valeur par défaut si aucune correspondance

        for keyword, normalized_value in self.CONDITION_NORMALIZATION.items():
            if keyword in cond_raw:
                condition = normalized_value
                break   # on prend la première correspondance (ordre important)

        adapter["condition"] = condition

        # ---- RÈGLE 5 : Nettoyer le titre ---------------------------
        # re.sub(r"\s+", " ", title) remplace toute séquence d'espaces
        # (espaces, tabs, sauts de ligne) par un seul espace.
        adapter["title"] = re.sub(r"\s+", " ", title).strip()

        # ---- RÈGLE 6 : Forcer le prix en float ---------------------
        # On arrondit à 2 décimales pour éviter 299.990000001 (erreurs float).
        adapter["price"] = round(float(price_float), 2)

        # ---- RÈGLE 7 : Forcer reviews_count en int -----------------
        # Peut arriver comme "1,234" (string) ou 1234 (int) selon l'extraction.
        # None → 0 car un produit sans avis = 0 avis, pas "inconnu".
        try:
            raw_reviews = adapter.get("reviews_count") or 0
            if isinstance(raw_reviews, str):
                raw_reviews = raw_reviews.replace(",", "")
            adapter["reviews_count"] = int(float(raw_reviews))
        except (TypeError, ValueError):
            adapter["reviews_count"] = 0

        # ---- RÈGLE 8 : Forcer rating en float ou None --------------
        # None est correct quand il n'y a pas de note.
        # 0.0 voudrait dire "noté 0 étoiles" ce qui est différent.
        try:
            r = adapter.get("rating")
            adapter["rating"] = round(float(r), 2) if r is not None else None
        except (TypeError, ValueError):
            adapter["rating"] = None

        # ---- RÈGLE 9 : Valider la devise ---------------------------
        # Si la devise n'a pas été détectée, USD par défaut (marché eBay US).
        valid_currencies = {"USD", "EUR", "GBP", "CAD", "AUD"}
        if adapter.get("currency") not in valid_currencies:
            adapter["currency"] = "USD"

        # ---- RÈGLE 10 : Nettoyer seller_score ---------------------
        # Peut contenir des caractères non-numériques parasites.
        score_raw = str(adapter.get("seller_score") or "").strip()
        score_match = re.search(r"[\d,]+", score_raw)
        adapter["seller_score_clean"] = (
            int(score_match.group().replace(",", "")) if score_match else None
        )

        # ---- ENRICHISSEMENT 1 : price_bucket ----------------------
        # Tranche de prix calculée ici une fois pour toutes.
        # Utilisée directement dans dbt GROUP BY et dans les graphes.
        #
        # Ces tranches sont adaptées à l'électronique eBay :
        # < $100    : accessoires (câbles, coques...)
        # $100-300  : mid-range phones, vieilles tablettes
        # $300-600  : flagship phones, laptops budget
        # $600-1000 : premium phones, laptops mid-range
        # $1000+    : MacBook, laptops haut de gamme
        adapter["price_bucket"] = self._get_price_bucket(price_float)

        # ---- ENRICHISSEMENT 2 : is_new_condition ------------------
        # Booléen simple pour faciliter les filtres dans l'analyse.
        # t-test : "prix des produits neufs vs occasions"
        adapter["is_new_condition"] = (condition == "New")

        # ---- ENRICHISSEMENT 3 : has_free_shipping -----------------
        # Normaliser en booléen strict (True/False, jamais None)
        adapter["free_shipping"] = bool(adapter.get("free_shipping", False))

        # ---- ENRICHISSEMENT 4 : scraped_week ----------------------
        # Numéro de semaine — utile pour agréger par semaine dans dbt
        # Exemple : "2026-W11" pour la semaine 11 de 2026
        try:
            date_str = adapter.get("scraped_date", "")
            if date_str:
                d = datetime.strptime(date_str, "%Y-%m-%d")
                adapter["scraped_week"] = d.strftime("%Y-W%W")
            else:
                adapter["scraped_week"] = None
        except ValueError:
            adapter["scraped_week"] = None

        return item

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_price_bucket(self, price: float) -> str:
        """
        Classe un prix dans une tranche standardisée.

        POURQUOI DES TRANCHES FIXES ?
            Pour que les histogrammes et barplots soient cohérents
            entre les runs. Sans ça, chaque run pourrait avoir des
            tranches différentes et les graphes ne seraient pas comparables.

        POURQUOI CES TRANCHES PRÉCISES ?
            Elles sont calibrées pour l'électronique eBay :
            la distribution réelle des prix se concentre entre $50 et $1500.
            Des tranches trop larges (< $500, > $500) écraseraient la variance.
            Des tranches trop fines (< $10, $10-20...) seraient trop dispersées.
        """
        if price < 100:    return "< $100"
        if price < 300:    return "$100–300"
        if price < 600:    return "$300–600"
        if price < 1000:   return "$600–1000"
        return "> $1000"


# ======================================================================
# PIPELINE 2 : Filtre de doublons
# ======================================================================

class DuplicateFilterPipeline:
    """
    Supprime les produits en double dans le même run du spider.

    POURQUOI DES DOUBLONS APPARAISSENT ?
        eBay peut afficher le même produit dans plusieurs catégories,
        sur plusieurs pages de résultats, ou dans des sections "sponsorisées".
        Sans ce filtre, un même iPhone pourrait apparaître 5 fois
        dans nos données → la moyenne des prix serait faussée.

    COMMENT ÇA MARCHE :
        On maintient un set() Python des product_id déjà vus.
        Un set est une structure de données qui ne contient pas de doublons
        et vérifie l'appartenance en O(1) (ultra-rapide).

        Pour chaque item :
            Si product_id dans self.seen_ids → DropItem (doublon)
            Sinon → ajouter au set, laisser passer l'item

    LIMITE IMPORTANTE :
        Ce filtre est EN MÉMOIRE — il se réinitialise à chaque run.
        Il ne détecte PAS les doublons entre deux runs différents.

        Exemple : si tu lances le spider lundi et mardi,
        un produit scrapé les deux jours ne sera pas filtré ici.
        C'est VOULU : on veut garder le prix du lundi ET du mardi
        pour l'analyse temporelle (time-series).

        La déduplication inter-run se fait dans dbt (staging model)
        en comparant les product_id + scraped_date dans Bigtable.
    """

    # ------------------------------------------------------------------
    # Cycle de vie du pipeline
    # ------------------------------------------------------------------

    def open_spider(self, spider):
        """
        Appelée UNE FOIS quand le spider démarre.
        Initialise le set vide qui va stocker les IDs vus.
        """
        self.seen_ids     = set()
        self.total_passed = 0
        self.total_dropped = 0
        spider.logger.info("DuplicateFilterPipeline : initialisé")

    def process_item(self, item, spider):
        """
        Appelée pour chaque item qui passe le pipeline précédent.

        LOGIQUE :
            1. Extraire le product_id
            2. Si déjà vu → DropItem
            3. Sinon → mémoriser + laisser passer
        """
        adapter    = ItemAdapter(item)
        product_id = adapter.get("product_id", "")

        # Si product_id est vide (extraction échouée), on génère un ID
        # basé sur le titre + prix pour quand même détecter les doublons évidents
        if not product_id:
            title    = adapter.get("title", "")
            price    = str(adapter.get("price", ""))
            product_id = hashlib.md5(f"{title}{price}".encode()).hexdigest()[:12]
            adapter["product_id"] = product_id

        # Vérifier si on a déjà vu cet ID dans ce run
        if product_id in self.seen_ids:
            self.total_dropped += 1
            spider.logger.debug(f"DOUBLON détecté : {product_id}")
            raise DropItem(f"Doublon product_id : {product_id}")

        # Nouveau produit → mémoriser et laisser passer
        self.seen_ids.add(product_id)
        self.total_passed += 1
        return item

    def close_spider(self, spider):
        """
        Appelée UNE FOIS quand le spider se termine.
        Affiche le bilan de déduplication dans les logs.

        Ce bilan est important pour monitorer la qualité du scraping :
        si 30% des items sont des doublons → quelque chose ne va pas
        dans la logique de pagination du spider.
        """
        spider.logger.info(
            f"\n{'─'*40}\n"
            f"DuplicateFilterPipeline — Bilan\n"
            f"  Produits uniques conservés : {self.total_passed}\n"
            f"  Doublons rejetés           : {self.total_dropped}\n"
            f"  Taux de doublons           : "
            f"{self.total_dropped / max(self.total_passed + self.total_dropped, 1) * 100:.1f}%\n"
            f"{'─'*40}"
        )