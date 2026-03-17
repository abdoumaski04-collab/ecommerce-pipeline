"""
settings.py — Configuration globale Scrapy
==========================================

RÔLE DE CE FICHIER :
    Paramètres partagés par TOUS les spiders du projet.
    Les custom_settings dans ebay_spider.py REMPLACENT ces valeurs
    pour ce spider spécifique.

    Règle simple :
    - settings.py = valeurs par défaut pour tout le projet
    - custom_settings = exceptions pour un spider particulier
"""

BOT_NAME        = "ebay_scraper"
SPIDER_MODULES  = ["ebay_scraper.spiders"]
NEWSPIDER_MODULE = "ebay_scraper.spiders"

# Toujours respecter robots.txt (valeur par défaut globale)
ROBOTSTXT_OBEY           = True
DOWNLOAD_DELAY           = 2
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS      = 1

# AutoThrottle global
AUTOTHROTTLE_ENABLED            = True
AUTOTHROTTLE_START_DELAY        = 1
AUTOTHROTTLE_MAX_DELAY          = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# Headers par défaut
DEFAULT_REQUEST_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Logs
LOG_LEVEL = "INFO"
LOG_FILE  = "logs/scrapy.log"

# Encodage des fichiers exportés
FEED_EXPORT_ENCODING = "utf-8"

# Réacteur asyncio recommandé pour Scrapy 2.x+
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"