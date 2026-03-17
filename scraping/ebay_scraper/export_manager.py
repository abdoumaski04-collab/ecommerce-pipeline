"""
export_manager.py — Validation et gestion des exports locaux
=============================================================

ÉTAPE 4 DU PIPELINE : Sauvegarde locale et validation
------------------------------------------------------

RÔLE DE CE FICHIER :
    Après que Scrapy a sauvegardé le CSV/JSON automatiquement,
    ce module fait deux choses :

    1. VALIDATION : vérifie que le fichier exporté est complet
       et cohérent avant de le marquer comme "prêt pour Bigtable"

    2. MANIFESTE : écrit les métadonnées du run dans manifest.json
       Airflow lit ce fichier pour savoir quels fichiers charger
       dans Bigtable et lesquels ont déjà été traités.

POURQUOI CE MODULE EST IMPORTANT EN PRODUCTION :
    Sans validation → un fichier corrompu ou vide peut entrer
    dans Bigtable et fausser toutes les analyses.

    Sans manifeste → Airflow ne sait pas quel fichier est nouveau,
    risque de charger deux fois les mêmes données dans Bigtable
    (doublons inter-run que DuplicateFilterPipeline ne détecte pas).

UTILISATION :
    # Appelé automatiquement par le pipeline ExportValidationPipeline
    # Ou manuellement après un run :
    python export_manager.py --file data/ebay_2026-03-14.csv
"""

import os
import json
import csv
import hashlib
import argparse
from datetime import datetime
from pathlib import Path


# ======================================================================
# CLASSE PRINCIPALE
# ======================================================================

class ExportManager:
    """
    Gère la validation et le suivi des fichiers exportés par Scrapy.

    UTILISATION DANS LE SPIDER :
        # Dans close_spider() d'un pipeline :
        manager = ExportManager(data_dir="data")
        manager.validate_and_register(filepath, spider_stats)
    """

    # Colonnes minimales obligatoires dans chaque fichier exporté.
    # Si une de ces colonnes est absente → le fichier est rejeté.
    REQUIRED_COLUMNS = {
        "product_id",
        "title",
        "price",
        "currency",
        "condition",
        "category",
        "source",
        "scraped_at",
        "scraped_date",
    }

    def __init__(self, data_dir: str = "data"):
        """
        Initialise le gestionnaire avec le dossier de données.

        PARAMÈTRES :
            data_dir : chemin vers le dossier data/ (relatif au projet)
        """
        self.data_dir      = Path(data_dir)
        self.manifest_path = self.data_dir / "manifest.json"

        # Créer le dossier data/ s'il n'existe pas
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # MÉTHODE PRINCIPALE
    # ------------------------------------------------------------------

    def validate_and_register(
        self,
        filepath: str,
        stats: dict = None
    ) -> dict:
        """
        Valide un fichier exporté et l'enregistre dans le manifeste.

        ÉTAPES :
            1. Vérifier que le fichier existe et n'est pas vide
            2. Vérifier que les colonnes obligatoires sont présentes
            3. Compter les lignes et vérifier la cohérence des données
            4. Calculer un checksum MD5 (pour détecter les corruptions)
            5. Écrire les métadonnées dans manifest.json

        PARAMÈTRES :
            filepath : chemin vers le fichier CSV à valider
            stats    : dict optionnel avec les stats du spider
                       (nb produits scrapés, nb rejetés, etc.)

        RETOURNE :
            Un dict avec le statut de validation et les métadonnées
        """
        filepath = Path(filepath)

        # ---- Vérification 1 : fichier existe -----------------------
        if not filepath.exists():
            return self._make_result(
                filepath=str(filepath),
                status="error",
                message=f"Fichier introuvable : {filepath}"
            )

        # ---- Vérification 2 : fichier non vide ---------------------
        file_size = filepath.stat().st_size
        if file_size == 0:
            return self._make_result(
                filepath=str(filepath),
                status="error",
                message="Fichier vide — aucune donnée exportée"
            )

        # ---- Vérification 3 : colonnes et contenu ------------------
        validation = self._validate_csv(filepath)
        if not validation["valid"]:
            return self._make_result(
                filepath=str(filepath),
                status="error",
                message=validation["message"]
            )

        # ---- Calcul du checksum MD5 --------------------------------
        # Le MD5 est une "empreinte" du fichier.
        # Si Airflow recalcule le MD5 et qu'il est identique → fichier déjà traité.
        # Si le MD5 a changé → fichier modifié ou corrompu.
        checksum = self._compute_md5(filepath)

        # ---- Construction des métadonnées du run -------------------
        metadata = {
            "file":           filepath.name,
            "filepath":       str(filepath.absolute()),
            "file_size_bytes": file_size,
            "checksum_md5":   checksum,
            "row_count":      validation["row_count"],
            "categories":     validation["categories"],
            "price_range": {
                "min": validation["price_min"],
                "max": validation["price_max"],
                "avg": validation["price_avg"],
            },
            "scraped_at":     datetime.utcnow().isoformat(),
            "status":         "ready",   # "ready" = prêt pour Bigtable
            "loaded_to_bigtable": False, # Airflow met à True après chargement
            "spider_stats":   stats or {},
        }

        # ---- Enregistrement dans le manifeste ----------------------
        self._write_to_manifest(metadata)

        print(
            f"\n✓ Export validé : {filepath.name}\n"
            f"  Lignes       : {validation['row_count']}\n"
            f"  Catégories   : {validation['categories']}\n"
            f"  Prix (min/max): ${validation['price_min']} / ${validation['price_max']}\n"
            f"  MD5          : {checksum}\n"
            f"  Statut       : ready → en attente Airflow\n"
        )

        return self._make_result(
            filepath=str(filepath),
            status="ready",
            message="Validation réussie",
            metadata=metadata
        )

    # ------------------------------------------------------------------
    # VALIDATION CSV
    # ------------------------------------------------------------------

    def _validate_csv(self, filepath: Path) -> dict:
        """
        Valide le contenu d'un fichier CSV exporté par Scrapy.

        VÉRIFICATIONS :
            - Les colonnes obligatoires sont présentes
            - Au moins 1 ligne de données (pas que l'en-tête)
            - Les prix sont bien numériques
            - Les catégories présentes dans le fichier

        RETOURNE :
            dict avec valid (bool), message, et statistiques du fichier
        """
        result = {
            "valid":      False,
            "message":    "",
            "row_count":  0,
            "categories": [],
            "price_min":  None,
            "price_max":  None,
            "price_avg":  None,
        }

        try:
            with open(filepath, encoding="utf-8") as f:
                reader    = csv.DictReader(f)
                columns   = set(reader.fieldnames or [])

                # Vérifier les colonnes obligatoires
                missing = self.REQUIRED_COLUMNS - columns
                if missing:
                    result["message"] = f"Colonnes manquantes : {missing}"
                    return result

                # Parcourir les lignes pour les stats
                prices     = []
                categories = set()
                row_count  = 0

                for row in reader:
                    row_count += 1
                    categories.add(row.get("category", "unknown"))

                    try:
                        price = float(row.get("price", 0) or 0)
                        if price > 0:
                            prices.append(price)
                    except (ValueError, TypeError):
                        pass

                # Au moins 10 lignes pour être utile
                if row_count < 10:
                    result["message"] = (
                        f"Trop peu de données : {row_count} lignes "
                        f"(minimum 10 requis)"
                    )
                    return result

                # Statistiques prix
                if prices:
                    result["price_min"] = round(min(prices), 2)
                    result["price_max"] = round(max(prices), 2)
                    result["price_avg"] = round(sum(prices) / len(prices), 2)

                result["valid"]      = True
                result["row_count"]  = row_count
                result["categories"] = sorted(categories)
                result["message"]    = "OK"

        except Exception as e:
            result["message"] = f"Erreur lecture CSV : {e}"

        return result

    # ------------------------------------------------------------------
    # MANIFESTE
    # ------------------------------------------------------------------

    def _write_to_manifest(self, metadata: dict) -> None:
        """
        Ajoute les métadonnées du run dans manifest.json.

        STRUCTURE DU MANIFESTE :
        {
          "runs": [
            {
              "file": "ebay_2026-03-14T10-32.csv",
              "row_count": 720,
              "status": "ready",
              "loaded_to_bigtable": false,
              "scraped_at": "2026-03-14T10:32:11",
              ...
            },
            ...
          ],
          "last_updated": "2026-03-14T10:32:11"
        }

        POURQUOI UN SEUL FICHIER MANIFESTE ?
            Airflow lit UN seul fichier pour connaître tous les runs.
            Plus simple que de scanner le dossier data/ à chaque fois.
            Le status "loaded_to_bigtable" permet à Airflow de ne traiter
            que les nouveaux fichiers, jamais les anciens déjà chargés.
        """
        # Charger le manifeste existant ou créer un nouveau
        if self.manifest_path.exists():
            with open(self.manifest_path, encoding="utf-8") as f:
                try:
                    manifest = json.load(f)
                except json.JSONDecodeError:
                    manifest = {"runs": []}
        else:
            manifest = {"runs": []}

        # Ajouter le nouveau run
        manifest["runs"].append(metadata)
        manifest["last_updated"] = datetime.utcnow().isoformat()
        manifest["total_runs"]   = len(manifest["runs"])

        # Sauvegarder
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    def get_pending_files(self) -> list:
        """
        Retourne la liste des fichiers prêts mais pas encore chargés dans Bigtable.

        UTILISÉ PAR AIRFLOW :
            Le DAG Airflow appelle cette méthode pour savoir
            quels fichiers doivent être envoyés vers Bigtable.

        RETOURNE :
            Liste de dicts avec les métadonnées des fichiers en attente
        """
        if not self.manifest_path.exists():
            return []

        with open(self.manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)

        # Filtrer les fichiers "ready" non encore chargés
        return [
            run for run in manifest.get("runs", [])
            if run.get("status") == "ready"
            and not run.get("loaded_to_bigtable", False)
        ]

    def mark_as_loaded(self, filename: str) -> None:
        """
        Marque un fichier comme chargé dans Bigtable dans le manifeste.

        UTILISÉ PAR AIRFLOW après un chargement réussi :
            manager.mark_as_loaded("ebay_2026-03-14T10-32.csv")

        Cela empêche Airflow de recharger le même fichier lors du prochain run.
        """
        if not self.manifest_path.exists():
            return

        with open(self.manifest_path, encoding="utf-8") as f:
            manifest = json.load(f)

        # Mettre à jour le statut du fichier
        for run in manifest.get("runs", []):
            if run.get("file") == filename:
                run["loaded_to_bigtable"] = True
                run["loaded_at"]          = datetime.utcnow().isoformat()
                run["status"]             = "loaded"
                break

        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        print(f"✓ Marqué comme chargé dans Bigtable : {filename}")

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _compute_md5(self, filepath: Path) -> str:
        """
        Calcule le checksum MD5 d'un fichier.

        Le MD5 est une empreinte de 32 caractères hexadécimaux.
        Deux fichiers identiques ont le même MD5.
        Si le MD5 change → le fichier a été modifié (ou corrompu).

        On lit le fichier par blocs de 8192 bytes pour gérer
        les gros fichiers sans saturer la mémoire.
        """
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def _make_result(
        self,
        filepath: str,
        status: str,
        message: str,
        metadata: dict = None
    ) -> dict:
        """Construit un dict de résultat standardisé."""
        return {
            "filepath": filepath,
            "status":   status,
            "message":  message,
            "metadata": metadata or {},
        }


# ======================================================================
# PIPELINE SCRAPY : déclenche la validation après chaque run
# ======================================================================

class ExportValidationPipeline:
    """
    Pipeline Scrapy (ordre 500) qui déclenche la validation
    automatiquement quand le spider se termine.

    ORDRE DANS LA CHAÎNE :
        300 → PriceCleaningPipeline
        400 → DuplicateFilterPipeline
        500 → ExportValidationPipeline  ← ce pipeline

    CE QU'IL FAIT :
        Il ne touche PAS aux items (il retourne chaque item intact).
        Il attend que le spider se termine (close_spider) puis
        valide le fichier CSV que Scrapy vient de générer.
    """

    def open_spider(self, spider):
        """Initialise le gestionnaire au démarrage."""
        self.manager     = ExportManager(data_dir="data")
        self.item_count  = 0

    def process_item(self, item, spider):
        """
        Ne modifie pas l'item — se contente de compter.
        Retourne l'item intact pour le pipeline suivant.
        """
        self.item_count += 1
        return item

    def close_spider(self, spider):
        """
        Appelée quand le spider se termine.
        Cherche les fichiers CSV générés dans data/ et les valide.
        """
        data_dir = spider.settings.get("FEEDS", {})

        # Trouver les fichiers CSV récents dans data/
        data_path = spider.settings.get("DATA_DIR", "data")
        csv_files = sorted(
            Path(data_path).glob("ebay_*.csv"),
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )

        if not csv_files:
            spider.logger.warning("ExportValidationPipeline : aucun fichier CSV trouvé")
            return

        # Valider le fichier le plus récent
        latest_file = csv_files[0]
        spider.logger.info(
            f"ExportValidationPipeline : validation de {latest_file.name}"
        )

        spider_stats = {
            "items_scraped":  self.item_count,
            "categories":     list(spider.targets.keys()),
            "max_pages":      spider.max_pages,
            "run_started_at": spider.run_started_at,
        }

        result = self.manager.validate_and_register(
            filepath=str(latest_file),
            stats=spider_stats,
        )

        if result["status"] == "ready":
            spider.logger.info(
                f"✓ Fichier validé et enregistré dans manifest.json\n"
                f"  → Prêt pour chargement Airflow → Bigtable"
            )
        else:
            spider.logger.error(
                f"✗ Validation échouée : {result['message']}"
            )


# ======================================================================
# SCRIPT EN LIGNE DE COMMANDE (pour validation manuelle)
# ======================================================================

if __name__ == "__main__":
    """
    Permet de valider manuellement un fichier CSV depuis le terminal :
        python export_manager.py --file data/ebay_2026-03-14.csv
    """
    parser = argparse.ArgumentParser(
        description="Valide un fichier CSV exporté par le spider eBay"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Chemin vers le fichier CSV à valider"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Dossier data/ (défaut: data)"
    )
    args = parser.parse_args()

    manager = ExportManager(data_dir=args.data_dir)
    result  = manager.validate_and_register(filepath=args.file)

    print(f"\nRésultat : {result['status'].upper()}")
    print(f"Message  : {result['message']}")

    if result["status"] == "ready":
        print("\nFichiers en attente de chargement Bigtable :")
        for f in manager.get_pending_files():
            print(f"  - {f['file']} ({f['row_count']} lignes)")