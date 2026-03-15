import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time
import random

def scrap_jumia_montres(max_pages=5):
    # URL de la catégorie montres connectées
    base_url = "https://www.jumia.ma/montres-connectees/?page={}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    tous_les_produits = []
    
    print("📡 Scraping Jumia - Montres connectées")
    print(f"📄 Pages prévues: {max_pages}\n")
    
    for page in range(1, max_pages + 1):
        url = base_url.format(page)
        print(f"📄 Page {page}/{max_pages}...")
        
        # Pause aléatoire entre les pages
        time.sleep(random.uniform(4, 7))
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Page {page} inaccessible (code {response.status_code})")
                continue
            
            soup = BeautifulSoup(response.content, "html.parser")
            articles = soup.find_all("article", class_="prd")
            
            if not articles:
                print(f"📭 Plus de produits à la page {page}, arrêt")
                break
            
            print(f"📦 {len(articles)} produits trouvés")
            
            for article in articles:
                try:
                    # Titre
                    titre_elem = article.find("h3", class_="name")
                    titre = titre_elem.text.strip() if titre_elem else "Titre non disponible"
                    
                    # Prix
                    prix_elem = article.find("div", class_="prc")
                    prix = prix_elem.text.strip() if prix_elem else "Prix non disponible"
                    
                    # Note
                    note_elem = article.find("div", class_="stars")
                    note = note_elem.text.strip() if note_elem else "Pas de note"
                    
                    # Image
                    img_elem = article.find("img", class_="img")
                    image = img_elem.get("data-src") or img_elem.get("src") if img_elem else ""
                    
                    tous_les_produits.append({
                        "titre": titre,
                        "prix": prix,
                        "note": note,
                        "image": image,
                        "site": "jumia",
                        "categorie": "montres_connectees",
                        "date": datetime.now().isoformat()
                    })
                    
                    print(f"  ✅ {titre[:40]}... {prix}")
                    
                except Exception as e:
                    print(f"  ⚠️ Erreur sur un produit: {e}")
                    continue
            
            print(f"✅ Page {page} terminée ({len(tous_les_produits)} produits cumulés)\n")
            
        except Exception as e:
            print(f"❌ Erreur page {page}: {e}")
            continue
    
    # Sauvegarde finale
    chemin = "../../data/jumia_montres.json"
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(tous_les_produits, f, indent=2, ensure_ascii=False)
    
    print(f"\n🎉 SCRAPING TERMINÉ !")
    print(f"📦 TOTAL: {len(tous_les_produits)} montres connectées")
    print(f"💾 Fichier sauvegardé: {chemin}")
    
    return tous_les_produits

if __name__ == "__main__":
    # Lancer le scraping (ajuster max_pages selon besoin)
    scrap_jumia_montres(max_pages=5)