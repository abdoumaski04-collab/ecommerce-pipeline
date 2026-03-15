import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import time
import random

def scrap_jumia_pc_portables(max_pages=5):  # 🔹 AJOUT : paramètre pages
    base_url = "https://www.jumia.ma/pc-portables/?page={}"  # 🔹 AJOUT : URL avec page
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    tous_les_produits = []  # 🔹 AJOUT : liste pour accumuler
    
    print(f"📡 Scraping Jumia - PC Portables (max {max_pages} pages)")
    
    for page in range(1, max_pages + 1):  # 🔹 AJOUT : boucle sur les pages
        url = base_url.format(page)
        print(f"\n📄 Page {page}/{max_pages}...")
        
        time.sleep(random.uniform(4, 7))  # 🔹 AJOUT : pause entre pages
        
        try:
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                print(f"❌ Page {page} inaccessible")
                continue
            
            soup = BeautifulSoup(response.content, "html.parser")
            articles = soup.find_all("article", class_="prd")
            
            if not articles:  # 🔹 AJOUT : arrêt si page vide
                print(f"📭 Plus de produits à la page {page}, arrêt")
                break
            
            print(f"📦 {len(articles)} produits trouvés")
            
            for article in articles:
                try:
                    titre = article.find("h3", class_="name").text.strip()
                    prix = article.find("div", class_="prc").text.strip()
                    note_elem = article.find("div", class_="stars")
                    note = note_elem.text.strip() if note_elem else "Pas de note"
                    img_elem = article.find("img", class_="img")
                    image = img_elem.get("data-src") or img_elem.get("src") if img_elem else ""
                    
                    tous_les_produits.append({  # 🔹 AJOUT : ajout à la liste
                        "titre": titre,
                        "prix": prix,
                        "note": note,
                        "image": image,
                        "site": "jumia",
                        "categorie": "pc_portables",
                        "date": datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    continue
            
            print(f"✅ Page {page} terminée")
            
        except Exception as e:
            print(f"❌ Erreur page {page}: {e}")
            continue
    
    # Sauvegarde finale
    chemin = "../../data/jumia_pc_portables.json"
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(tous_les_produits, f, indent=2, ensure_ascii=False)
    
    print(f"\n🎉 TOTAL: {len(tous_les_produits)} PC portables sauvegardés dans {chemin}")
    return tous_les_produits

if __name__ == "__main__":
    scrap_jumia_pc_portables(max_pages=5)  # 🔹 AJOUT : appel avec paramètre