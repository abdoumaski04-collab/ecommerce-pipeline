import json
import os

def nettoyer_prix(prix_str):
    """Convertit '2,250.00 Dhs' en nombre float"""
    if prix_str == "Prix non disponible" or "Pas de note" in prix_str:
        return None
    
    # Enlever " Dhs" et les espaces
    prix_propre = prix_str.replace(" Dhs", "").strip()
    prix_propre = prix_propre.replace("EUR", "").strip()
    
    # Remplacer la virgule par rien
    prix_propre = prix_propre.replace(",", "")
    
    try:
        return float(prix_propre)
    except:
        return None

def ajouter_devise(fichier_entree, fichier_sortie=None):
    """
    Ajoute le champ 'devise' à chaque produit
    et nettoie le prix en nombre
    """
    
    # Lire le fichier existant
    with open(fichier_entree, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"\n📄 Traitement de {fichier_entree}...")
    print(f"   {len(data)} produits trouvés")
    
    produits_modifies = []
    
    for produit in data:
        # Nettoyer le prix
        prix_propre = nettoyer_prix(produit['prix'])
        
        # Créer un nouveau produit avec devise
        nouveau_produit = {
            "titre": produit['titre'],
            "prix": prix_propre,  # Prix en nombre
            "prix_original": produit['prix'],  # On garde l'original pour référence
            "devise": "Dhs",  # Ajout de la devise
            "note": produit['note'],
            "image": produit['image'],
            "site": produit['site'],
            "categorie": produit['categorie'],
            "date": produit['date']
        }
        
        produits_modifies.append(nouveau_produit)
    
    # Définir le fichier de sortie
    if fichier_sortie is None:
        # Créer un nouveau nom (ex: jumia_smartphones_devise.json)
        base = os.path.splitext(fichier_entree)[0]
        fichier_sortie = f"{base}_devise.json"
    
    # Sauvegarder
    with open(fichier_sortie, 'w', encoding='utf-8') as f:
        json.dump(produits_modifies, f, indent=2, ensure_ascii=False)
    
    print(f"   ✅ {len(produits_modifies)} produits sauvegardés dans {fichier_sortie}")
    print(f"   💰 Devise ajoutée: Dhs")
    
    return produits_modifies

def traiter_tous_les_fichiers():
    """Traite tous les fichiers Jumia"""
    
    # Liste des fichiers à traiter
    fichiers = [
        "data/jumia_smartphones.json",
        "data/jumia_pc_portables.json",
        "data/jumia_montres.json"
    ]
    
    print("🚀 AJOUT DES DEVISES AUX FICHIERS JUMIA")
    print("=" * 50)
    
    for fichier in fichiers:
        if os.path.exists(fichier):
            ajouter_devise(fichier)
        else:
            print(f"⚠️ Fichier {fichier} non trouvé")
    
    print("\n✅ Traitement terminé !")
    print("📁 Nouveaux fichiers créés avec '_devise.json'")

if __name__ == "__main__":
    traiter_tous_les_fichiers()