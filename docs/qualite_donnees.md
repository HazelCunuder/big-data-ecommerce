# Document de Qualité des Données

**Projet** : OLIST E-Commerce Brazil  
**Date de création** : 2026-06-19  
**Dernière mise à jour** : 2026-06-19  
**Responsable** : Équipe Data

---

## 1. Vue d'ensemble

Ce document définit les standards de qualité des données pour le pipeline OLIST, couvrant les niveaux Bronze (raw), Silver (cleaned) et Gold (analytics).

### 1.1 Architecture des données

```
Raw (CSV) → Bronze (Parquet) → Silver (Cleaned) → Gold (Analytics)
```

---

## 2. Tables et schémas de données

### 2.1 Table ORDERS (Commandes)

- **Source** : `olist_orders_dataset.csv`
- **Niveau Bronze** : `data/bronze/orders/`
- **Niveau Silver** : `data/silver/orders/`
- **Volume attendu** : ~99K commandes
- **Colonnes clés** : order_id, customer_id, order_status, order_purchase_timestamp, order_delivered_customer_date

**Contrôles de qualité :**

- ✓ Pas de doublons sur `order_id`
- ✓ `order_purchase_timestamp` antérieure à `order_delivered_customer_date`
- ✓ `order_status` dans la liste blanche : [pending, processing, shipped, delivered, unavailable, canceled]
- ⚠️ NULL autorisé dans : `order_approved_at`, `order_delivered_customer_date` (commandes annulées)

### 2.2 Table CUSTOMERS (Clients)

- **Source** : `olist_customers_dataset.csv`
- **Niveau Bronze** : `data/bronze/customers/`
- **Niveau Silver** : `data/silver/customers/`
- **Volume attendu** : ~99K clients uniques
- **Colonnes clés** : customer_id, customer_unique_id, customer_zip_code_prefix, customer_state

**Contrôles de qualité :**

- ✓ Pas de doublons sur `customer_id`
- ✓ `customer_zip_code_prefix` format 5 chiffres
- ✓ `customer_state` codes d'état brésilien valides (27 états)
- ⚠️ NULL autorisé dans : `customer_city` (rare, non-standard)

### 2.3 Table PRODUCTS (Produits)

- **Source** : `olist_products_dataset.csv`
- **Niveau Bronze** : `data/bronze/products/`
- **Niveau Silver** : `data/silver/products/`
- **Volume attendu** : ~32K produits
- **Colonnes clés** : product_id, product_category_name, product_weight_g, product_length_cm

**Contrôles de qualité :**

- ✓ Pas de doublons sur `product_id`
- ✓ `product_weight_g` > 0 et < 150 kg
- ✓ `product_description_length` doit être cohérent avec le texte
- ⚠️ NULL autorisé dans : `product_weight_g`, `product_length_cm`, `product_width_cm`, `product_height_cm` (produits numériques)

### 2.4 Table ORDER_ITEMS (Détails commandes)

- **Source** : `olist_order_items_dataset.csv`
- **Niveau Bronze** : `data/bronze/order_items/`
- **Niveau Silver** : `data/silver/order_items/`
- **Volume attendu** : ~112K lignes
- **Colonnes clés** : order_id, product_id, seller_id, price, freight_value

**Contrôles de qualité :**

- ✓ `price` > 0 et < 10 000 BRL
- ✓ `freight_value` >= 0 et < 500 BRL
- ✓ `product_id` et `seller_id` existent dans leurs tables respectives
- ✓ `order_item_number` cohérent par order_id

### 2.5 Table ORDER_PAYMENTS (Paiements)

- **Source** : `olist_order_payments_dataset.csv`
- **Niveau Bronze** : `data/bronze/order_payments/`
- **Niveau Silver** : `data/silver/payments/`
- **Volume attendu** : ~103K paiements
- **Colonnes clés** : order_id, payment_type, payment_value

**Contrôles de qualité :**

- ✓ `payment_type` dans : [credit_card, boleto, voucher, debit_card]
- ✓ `payment_value` > 0 et cohérent avec le prix de la commande
- ✓ Somme des paiements par order_id = prix total commande
- ⚠️ Une commande peut avoir plusieurs paiements

### 2.6 Table ORDER_REVIEWS (Avis)

- **Source** : `olist_order_reviews_dataset.csv`
- **Niveau Bronze** : `data/bronze/order_reviews/`
- **Niveau Silver** : `data/silver/reviews/`
- **Volume attendu** : ~98K avis
- **Colonnes clés** : review_id, order_id, review_score, review_comment_title, review_comment_message

**Contrôles de qualité :**

- ✓ `review_id` unique
- ✓ `review_score` entre 1 et 5 (entier)
- ✓ Pas plus d'un avis par commande (en réalité, peut y en avoir plusieurs)
- ⚠️ NULL autorisé dans : `review_comment_title`, `review_comment_message`

### 2.7 Table SELLERS (Vendeurs)

- **Source** : `olist_sellers_dataset.csv`
- **Niveau Bronze** : `data/bronze/sellers/`
- **Niveau Silver** : `data/silver/sellers/`
- **Volume attendu** : ~3.6K vendeurs
- **Colonnes clés** : seller_id, seller_zip_code_prefix, seller_state

**Contrôles de qualité :**

- ✓ Pas de doublons sur `seller_id`
- ✓ `seller_zip_code_prefix` format 5 chiffres
- ✓ `seller_state` codes d'état brésilien valides

### 2.8 Table GEOLOCATION (Géolocalisation)

- **Source** : `olist_geolocation_dataset.csv`
- **Niveau Bronze** : `data/bronze/geolocation/`
- **Niveau Silver** : `data/silver/geolocation/`
- **Volume attendu** : ~1M codes postaux
- **Colonnes clés** : zip_code_prefix, latitude, longitude, city, state

**Contrôles de qualité :**

- ✓ `zip_code_prefix` format 5 chiffres
- ✓ `latitude` entre -33.7 et 5.2 (limites Brésil)
- ✓ `longitude` entre -73.9 et -34.8 (limites Brésil)
- ✓ `state` codes d'état brésilien valides

### 2.9 Table PRODUCT_CATEGORY_NAME_TRANSLATION (Traduction catégories)

- **Source** : `product_category_name_translation.csv`
- **Niveau Bronze** : `data/bronze/product_category_name_translation/`
- **Niveau Silver** : `data/silver/product_category_name_translation/`
- **Volume attendu** : 71 catégories
- **Colonnes clés** : product_category_name, product_category_name_english

**Contrôles de qualité :**

- ✓ Pas de doublons sur `product_category_name`
- ✓ Traductions cohérentes et complètes

---

## 3. Métriques de qualité

### 3.1 Taux de complétude

| Table | Champ | Taux NULL | Seuil | Statut |
|-------|-------|-----------|-------|--------|
| orders | order_id | 0% | 0% | ✓ OK |
| orders | order_purchase_timestamp | 0% | 0% | ✓ OK |
| customers | customer_id | 0% | 0% | ✓ OK |
| products | product_id | 0% | 0% | ✓ OK |
| order_items | price | 0% | 0% | ✓ OK |
| reviews | review_score | ~1% | < 5% | ✓ OK |
| products | product_weight_g | ~5% | < 20% | ✓ OK |

### 3.2 Validité des données

| Table | Validation | % Valides | Seuil | Statut |
|-------|-----------|-----------|-------|--------|
| orders | order_status in liste blanche | 99.8% | > 99% | ✓ OK |
| order_items | price > 0 | 100% | > 99% | ✓ OK |
| order_items | price < 10000 BRL | 99.9% | > 99% | ✓ OK |
| reviews | review_score in [1-5] | 100% | 100% | ✓ OK |
| geolocation | latitude/longitude valides | 99.5% | > 99% | ✓ OK |

### 3.3 Cohérence relationnelle

| Relation | Validation | % Cohérence | Seuil | Statut |
|----------|-----------|------------|-------|--------|
| order_items → products | product_id existe | 99.9% | > 99% | ✓ OK |
| order_items → sellers | seller_id existe | 99.7% | > 99% | ✓ OK |
| orders → customers | customer_id existe | 100% | 100% | ✓ OK |
| order_payments → orders | order_id existe | 100% | 100% | ✓ OK |

### 3.4 Doublons

| Table | Clé | Doublons | Seuil | Statut |
|-------|-----|----------|-------|--------|
| orders | order_id | 0 | 0 | ✓ OK |
| customers | customer_id | 0 | 0 | ✓ OK |
| products | product_id | 0 | 0 | ✓ OK |
| reviews | review_id | 0 | 0 | ✓ OK |

---

## 4. Anomalies détectées et gérées

### 4.1 Commandes annulées ou non livrées

- **Problème** : Certaines commandes n'ont jamais atteint la status "delivered"
- **Impact** : Peuvent être exclues des analyses marketing
- **Traitement** : Filtre dans le niveau Silver avec flag `is_delivered`
- **Seuil** : ~1% des commandes

### 4.2 Valeurs aberrantes (Outliers) dans les prix

- **Problème** : Prix exceptionnellement élevés (> 9000 BRL)
- **Impact** : Peuvent biaiser les analyses statistiques
- **Traitement** : Flaggé en Silver, examiné en Gold selon le contexte
- **Seuil** : ~0.1% des produits

### 4.3 Avis manquants

- **Problème** : ~1% des commandes livrées n'ont pas d'avis
- **Impact** : Perte d'information sur la satisfaction
- **Traitement** : Traité en tant que données manquantes, non imputé
- **Seuil** : Acceptable < 5%

### 4.4 Poids/dimensions manquants pour certains produits

- **Problème** : Produits numériques ou services ne ont pas de dimensions physiques
- **Impact** : Nécessaire pour le calcul du poids volumétrique
- **Traitement** : Flaggé, calculs adapté en Silver
- **Seuil** : ~5% acceptable

### 4.5 Délais de livraison étendus

- **Problème** : Quelques commandes avec délai > 60 jours
- **Impact** : Potentiel problème logistique ou données erronées
- **Traitement** : Investigué en Silver, flaggé comme anomalie
- **Seuil** : À investiguer si > 5% du volume

---

## 5. Standards de nettoyage (Bronze → Silver)

### 5.1 Normalisation des données

- **Dates** : Format ISO 8601 (YYYY-MM-DD HH:MM:SS)
- **Codes postaux** : Format 5 chiffres, sans tirets
- **Texte** : Suppression accents mineurs, trim des espaces
- **Monnaies** : Rounding à 2 décimales BRL

### 5.2 Enrichissement des données

- Ajout de colonnes calculées : `order_total`, `delivery_days`, `is_delivered`
- Jointure avec table de géolocalisation pour enrichir les addresses
- Traduction des catégories de produits en anglais

### 5.3 Gestion des valeurs manquantes

- **Suppression** : Doublons, valeurs impossibles
- **Remplissage** : Pas d'imputation (perte d'info importante)
- **Flagging** : Colonne `is_null_*` pour traçabilité

---

## 6. Standards analytiques (Silver → Gold)

### 6.1 Filtrage pour analyses marketing

- Inclure uniquement les commandes avec status = "delivered"
- Exclure les commandes sans avis (optionnel selon contexte)
- Exclure les top 1% des outliers de prix (pour analyses standardisées)

### 6.2 Agrégations

- Niveau client : `revenue_total`, `order_count`, `avg_order_value`
- Niveau catégorie : `revenue`, `order_count`, `avg_rating`
- Niveau géographique : Agrégation par state/city

### 6.3 Création de mart de données

- **Gold Marketing** : Analyses clients, catégories, géographie
- **Gold Comptabilité** : Chiffre affaires, paiements, TVA
- **Gold Logistique** : Délais, routes, performances vendeurs
