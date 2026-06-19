# Schéma d'architecture — Pipeline Olist

## Vue d'ensemble

Le pipeline est composé de 5 notebooks PySpark, exécutés dans un ordre précis. Chaque notebook lit dans une zone et écrit dans la zone suivante.

```
raw/ (9 CSV)
    │
    ▼
bronze.ipynb ───────────► data/bronze/
    │
    ▼
silvernotebook.ipynb ───► data/silver/
    │
    ├──────────────┬──────────────┐
    ▼              ▼              ▼
gold_comptabilite  gold_logistique  gold_marketing
    │              │              │
    ▼              ▼              ▼
data/gold/account/  data/gold/logistique/  data/gold/marketing/
```

## Ordre d'exécution

1. **`bronze.ipynb`** — doit être exécuté en premier
2. **`silvernotebook.ipynb`** — dépend de `data/bronze/`
3. **`gold_comptabilite.ipynb`**, **`gold_logistique.ipynb`**, **`gold_marketing.ipynb`** — chacun dépend de `data/silver/`, mais sont indépendants entre eux (peuvent être exécutés dans n'importe quel ordre, ou en parallèle)

## Détail par notebook

### `bronze.ipynb`

**Entrée** : `raw/*.csv` (9 fichiers)
**Sortie** : `data/bronze/` (9 tables Parquet)

Traitements :
- Lecture des CSV avec options de parsing robustes (`multiLine=True`, `escape='"'`, `mode=PERMISSIVE`)
- Audit des valeurs nulles
- Validation des clés de jointure (`order_id`, `product_id`, `seller_id`, `customer_id`) via `left_anti` join pour détecter les lignes orphelines
- Export en Parquet, sans aucune modification de valeur

### `silvernotebook.ipynb`

**Entrée** : `data/bronze/`
**Sortie** : `data/silver/`

Traitements :
- Audit des nulls et des doublons sur toutes les tables
- Conversion de `review_score` en `IntegerType`
- Suppression des lignes sans clé primaire essentielle (`order_id`, `customer_id`, `product_id`)
- Valeurs par défaut : `review_comment_title`/`message` → chaîne vide, `product_category_name` → `"unknown"`, `freight_value` → `0.0`
- Déduplication sur les clés primaires (`order_id`, `customer_id`, `product_id`, `seller_id`), et sur `geolocation_zip_code_prefix` pour la géolocalisation
- Ajout des colonnes dérivées : `delivery_delay_days`, `is_late`, `actual_delivery_days`
- Vérification des incohérences temporelles (livraison avant achat, approbation avant achat)

### `gold_comptabilite.ipynb`

**Entrée** : `data/silver/`
**Sortie** : `data/gold/account/`

Indicateurs : chiffre d'affaires (total et mensuel, segmenté par statut), panier moyen, répartition des paiements, parcellement, chiffre d'affaires par état/région (client et vendeur), top vendeurs, chiffre d'affaires par catégorie.

### `gold_logistique.ipynb`

**Entrée** : `data/silver/`
**Sortie** : `data/gold/logistique/`

Indicateurs : délai moyen de livraison, taux de respect des délais, taux de retard, performance de livraison par état et par vendeur, coûts de frais de port.

### `gold_marketing.ipynb`

**Entrée** : `data/silver/`
**Sortie** : `data/gold/marketing/`

Indicateurs : note moyenne de satisfaction, impact des retards sur la satisfaction, chiffre d'affaires et note par catégorie, répartition géographique des clients, évolution mensuelle, fidélité des clients.

> **Point de vigilance** : `gold_marketing.ipynb` utilise `df_sellers` dans une jointure sans l'avoir chargé explicitement dans la cellule d'initialisation — à corriger avant l'exécution finale (`df_sellers = spark.read.parquet("../data/silver/sellers/")` manquant).

## Dépendances entre tables (au sein de silver)

| Table silver | Utilisée par |
|---|---|
| `orders` | les 3 notebooks gold |
| `customers` | comptabilité, logistique, marketing |
| `order_items` | comptabilité, logistique, marketing |
| `payments` | comptabilité, marketing |
| `products` | comptabilité, marketing |
| `sellers` | comptabilité, logistique, marketing |
| `reviews` | marketing |
| `product_category_name_translation` | comptabilité, marketing |
| `geolocation` | non utilisée directement en gold (état déjà présent dans `customers`/`sellers`) |

## Reproductibilité

Pour relancer le pipeline depuis zéro :
- Lancer le script

**Prérequis** : Java (JAVA_HOME configuré selon l'OS — voir README), PySpark installé, aucune variable `SPARK_REMOTE` définie (sinon la session bascule en mode Spark Connect, qui ne supporte pas certains appels utilisés dans les notebooks).
