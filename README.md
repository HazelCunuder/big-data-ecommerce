# Pipeline Big Data E-commerce — Olist

Pipeline locale PySpark construite à partir du dataset public **Brazilian E-Commerce Public Dataset by Olist**. Le projet transforme des fichiers CSV bruts en indicateurs métier exploitables (comptabilité, logistique, marketing), en suivant une architecture en quatre zones : **raw → bronze → silver → gold**.

## Sommaire

- [Architecture](#architecture)
- [Structure du repository](#structure-du-repository)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Exécution](#exécution)
- [Documentation](#documentation)
- [Indicateurs produits](#indicateurs-produits)
- [Limites connues](#limites-connues)

---

## Architecture

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

- **raw** : fichiers CSV d'origine, jamais modifiés
- **bronze** : données lues par Spark et sauvegardées en Parquet, sans transformation de valeur
- **silver** : données nettoyées, typées, dédupliquées, enrichies de colonnes calculées
- **gold** : indicateurs métier agrégés, répartis en trois axes (comptabilité, logistique, marketing)

Le détail complet (dépendances entre tables, traitements par notebook) est dans [`docs/schema_architecture.md`](docs/schema_architecture.md).

---

## Structure du repository

```
.
├── bronze
│   ├── bronze.ipynb            # Notebook : ingestion raw → bronze
│   └── first_model.md          # Modèle de données (ERD, jointures)
├── data                         # Généré à l'exécution — non versionné (.gitignore)
│   ├── bronze
│   ├── gold
│   │   ├── account
│   │   ├── logistique
│   │   └── marketing
│   └── silver
├── docs
│   ├── journal_incidents.md    # Incidents techniques rencontrés et résolus
│   ├── qualite_donnees.md      # Standards et contrôles qualité des données
│   └── schema_architecture.md  # Détail de l'architecture et des dépendances
├── gold
│   ├── gold_comptabilite.ipynb # Notebook : silver → gold (axe comptable)
│   ├── gold_logistique.ipynb   # Notebook : silver → gold (axe logistique)
│   └── gold_marketing.ipynb    # Notebook : silver → gold (axe marketing)
├── pipeline.py                  # Script unique : exécute tout le pipeline (bronze → silver → gold)
├── raw                           # Fichiers CSV sources (dataset Olist)
├── silver
│   └── silvernotebook.ipynb    # Notebook : bronze → silver
└── README.md
```

> **Note sur le dossier `data/`** : il est exclu du versioning Git (voir `.gitignore`) car il contient des artefacts générés, pas du code source. Il est recréé automatiquement à chaque exécution, soit par les notebooks (`bronze.ipynb` → `silvernotebook.ipynb` → `gold_*.ipynb`), soit par `pipeline.py` en une seule commande. Les deux méthodes produisent le même résultat dans `data/`.

---

## Prérequis

- **Python** 3.12
- **Java** 11 ou 17 (requis par PySpark) — vérifier avec `java -version`
- **PySpark** 3.5+ (inclut Spark, pas besoin d'installation séparée du binaire Apache Spark)

### Vérification de l'environnement avant exécution

Aucune variable `SPARK_REMOTE` ne doit être définie — sinon la session Spark bascule en mode **Spark Connect**, qui ne supporte pas certains appels utilisés dans les notebooks (ex. `sparkContext.setLogLevel`) :

```bash
echo $SPARK_REMOTE   # doit être vide
```

---

## Installation

```bash
# Cloner le repository
git clone <url-du-repo>
cd <nom-du-repo>

# Créer et activer un environnement virtuel
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
.venv\Scripts\activate           # Windows

# Installer les dépendances
pip install -r requirements.txt
```

### `requirements.txt`

```
pyspark>=3.5.0
pandas
python-dotenv
jupyter
matplotlib
```

### Configuration de `JAVA_HOME`

PySpark a besoin de savoir où se trouve Java. Créer un fichier `.env` à la racine du projet (non versionné) :

```
JAVA_HOME=/chemin/vers/votre/java
```

Pour trouver ce chemin :

```bash
# macOS
/usr/libexec/java_home -V

# Linux
update-alternatives --list java

# Windows (PowerShell)
where.exe java
```

> Le chemin Java varie selon le système d'exploitation et l'installation de chacun — ne jamais le coder en dur dans le notebook partagé. Le chargement via `.env` + `python-dotenv` (voir `INC-001` dans le journal d'incidents) résout ce problème de portabilité entre machines.

---

## Exécution

### Option A — Notebooks, étape par étape

Exécuter dans cet ordre exact (`Restart Kernel and Run All` pour chaque notebook, afin de garantir la reproductibilité) :

```
1. bronze/bronze.ipynb
2. silver/silvernotebook.ipynb
3. gold/gold_comptabilite.ipynb
4. gold/gold_logistique.ipynb
5. gold/gold_marketing.ipynb
```

Les trois notebooks gold sont indépendants entre eux : ils peuvent être exécutés dans n'importe quel ordre une fois `silvernotebook.ipynb` terminé.

### Option B — Script unique

```bash
python pipeline.py
```

Exécute l'intégralité du pipeline (bronze → silver → gold) en une seule commande. Recommandé pour vérifier rapidement la reproductibilité de bout en bout.

### Données placées dans `raw/`

Avant toute exécution, télécharger le [dataset Olist sur Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) et placer les 9 fichiers CSV dans `raw/`.

---

## Documentation

| Document | Contenu |
|---|---|
| [`bronze/first_model.md`](bronze/first_model.md) | Modèle de données (ERD), tables, jointures identifiées |
| [`docs/schema_architecture.md`](docs/schema_architecture.md) | Architecture détaillée, ordre d'exécution, dépendances entre tables |
| [`docs/qualite_donnees.md`](docs/qualite_donnees.md) | Standards et contrôles qualité (complétude, validité, cohérence, doublons) |
| [`docs/journal_incidents.md`](docs/journal_incidents.md) | Incidents techniques rencontrés (environnement, parsing CSV) et leur résolution |

---

## Indicateurs produits

### Axe comptable (`data/gold/account/`)

Chiffre d'affaires (total et mensuel, segmenté par statut de commande), panier moyen, répartition des paiements par type, parcellement moyen, chiffre d'affaires par état/région (client et vendeur), top vendeurs, chiffre d'affaires par catégorie de produit.

### Axe logistique (`data/gold/logistique/`)

Délai moyen de livraison, taux de respect des délais, taux de retard, performance de livraison par état et par vendeur, coûts de frais de port.

### Axe marketing (`data/gold/marketing/`)

Note moyenne de satisfaction, impact des retards sur la satisfaction client, chiffre d'affaires et note par catégorie, répartition géographique des clients, évolution mensuelle des commandes, fidélité des clients.

---

## Limites connues

- La déduplication de `geolocation` par `zip_code_prefix` retient une occurrence arbitraire (`dropDuplicates`) plutôt qu'une moyenne des coordonnées GPS — suffisant pour un rattachement à l'état/la ville, insuffisant pour une analyse géographique fine.
- L'offre (vendeurs) et la demande (clients) sont fortement concentrées sur l'état de São Paulo, ce qui limite la représentativité de certains indicateurs régionaux à faible volume.
- `gold_marketing.ipynb` dépend de `df_sellers`, à charger explicitement depuis `data/silver/sellers/` si ce n'est pas déjà fait dans la cellule d'initialisation.
- Le pipeline n'a pas encore été testé de façon exhaustive sur un environnement totalement vierge (voir `docs/journal_incidents.md`, incident lié à Spark Connect).