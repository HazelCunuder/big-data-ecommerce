"""
Pipeline complet Olist - Bronze, Silver et Gold (POO)
Ce script exécute tout le pipeline de traitement des données
"""

import os
import sys
from pathlib import Path

# Configuration PySpark pour Windows
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, round as _round,
    countDistinct, date_format, max as _max,
    collect_set, concat_ws, create_map, lit, coalesce,
    when, datediff
)
from pyspark.sql.types import DoubleType, IntegerType
from itertools import chain


class OlistPipeline:
    """Pipeline Olist complet - Bronze, Silver et Gold"""
    
    def __init__(self, app_name: str = "olist-pipeline-complet"):
        """Initialisation du pipeline"""
        self.spark = None
        self.app_name = app_name
        self.base_dir = Path.cwd()
        self.raw_dir = self.base_dir / "raw"
        self.data_dir = self.base_dir / "data"
        
        # DataFrames
        self.df_orders = None
        self.df_products = None
        self.df_customers = None
        self.df_items = None
        self.df_payments = None
        self.df_reviews = None
        self.df_sellers = None
        self.df_geo = None
        self.df_pcnt = None
        
    def initialize_spark(self) -> None:
        """Création de la session Spark"""
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        print("✓ Session Spark initialisée")
    
    def bronze_layer(self) -> None:
        """Niveau Bronze - Chargement des données brutes"""
        print("\n" + "=" * 80)
        print("NIVEAU BRONZE - Chargement des données brutes")
        print("=" * 80)
        
        # Lecture des fichiers CSV bruts
        self.df_orders = self._read_csv(self.raw_dir / "olist_orders_dataset.csv")
        self.df_products = self._read_csv(self.raw_dir / "olist_products_dataset.csv")
        self.df_customers = self._read_csv(self.raw_dir / "olist_customers_dataset.csv")
        self.df_items = self._read_csv(self.raw_dir / "olist_order_items_dataset.csv")
        self.df_payments = self._read_csv(self.raw_dir / "olist_order_payments_dataset.csv")
        self.df_geo = self._read_csv(self.raw_dir / "olist_geolocation_dataset.csv")
        self.df_reviews = self._read_csv(self.raw_dir / "olist_order_reviews_dataset.csv")
        self.df_sellers = self._read_csv(self.raw_dir / "olist_sellers_dataset.csv")
        self.df_pcnt = self._read_csv(self.raw_dir / "product_category_name_translation.csv")
        
        # Création des répertoires et sauvegarde
        bronze_dir = self.data_dir / "bronze"
        self._save_parquet(self.df_orders, bronze_dir / "orders")
        self._save_parquet(self.df_products, bronze_dir / "products")
        self._save_parquet(self.df_customers, bronze_dir / "customers")
        self._save_parquet(self.df_items, bronze_dir / "order_items")
        self._save_parquet(self.df_payments, bronze_dir / "order_payments")
        self._save_parquet(self.df_geo, bronze_dir / "geolocation")
        self._save_parquet(self.df_reviews, bronze_dir / "order_reviews")
        self._save_parquet(self.df_sellers, bronze_dir / "sellers")
        self._save_parquet(self.df_pcnt, bronze_dir / "product_category_name_translation")
        
        print("✓ Niveau Bronze terminé")
    
    def silver_layer(self) -> None:
        """Niveau Silver - Nettoyage et transformation"""
        print("\n" + "=" * 80)
        print("NIVEAU SILVER - Nettoyage et transformation")
        print("=" * 80)
        
        # Lecture des données Bronze
        bronze_dir = self.data_dir / "bronze"
        self.df_orders = self.spark.read.parquet(str(bronze_dir / "orders"))
        self.df_customers = self.spark.read.parquet(str(bronze_dir / "customers"))
        self.df_items = self.spark.read.parquet(str(bronze_dir / "order_items"))
        self.df_payments = self.spark.read.parquet(str(bronze_dir / "order_payments"))
        self.df_reviews = self.spark.read.parquet(str(bronze_dir / "order_reviews"))
        self.df_products = self.spark.read.parquet(str(bronze_dir / "products"))
        self.df_sellers = self.spark.read.parquet(str(bronze_dir / "sellers"))
        self.df_geo = self.spark.read.parquet(str(bronze_dir / "geolocation"))
        self.df_pcnt = self.spark.read.parquet(str(bronze_dir / "product_category_name_translation"))
        
        # Conversion des types
        self.df_reviews = self.df_reviews.withColumn(
            "review_score", col("review_score").cast(IntegerType())
        )
        
        # Suppression des NULL sur les clés
        self.df_orders = self.df_orders.dropna(subset=["order_id", "customer_id"])
        self.df_items = self.df_items.dropna(subset=["order_id", "product_id"])
        self.df_reviews = self.df_reviews.dropna(subset=["order_id"])
        
        # Insertion de valeurs par défaut
        self.df_reviews = self.df_reviews.fillna({
            "review_comment_title": "",
            "review_comment_message": ""
        })
        self.df_products = self.df_products.fillna({
            "product_category_name": "unknown"
        })
        self.df_items = self.df_items.fillna({"freight_value": 0.0})
        
        # Suppression des doublons
        self.df_orders = self.df_orders.dropDuplicates(["order_id"])
        self.df_customers = self.df_customers.dropDuplicates(["customer_id"])
        self.df_products = self.df_products.dropDuplicates(["product_id"])
        self.df_sellers = self.df_sellers.dropDuplicates(["seller_id"])
        self.df_geo = self.df_geo.dropDuplicates(["geolocation_zip_code_prefix"])
        
        # Ajout de colonnes calculées
        self.df_orders = self.df_orders \
            .withColumn(
                "delivery_delay_days",
                datediff("order_delivered_customer_date", "order_estimated_delivery_date")
            ) \
            .withColumn(
                "is_late",
                when(col("delivery_delay_days") > 0, 1).otherwise(0)
            ) \
            .withColumn(
                "actual_delivery_days",
                datediff("order_delivered_customer_date", "order_purchase_timestamp")
            )
        
        # Sauvegarde en Silver
        silver_dir = self.data_dir / "silver"
        self._save_parquet(self.df_orders, silver_dir / "orders")
        self._save_parquet(self.df_customers, silver_dir / "customers")
        self._save_parquet(self.df_items, silver_dir / "order_items")
        self._save_parquet(self.df_payments, silver_dir / "payments")
        self._save_parquet(self.df_reviews, silver_dir / "reviews")
        self._save_parquet(self.df_products, silver_dir / "products")
        self._save_parquet(self.df_sellers, silver_dir / "sellers")
        self._save_parquet(self.df_geo, silver_dir / "geolocation")
        self._save_parquet(self.df_pcnt, silver_dir / "product_category_name_translation")
        
        print("✓ Niveau Silver terminé")
    
    def gold_accounting(self) -> None:
        """Gold - Analyse comptable"""
        print("\n--- Gold Comptabilité ---")
        
        # Segmentation des commandes
        df_orders_delivered = self.df_orders.filter(col("order_status") == "delivered")
        df_orders_in_transit = self.df_orders.filter(
            col("order_status").isin(["created", "approved", "processing", "invoiced", "shipped"])
        )
        df_orders_excluded = self.df_orders.filter(
            col("order_status").isin(["canceled", "unavailable"])
        )
        
        # Chiffre d'affaires par segment
        df_gold_revenue_delivered = self.df_items.join(
            df_orders_delivered.select("order_id", "customer_id", "order_purchase_timestamp"),
            "order_id", "inner"
        )
        df_gold_revenue_in_transit = self.df_items.join(
            df_orders_in_transit.select("order_id", "customer_id", "order_purchase_timestamp"),
            "order_id", "inner"
        )
        df_gold_revenue_excluded = self.df_items.join(
            df_orders_excluded.select("order_id", "customer_id", "order_purchase_timestamp"),
            "order_id", "inner"
        )
        
        # Chiffre d'affaires mensuel
        df_monthly_revenue = df_gold_revenue_delivered.withColumn(
            "month", date_format(col("order_purchase_timestamp"), "yyyy-MM")
        ).groupBy("month").agg(
            _round(_sum("price"), 2).alias("ca_produits"),
            _round(_sum("freight_value"), 2).alias("total_frais_livraison"),
            _round(_sum("price") + _sum("freight_value"), 2).alias("ca_total")
        ).orderBy("month")
        
        # Répartition des paiements par type
        total_payments = self.df_payments.agg(_sum("payment_value")).collect()[0][0]
        df_gold_payment_breakdown = self.df_payments.groupBy("payment_type").agg(
            _round(_sum("payment_value"), 2).alias("valeur_totale"),
            count("*").alias("nb_transactions"),
            _round(avg("payment_value"), 2).alias("valeur_moyenne")
        ).withColumn(
            "pourcentage_valeur",
            _round((col("valeur_totale") / total_payments) * 100, 2)
        ).orderBy(col("valeur_totale").desc())
        
        # Parcellement moyen
        df_installments_by_type = self.df_payments.groupBy("payment_type").agg(
            _round(avg("payment_installments"), 2).alias("parcelles_moyennes"),
            count("*").alias("nb_transactions")
        ).orderBy(col("nb_transactions").desc())
        
        # Agrégation paiements
        df_payments_agg = self.df_payments.groupBy("order_id").agg(
            _sum("payment_value").alias("valeur_totale_payee"),
            _max("payment_installments").alias("max_parcelles"),
            concat_ws(",", collect_set("payment_type")).alias("types_paiement")
        )
        
        # Paiements par état
        total_customer_payments = df_payments_agg.join(
            self.df_orders.select("order_id", "customer_id"), "order_id", "inner"
        ).agg(_sum("valeur_totale_payee")).collect()[0][0]
        
        df_payments_by_customer_state = df_payments_agg.join(
            self.df_orders.select("order_id", "customer_id"), "order_id", "inner"
        ).join(
            self.df_customers.select("customer_id", "customer_state"), "customer_id", "inner"
        ).groupBy("customer_state").agg(
            _round(_sum("valeur_totale_payee"), 2).alias("valeur_totale"),
            count("*").alias("nb_commandes"),
            _round(avg("valeur_totale_payee"), 2).alias("panier_moyen")
        ).withColumn(
            "pourcentage_valeur",
            _round((col("valeur_totale") / total_customer_payments) * 100, 2)
        ).orderBy(col("valeur_totale").desc())
        
        # Paiements par région
        region_map = {
            "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
            "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
            "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
            "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
            "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
            "PR": "Sul", "RS": "Sul", "SC": "Sul"
        }
        mapping_expr = create_map([lit(x) for x in chain(*region_map.items())])
        
        df_payments_by_customer_region = df_payments_by_customer_state.withColumn(
            "region", mapping_expr[col("customer_state")]
        ).groupBy("region").agg(
            _round(_sum("valeur_totale"), 2).alias("valeur_totale"),
            _sum("nb_commandes").alias("nb_commandes")
        ).orderBy(col("valeur_totale").desc())
        
        # CA par état vendeur
        total_seller_revenue = self.df_items.agg(_sum("price")).collect()[0][0]
        df_revenue_by_seller_state = self.df_items.join(
            self.df_sellers.select("seller_id", "seller_state"), "seller_id", "inner"
        ).groupBy("seller_state").agg(
            _round(_sum("price"), 2).alias("valeur_totale"),
            count("*").alias("nb_items"),
            _round(avg("price"), 2).alias("prix_moyen")
        ).withColumn(
            "pourcentage_valeur",
            _round((col("valeur_totale") / total_seller_revenue) * 100, 2)
        ).orderBy(col("valeur_totale").desc())
        
        # Top vendeurs
        df_top_sellers = self.df_items.join(
            df_orders_delivered.select("order_id"), "order_id", "inner"
        ).join(
            self.df_sellers.select("seller_id", "seller_city", "seller_state"), "seller_id", "inner"
        ).groupBy("seller_id", "seller_city", "seller_state").agg(
            _round(_sum("price"), 2).alias("ca_genere"),
            count("*").alias("nb_items_vendus"),
            _round(avg("price"), 2).alias("prix_moyen")
        ).orderBy(col("ca_genere").desc())
        
        # CA par catégorie
        total_category_revenue = self.df_items.agg(_sum("price")).collect()[0][0]
        df_revenue_by_category = self.df_items.join(
            self.df_products.select("product_id", "product_category_name"), "product_id", "left"
        ).join(
            self.df_pcnt.select("product_category_name", "product_category_name_english"),
            "product_category_name", "left"
        ).withColumn(
            "categorie",
            coalesce(col("product_category_name_english"), col("product_category_name"))
        ).groupBy("categorie").agg(
            _round(_sum("price"), 2).alias("valeur_totale"),
            count("*").alias("nb_items"),
            _round(avg("price"), 2).alias("prix_moyen")
        ).withColumn(
            "pourcentage_valeur",
            _round((col("valeur_totale") / total_category_revenue) * 100, 2)
        ).orderBy(col("valeur_totale").desc())
        
        # Sauvegarde
        gold_account_dir = self.data_dir / "gold" / "account"
        self._save_parquet(df_gold_revenue_delivered, gold_account_dir / "revenue_delivered")
        self._save_parquet(df_gold_revenue_in_transit, gold_account_dir / "revenue_in_transit")
        self._save_parquet(df_gold_revenue_excluded, gold_account_dir / "revenue_excluded")
        self._save_parquet(df_monthly_revenue, gold_account_dir / "monthly_revenue")
        self._save_parquet(df_gold_payment_breakdown, gold_account_dir / "payment_breakdown")
        self._save_parquet(df_installments_by_type, gold_account_dir / "installments_by_type")
        self._save_parquet(df_payments_by_customer_state, gold_account_dir / "payments_by_customer_state")
        self._save_parquet(df_payments_by_customer_region, gold_account_dir / "payments_by_customer_region")
        self._save_parquet(df_revenue_by_seller_state, gold_account_dir / "revenue_by_seller_state")
        self._save_parquet(df_top_sellers, gold_account_dir / "top_sellers")
        self._save_parquet(df_revenue_by_category, gold_account_dir / "revenue_by_category")
        
        print("✓ Gold Comptabilité terminé")
    
    def gold_logistics(self) -> None:
        """Gold - Analyse logistique"""
        print("\n--- Gold Logistique ---")
        
        df_orders_delivered = self.df_orders.filter(col("order_status") == "delivered")
        
        # Délais de livraison
        df_delivery_delays = df_orders_delivered \
            .withColumn("delivery_delay_days", 
                        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
            .select("order_id", "delivery_delay_days")
        
        # Performance par état
        df_orders_with_customers = df_orders_delivered.join(
            self.df_customers, 
            df_orders_delivered["customer_id"] == self.df_customers["customer_id"],
            "inner"
        )
        
        df_performance_by_state = df_orders_with_customers \
            .withColumn("delivery_delay_days", 
                        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
            .withColumn("on_time", 
                        when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"), 1)
                        .otherwise(0)) \
            .groupBy("customer_state") \
            .agg(
                count("*").alias("total_orders"),
                _round(avg("delivery_delay_days"), 2).alias("avg_delivery_delay_days"),
                _round(avg("on_time") * 100, 2).alias("on_time_rate_pct"),
                count(when(col("on_time") == 0, 1)).alias("late_orders")
            ) \
            .orderBy("avg_delivery_delay_days")
        
        # Coûts de livraison
        df_freight_costs = self.df_items.groupBy("order_id") \
            .agg(
                _round(_sum("freight_value"), 2).alias("total_freight_cost"),
                count("*").alias("item_count"),
                _round(avg("freight_value"), 2).alias("avg_freight_per_item")
            )
        
        # Performance par vendeur
        df_orders_with_sellers = df_orders_delivered.join(
            self.df_items, "order_id", "inner"
        ).join(self.df_sellers, "seller_id", "inner")
        
        df_seller_performance = df_orders_with_sellers \
            .withColumn("delivery_delay_days", 
                        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
            .withColumn("on_time", 
                        when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"), 1)
                        .otherwise(0)) \
            .groupBy("seller_id", "seller_city", "seller_state") \
            .agg(
                count("*").alias("total_orders"),
                _round(avg("delivery_delay_days"), 2).alias("avg_delivery_delay_days"),
                _round(avg("on_time") * 100, 2).alias("on_time_rate_pct"),
                count(when(col("on_time") == 0, 1)).alias("late_orders"),
                _round(_sum("freight_value"), 2).alias("total_freight_value"),
                _round(avg("freight_value"), 2).alias("avg_freight_value")
            ) \
            .orderBy("total_orders", ascending=False)
        
        # Sauvegarde
        gold_logistics_dir = self.data_dir / "gold" / "logistique"
        self._save_parquet(df_delivery_delays, gold_logistics_dir / "delivery_delays")
        self._save_parquet(df_performance_by_state, gold_logistics_dir / "performance_by_state")
        self._save_parquet(df_freight_costs, gold_logistics_dir / "freight_costs")
        self._save_parquet(df_seller_performance, gold_logistics_dir / "seller_performance")
        
        print("✓ Gold Logistique terminé")
    
    def gold_marketing(self) -> None:
        """Gold - Analyse marketing"""
        print("\n--- Gold Marketing ---")
        
        # DataFrame central
        df_base = self.df_orders \
            .join(self.df_customers, "customer_id", "left") \
            .join(self.df_items, "order_id", "left") \
            .join(self.df_products, "product_id", "left") \
            .join(self.df_pcnt, "product_category_name", "left") \
            .join(self.df_payments, "order_id", "left") \
            .join(self.df_reviews, "order_id", "left") \
            .join(self.df_sellers, "seller_id", "left")
        
        df_base = df_base.filter(col("order_status") == "delivered")
        
        # Satisfaction globale
        df_gold_satisfaction = self.df_reviews \
            .join(self.df_orders, "order_id", "left") \
            .filter(col("order_status") == "delivered") \
            .agg(
                _round(avg("review_score"), 2).alias("note_moyenne"),
                count("review_id").alias("nombre_avis"),
            )
        
        # Retard vs Satisfaction
        df_gold_retard_satisfaction = df_base \
            .groupBy("is_late") \
            .agg(
                _round(avg("review_score"), 2).alias("note_moyenne"),
                count("order_id").alias("nombre_commandes")
            ) \
            .withColumn("statut_livraison",
                when(col("is_late") == 1, "En retard").otherwise("Dans les délais")
            ) \
            .select("statut_livraison", "note_moyenne", "nombre_commandes") \
            .orderBy("note_moyenne")
        
        # Top catégories
        df_gold_categories = df_base \
            .groupBy("product_category_name_english") \
            .agg(
                _round(_sum("price"), 2).alias("chiffre_affaires"),
                count("order_id").alias("nombre_commandes"),
                _round(avg("review_score"), 2).alias("note_moyenne")
            ) \
            .filter(col("product_category_name_english").isNotNull()) \
            .orderBy(col("chiffre_affaires").desc())
        
        # Géographie clients
        df_gold_geo = df_base \
            .groupBy("customer_state") \
            .agg(
                count("order_id").alias("nombre_commandes"),
                _round(_sum("price"), 2).alias("chiffre_affaires"),
                _round(avg("review_score"), 2).alias("note_moyenne")
            ) \
            .orderBy(col("chiffre_affaires").desc())
        
        # Évolution mensuelle
        df_gold_mensuel = df_base \
            .withColumn("mois", date_format("order_purchase_timestamp", "yyyy-MM")) \
            .groupBy("mois") \
            .agg(
                count("order_id").alias("nombre_commandes"),
                _round(_sum("price"), 2).alias("chiffre_affaires"),
                _round(avg("review_score"), 2).alias("note_moyenne")
            ) \
            .orderBy("mois")
        
        # Fidélité clients
        df_gold_fidelite = self.df_orders \
            .filter(col("order_status") == "delivered") \
            .groupBy("customer_id") \
            .agg(count("order_id").alias("nombre_commandes")) \
            .groupBy("nombre_commandes") \
            .agg(count("customer_id").alias("nombre_clients")) \
            .orderBy("nombre_commandes")
        
        # Sauvegarde
        gold_marketing_dir = self.data_dir / "gold" / "marketing"
        self._save_parquet(df_gold_satisfaction, gold_marketing_dir / "satisfaction_globale")
        self._save_parquet(df_gold_retard_satisfaction, gold_marketing_dir / "retard_satisfaction")
        self._save_parquet(df_gold_categories, gold_marketing_dir / "top_categories")
        self._save_parquet(df_gold_geo, gold_marketing_dir / "geo_clients")
        self._save_parquet(df_gold_mensuel, gold_marketing_dir / "evolution_mensuelle")
        self._save_parquet(df_gold_fidelite, gold_marketing_dir / "fidelite_clients")
        
        print("✓ Gold Marketing terminé")
    
    def gold_layer(self) -> None:
        """Niveau Gold - Toutes les analyses métier"""
        print("\n" + "=" * 80)
        print("NIVEAU GOLD - Analyses métier")
        print("=" * 80)
        
        # Relecture des données Silver
        silver_dir = self.data_dir / "silver"
        self.df_orders = self.spark.read.parquet(str(silver_dir / "orders"))
        self.df_customers = self.spark.read.parquet(str(silver_dir / "customers"))
        self.df_items = self.spark.read.parquet(str(silver_dir / "order_items"))
        self.df_payments = self.spark.read.parquet(str(silver_dir / "payments"))
        self.df_reviews = self.spark.read.parquet(str(silver_dir / "reviews"))
        self.df_products = self.spark.read.parquet(str(silver_dir / "products"))
        self.df_sellers = self.spark.read.parquet(str(silver_dir / "sellers"))
        self.df_pcnt = self.spark.read.parquet(str(silver_dir / "product_category_name_translation"))
        
        self.gold_accounting()
        self.gold_logistics()
        self.gold_marketing()
    
    def run(self) -> None:
        """Exécution complète du pipeline"""
        print("=" * 80)
        print("DÉBUT DU PIPELINE OLIST")
        print("=" * 80)
        
        try:
            self.initialize_spark()
            self.bronze_layer()
            self.silver_layer()
            self.gold_layer()
            
            print("\n" + "=" * 80)
            print("PIPELINE TERMINÉ AVEC SUCCÈS")
            print("=" * 80)
            print("\nRésumé:")
            print("- Niveau Bronze: Données brutes chargées et sauvegardées")
            print("- Niveau Silver: Données nettoyées et transformées")
            print("- Niveau Gold Comptabilité: Indicateurs financiers calculés")
            print("- Niveau Gold Logistique: Indicateurs logistiques calculés")
            print("- Niveau Gold Marketing: Indicateurs marketing calculés")
            print("\nLes données sont disponibles dans le répertoire data/")
            
        except Exception as e:
            print(f"\n❌ Erreur lors de l'exécution du pipeline: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()
                print("\n✓ Session Spark fermée")
    
    def _read_csv(self, path: Path) -> DataFrame:
        """Lecture d'un fichier CSV avec options standard"""
        return self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("sep", ",") \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .csv(str(path))
    
    def _save_parquet(self, df: DataFrame, path: Path) -> None:
        """Sauvegarde d'un DataFrame en Parquet"""
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write.mode("overwrite").parquet(str(path))


if __name__ == "__main__":
    pipeline = OlistPipeline()
    pipeline.run()
