# 🎮 Big Data Project — Steam Market Analysis (Ubisoft)

## 🎯 Objectifs du Projet
Ce projet d'Analyse Exploratoire de Données (EDA) a été réalisé dans un contexte de Business Intelligence pour le compte d'Ubisoft. L'objectif principal est d'analyser globalement le marché de la plateforme Steam à l'aide de technologies Big Data afin de modéliser et comprendre les facteurs clés qui dictent la popularité, la satisfaction et la rentabilité (chiffre d'affaires estimé) des jeux vidéo.

---

## 📬 Livrables & Liens Databricks Publics

Pour des raisons d'optimisation de la mémoire et pour respecter les contraintes techniques de taille de publication de Databricks, le projet a été scindé en deux volets complémentaires :

* **[🔗 Partie 1 : Chargement, Nettoyage & Analyse Macro](https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/1871887574472583?o=7474652616624392)**
  * *Contenu :* Connexion au bucket Amazon S3 (`s3://...`), flattening dynamique des structures complexes et imbriquées du JSON (gestion des tags), typage strict des données, étude de la temporalité des sorties (impact de la période Covid-19), distribution globale des structures de prix, étude de l'internationalisation (langues) et des restrictions d'âge.
  * *Outils :* PySpark SQL, API Regexp, Databricks native charting.

* **[🔗 Partie 2 : Analyses Segmentées — Genres & Plateformes](https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/4473242428596805?o=7474652616624392)**
  * *Contenu :* Analyse unitaire et avancée des genres via l'explosion des tableaux de données composites (`F.explode`), calcul des ratios réels de satisfaction de l'audience, modélisation des stratégies de spécialisation des éditeurs via l'usage de fonctions de fenêtrage avancées (`Window.partitionBy`), estimation financière du chiffre d'affaires brut par segment et cartographie des préférences de portages techniques (Windows vs Mac vs Linux).
  * *Outils :* PySpark Window Functions, Multidimensional Aggregations.

---

## 🚀 Synthèse Décisionnelle : Facteurs Clés de Succès

1. **Hégémonie du modèle Premium sur l'Action/RPG :** Bien que les jeux indépendants à bas coût saturent les volumes de publication, la valeur financière et l'engagement de masse se concentrent sur les segments Action et RPG, validant le positionnement AAA d'Ubisoft.
2. **Priorisation technologique de l'infrastructure Windows :** Windows détient un monopole de fait (99% de disponibilité globale). Les portages macOS/Linux ne se justifient économiquement que sur les genres légers (Casual/Stratégie 2D) portés par des moteurs tiers automatisés (Unity/Godot).
3. **Criticité de la réputation initiale :** Les genres grand public souffrent d'une forte volatilité des avis. Une exécution technique irréprochable au Jour J est indispensable pour maintenir le score algorithmique sur le magasin Steam.
