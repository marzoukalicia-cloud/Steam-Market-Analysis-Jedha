# 🎮 Projet Big Data — Analyse Globale du Marché Steam (Ubisoft)

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=flat&logo=python&logoColor=fff)](#)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=fff)](#)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=fff)](#)
[![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=flat&logo=amazons3&logoColor=fff)](#)
[![JEDHA](https://img.shields.io/badge/JEDHA-Bloc%202-blueviolet?style=flat)](#)

## 🎯 Contexte & Objectif du Projet
Ce projet a été réalisé dans une posture de Business Intelligence pour le compte d'**Ubisoft** dans le cadre du Bootcamp JEDHA (Bloc 2 - Big Data). L'objectif est de conduire une analyse exploratoire (EDA) globale du marché de la plateforme Steam afin d'identifier les facteurs critiques de succès (politique tarifaire, choix de portage technique, segmentation des genres) qui influencent directement la popularité et les ventes d'un jeu vidéo pour guider nos futurs lancements sur PC.

---

## 💾 Source des Données (Dataset)
L'intégralité de l'analyse repose sur un jeu de données semi-structuré (JSON) contenant l'ensemble des métadonnées du catalogue Steam, requêté directement depuis notre infrastructure cloud :
* **Volume :** ~55 000 jeux (prix, genres, éditeurs, avis, plateformes, langues et dates de sortie).
* **Accès Data Lake :** `s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json`

---

## 📬 Liens des Notebooks Databricks Publics
Pour contourner les contraintes techniques de taille limite de publication de Databricks et garantir une exécution mémoire optimale sur les clusters Spark, le projet a été scindé en deux volets hautement complémentaires :

* **🔗 Partie 1 : Chargement, Nettoyage & Analyse Macro/lien databriks: https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/1871887574472583?o=7474652616624392**
  * *Périmètre technique :* Connexion au bucket Amazon S3, extraction et aplatissement dynamique (*flattening*) du schéma JSON imbriqué, typage strict des données et analyses macro-économiques (Q1 à Q6 + Q13).
* **🔗 Partie 2 : Analyses Sectorielles — Genres & Plateformes/lien databriks: https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/4473242428596805?o=7474652616624392**
  * *Périmètre technique :* Explosion des chaînes de caractères composites (`F.explode`) pour isoler les genres de façon unitaire, implémentation de fonctions de fenêtrage avancées (`Window.partitionBy`), modélisation du chiffre d'affaires estimé et matrices de compatibilité technique par OS (Q7 à Q12).

---

## 📊 Pipeline d'Analyse : Questions & Réponses Métier

| # | Question traitée | Approche technique (PySpark) | Ce que révèlent nos données brutes | Impact Stratégique (Ubisoft) |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Top publishers et developers** | `groupBy("publisher")` + tri décroissant. | Identification des éditeurs leaders en volume de titres sortis sur la boutique. | Permet de cartographier précisément le volume d'occupation de la concurrence sur la plateforme. |
| **2** | **Sorties par année + focus COVID** | Extraction de l'année par regex + agrégation temporelle. | **Pic massif de sorties en 2020 (9 501 jeux) et 2021 (11 391 jeux)** contre environ 7 000 en 2018. | Le marché a subi une accélération phénoménale de l'offre, augmentant drastiquement le niveau de saturation. |
| **3** | **Distribution des prix** | Segmentation par tranches de prix (`F.when`). | **Une écrasante majorité de jeux sont vendus à moins de 5$ ou sont Free-to-Play.** | Le catalogue Steam est structurellement saturé par des micro-produits indépendants à bas coût. |
| **4** | **Top langues supportées** | `groupBy("languages")` + décompte global. | **L'anglais domine avec plus de 41 000 jeux**, suivi par le chinois et le russe. | La localisation linguistique complète (notamment le Chinois) est obligatoire pour maximiser l'audience cible. |
| **5** | **Restrictions d'âge (16+/18+)** | Filtrage et extraction numérique sur `required_age`. | Les jeux avec restrictions strictes d'âge représentent une part minoritaire mais stable du catalogue. | Le marché mature (16+/18+) reste parfaitement viable pour les productions AAA complexes d'Ubisoft. |
| **6** | **Hall of Fame (top 10 jeux)** | Ratio d'avis positifs pondéré avec un filtre de confiance `&gt; 50` avis. | Les jeux majeurs de premier plan maintiennent des scores d'approbation publique extrêmement élevés. | La réputation algorithmique initiale est le premier levier de découvrabilité sur la boutique. |
| **7** | **Genres les plus représentés** | `F.explode(F.split(genre))` pour isoler les genres unitaires. | **Les catégories Indie (37 671), Action (19 862) et Adventure (17 072)** saturent l'offre de titres. | Les genres phares d'Ubisoft font face à la plus forte intensité concurrentielle en volume. |
| **8** | **Rating moyen par genre** | Moyenne de la note calculée groupée par genre unitaire explosé. | Les genres de niche ciblés (**RPG, Simulation, Stratégie**) maintiennent des scores de satisfaction très stables. | Les communautés spécialisées se révèlent plus fidèles si la promesse de gameplay est tenue. |
| **9** | **Genres les plus lucratifs** | Analyse des prix moyens et des volumes de ventes estimés par genre. | Les genres à forte rejouabilité et de niche conservent des prix moyens d'entrée plus résilients. | Permet d'ajuster la grille tarifaire d'un titre selon la propension à payer de sa communauté. |
| **10** | **ADN des éditeurs par genre** | Fenêtrage analytique `Window` + filtrage sur le Rang 1 du volume. | Les grands éditeurs n'éparpillent pas leurs efforts : ils se spécialisent massivement sur un genre maître. | Permet de détecter la couleur éditoriale historique et le savoir-faire technique de chaque concurrent. |
| **11** | **Distribution Win/Mac/Linux** | Somme des variables binaires converties divisée par le volume total. | **Windows détient un monopole absolu (99,9%)**. Mac (11,4%) et Linux (7,4%) sont très marginaux. | Windows est le passage obligé. Ignorer Mac/Linux n'impacte pas la viabilité commerciale globale. |
| **12** | **Support OS par genre** | Taux de compatibilité des OS croisés par genre unitaire propre. | Les genres légers (*Casual/Indie*) s'exportent plus sur Mac/Linux. **Les jeux d'Action AAA restent exclusifs à Windows.** | Les investissements de portage sur les moteurs lourds propriétaires doivent viser Windows en priorité. |
| **13** | **Analyse des promotions** | Calcul des pourcentages sur le champ binaire/calculé `discount`. | La présence de remises de prix et de soldes est un standard de comportement d'achat sur la plateforme. | Les mécaniques promotionnelles sont indispensables pour relancer le cycle de vie commercial d'un jeu. |

---

## 🛠️ Installation & Exécution
Ce notebook est conçu pour être exécuté directement sur **Databricks** via un cluster **PySpark**. Aucune installation locale n'est requise pour la partie cloud.

Pour une exécution ou un développement sur une infrastructure locale, installez l'environnement de calcul via pip :
```bash
pip install pyspark
```

## 💻 Stack Technique
Outil,Usage
PySpark,"Traitement distribué des données (DataFrames, transformations complexes, Window Functions, Regex)"
Databricks,"Environnement d'exécution cloud, visualisation avancée (Charts natifs) et dashboarding"
AWS S3,"Data Lake cloud, stockage de la source JSON brute distribuée"
Python,"Langage principal, manipulation de chaînes de caractères et fonctions natives (builtins.round)"

## 🚀 Synthèse Décisionnelle : Qu'est-ce qui dicte les ventes sur Steam ?
L'analyse finale de nos données met en évidence trois piliers fondamentaux pour la feuille de route d'Ubisoft :

Windows Hegemony: Windows represents the absolute standard for PC gaming, maintaining a near 100% availability rate across all analyzed categories.

Mac & Linux Porting Trends: Alternative platforms are leverageable mainly by lighter categories like Casual, Indie, and Strategy, whereas resource-intensive genres like Action and RPG remain largely exclusive to Windows due to high development and optimization costs.

The Revenue Focus: While independent games saturate the platform in terms of total volume, real financial value and mass player engagement are heavily concentrated within the Action and RPG segments. This strongly validates Ubisoft's core strategic focus on premium, large-scale AAA productions.

## 📂 Structure du Projet
📦 projet-steam-bigdata
 ┣ 📂 notebooks
 ┃ ┣ 📜 Steam_part1_loading_cleaning_AMK.ipynb
 ┃ ┗ 📜 Steam_part2_EDA_viz_AMK.ipynb
 ┗ 📜 README.md
