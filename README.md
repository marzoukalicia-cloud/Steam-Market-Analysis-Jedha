# 🎮 Projet Big Data — Analyse Globale du Marché Steam (Ubisoft)

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=flat&logo=python&logoColor=fff)](#)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=fff)](#)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=fff)](#)
[![AWS S3](https://img.shields.io/badge/AWS_S3-569A31?style=flat&logo=amazons3&logoColor=fff)](#)
[![JEDHA](https://img.shields.io/badge/JEDHA-Bloc%202-blueviolet?style=flat)](#)

## 🎯 Contexte & Objectif du Projet
Ce projet a été réalisé dans une posture de Business Intelligence pour le compte d'**Ubisoft** dans le cadre du Bootcamp JEDHA (Bloc 2 - Big Data). L'objectif est de conduire une analyse exploratoire (EDA) globale du marché de la plateforme Steam afin d'identifier les facteurs critiques de succès (politique tarifaire, choix de portage technique, segmentation des genres) qui rencontrent et influencent directement la popularité, la satisfaction et les ventes d'un jeu vidéo pour guider nos futurs lancements sur PC.

---

## 💾 Source des Données (Dataset)
L'intégralité de l'analyse repose sur un jeu de données semi-structuré (JSON) contenant l'ensemble des métadonnées du catalogue Steam, requêté directement depuis notre infrastructure cloud :
* **Volume :** ~55 000 jeux (prix, genres, éditeurs, avis, plateformes, langues et dates de sortie).
* **Accès Data Lake :** `s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json`

---

## 📬 Liens des Notebooks Databricks Publics
Pour contourner les contraintes techniques de taille limite de publication de Databricks et garantir une exécution mémoire optimale sur les clusters Spark, le projet a été scindé en deux volets hautement complémentaires :

* **🔗 Partie 1 : Chargement, Nettoyage & Analyse Macro/lien DataBriks: https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/1871887574472583?o=7474652616624392**
  * *Périmètre technique :* Connexion au stockage Amazon S3, extraction et aplatissement dynamique (*flattening*) du schéma JSON imbriqué, typage strict des données et analyses macro-économiques (Q1 à Q6 + Q13).
* **🔗 Partie 2 : Analyses Sectorielles — Genres & Plateformes/lien DataBriks: https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/4473242428596805?o=7474652616624392**
  * *Périmètre technique :* Explosion des chaînes de caractères composites (`F.explode`) pour normaliser et isoler les genres de façon unitaire, implémentation de fonctions de fenêtrage avancées (`Window.partitionBy`), modélisation du chiffre d'affaires estimé et matrices de compatibilité technique par OS (Q7 à Q12).

---

## 📊 Pipeline d'Analyse : Questions & Réponses Métier

| # | Question traitée | Approche technique (PySpark) | Résultats du Catalogue (Énoncé) | Levier Stratégique (Ubisoft) |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Top publishers et developers** | `groupBy("publisher")` + tri décroissant. | **Big Fish Games** est l'éditeur qui a publié le plus de jeux. | Révèle une stratégie d'occupation de l'espace par des acteurs spécialisés dans le volume de jeux Casual. |
| **2** | **Sorties par année + focus COVID** | Extraction de l'année par regex + agrégation temporelle. | **2021** est l'année avec le plus de sorties de jeux. | Confirme l'explosion de l'offre et de la concurrence post-crise sanitaire. |
| **3** | **Distribution des prix** | Segmentation par tranches de prix (`F.when`). | **La majorité des jeux ont un prix inférieur à 4,99$.** | Le marché PC est inondé de titres à très bas coût, imposant une forte barrière à la visibilité. |
| **4** | **Top langues supportées** | `groupBy("languages")` + décompte global. | **L'anglais, le chinois simplifié, le russe et le portugais brésilien** sont les plus représentés. | La localisation internationale ne doit plus se limiter au bloc occidental (EFIGS). |
| **5** | **Restrictions d'âge (16+/18+)** | Filtrage et extraction numérique sur `required_age`. | Les jeux réservés aux adultes forment une **part minoritaire** du catalogue Steam. | L'audience générale reste le cœur du volume, mais les niches matures restent viables si ciblées. |
| **6** | **Hall of Fame (top 10 jeux)** | Ratio d'avis positifs pondéré avec un filtre de confiance `> 50` avis. | **Portal 2 et Left 4 Dead 2** dominent le classement des jeux les mieux notés. | Souligne l'importance de l'engagement communautaire et de la rejouabilité pour rester au sommet. |
| **7** | **Genres les plus représentés** | `F.explode(F.split(genre))` pour isoler les genres unitaires. | Le genre **Indie** domine largement le catalogue en nombre absolu de titres. | Intensité concurrentielle extrême sur le secteur indépendant. |
| **8** | **Rating moyen par genre** | Moyenne de la note calculée groupée par genre unitaire explosé. | Les genres **RPG et Simulation** obtiennent globalement les meilleurs avis des joueurs. | Ces genres bénéficient de fortes mécaniques d'attachement et d'une communauté d'ultra-passionnés. |
| **9** | **Genres les plus lucratifs** | Analyse des prix moyens et des volumes de ventes estimés par genre. | Les genres **Simulation et Strategy** affichent le prix moyen le plus élevé du marché. | Démontre une plus forte propension à payer des joueurs pour des expériences complexes et chronophages. |
| **10** | **ADN des éditeurs par genre** | Fenêtrage analytique `Window` + filtrage sur le Rang 1 du volume. | Les éditeurs ont une tendance marquée à la **spécialisation** sur un ou deux genres spécifiques. | Valide le besoin pour Ubisoft de capitaliser sur ses piliers historiques (Action/Aventure) sans s'éparpiller. |
| **11** | **Distribution Win/Mac/Linux** | Somme des variables binaires converties divisée par le volume total. | Les jeux sont disponibles **très majoritairement sur Windows**, les autres OS étant très marginaux. | Windows demeure le passage obligatoire pour rentabiliser n'importe quel investissement PC. |
| **12** | **Support OS par genre** | Taux de compatibilité des OS croisés par genre unitaire propre. | Les genres **Casual et Indie** privilégient plus l'accessibilité multi-plateforme (Mac/Linux). | Les jeux d'Action lourds économisent les coûts de portage en ciblant exclusivement Windows. |
| **13** | **Analyse des promotions** | Calcul des pourcentages sur le champ binaire/calculé `discount`. | **Une part significative** du catalogue profite régulièrement de remises et soldes. | La promotion n'est pas un événement rare mais un outil standard d'acquisition de parts de marché. |

---

## 🛠️ Installation & Exécution
Ce notebook est conçu pour être exécuté directement sur **Databricks** via un cluster **PySpark**. Aucune installation locale n'est requise pour la partie cloud.

Pour une exécution ou un développement sur une infrastructure locale, installez l'environnement de calcul via pip :
```bash
pip install pyspark
```

## 💻 Stack Technique
PySpark: Traitement distribué des données (DataFrames, transformations complexes, Window Functions, Regex)

Databricks: Environnement d'exécution cloud, visualisation avancée (Charts natifs) et dashboarding

AWS S3: Data Lake cloud, stockage de la source JSON brute distribuée

Python: Langage principal, manipulation de chaînes de caractères et fonctions natives (builtins.round)

## 🚀 Synthèse Décisionnelle : Qu'est-ce qui dicte les ventes sur Steam ?
Based on the official metrics and global analysis of the 55,000 games on Steam, several critical drivers determine a video game's visibility, market fit, and financial outcome:

Windows Hegemony: Windows remains the absolute, non-negotiable standard for PC gaming, maintaining a near-monopoly across the storefront. While multi-platform support (Mac/Linux) is a common accessibility trend for lighter categories like Casual and Indie, resource-intensive AAA titles can safely bypass alternative OS porting to minimize development and optimization costs.

The Revenue Focus & Pricing Strategy: Although independent titles structurally saturate Steam's volume, financial resilience is driven by specific pricing power. Simulation and Strategy genres hold the highest average prices on the market, showing that highly-engaged player bases are willing to pay a premium. Meanwhile, the general catalog is saturated with micro-products below $4.99, meaning premium titles must heavily showcase their production value to justify higher price tiers.

Post-COVID Market Saturation & Timing: Publication volume peaked dramatically in 2021, leading to extreme competition for visibility. To break through this noise, initial algorithmic momentum is vital. As proven by the presence of titles like Portal 2 or Left 4 Dead 2 in the Hall of Fame, long-term player retention and high review scores are the strongest organic growth drivers on Steam.

Global Accessibility & Localization: Reaching commercial viability requires expanding beyond historical Western territories. While English is foundational, Simplified Chinese, Russian, and Brazilian Portuguese represent the largest linguistic player pools. Full localization into these high-growth regions is a mandatory strategic requirement for any upcoming launch.

Promotion & Longevity: Discount cultures are heavily embedded within Steam's buyer behavior. Running continuous and strategic seasonal promotions is not an exception but an industry standard required to convert wishlists into concrete revenue and sustain a game's lifecycle.

## 📂 Structure du Projet
📦 projet-steam-bigdata
 ┣ 📂 notebooks
 ┃ ┣ 📜 Steam_part1_loading_cleaning_AMK.ipynb
 ┃ ┗ 📜 Steam_part2_EDA_viz_AMK.ipynb
 ┣ 📂 outputs
 ┃ ┗ 📂 viz
 ┃   ┣ 📜 visualization_Q3.png
 ┃   ┣ 📜 visualization_Q4.png
 ┃   ┣ 📜 visualization_Q5.png
 ┃   ┣ 📜 visualization_Q7.png
 ┃   ┣ 📜 visualization_Q8.png
 ┃   ┣ 📜 visualization_Q10.png
 ┃   ┣ 📜 visualization_Q11.png
 ┃   ┗ 📜 visualization_Q12.png
 ┗ 📜 README.md

## Auteur
Projet réalisé dans le cadre du Bloc 2 — Big Data à Jedha Bootcamp.
By Alicia MARZOUK
