# 🎮 Projet Big Data — Analyse Globale du Marché Steam (Ubisoft)

## 🎯 Objectif du Projet
L'objectif de ce projet est d'exploiter les capacités de traitement de PySpark pour analyser le catalogue de la plateforme Steam. En adoptant différents niveaux de granularité, cette étude cartographie l'écosystème PC afin d'identifier les facteurs clés qui influencent directement la popularité, la satisfaction des joueurs et la rentabilité financière des jeux vidéo pour guider les futurs lancements d'Ubisoft.

---

## 💾 Source des Données
Les analyses s'appuient sur un jeu de données semi-structuré (JSON) regroupant l'ensemble des métadonnées des jeux :
* **Lien du Dataset S3 :** `s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json`

---

## 📬 Liens des Notebooks Databricks Publics

Pour contourner les contraintes techniques liées à la taille limite de publication de Databricks, le projet a été scindé en deux volets autonomes et complémentaires :

* **[🔗 Partie 1 : Chargement, Nettoyage & Analyse Macro] lien databriks:(https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/1871887574472583?o=7474652616624392)**
  * *Périmètre :* Connexion au stockage Amazon S3, extraction et aplatissement dynamique (*flattening*) des structures imbriquées du schéma JSON (gestion des tags), typage strict des données et analyses macro-économiques.
* **[🔗 Partie 2 : Analyses Sectorielles — Genres & Plateformes] lien databriks: (https://dbc-9df0b3e7-71bd.cloud.databricks.com/editor/notebooks/4473242428596805?o=7474652616624392)**
  * *Périmètre :* Explosion des chaînes de caractères de données composites (`F.explode`), calcul des taux de satisfaction réels, fonctions de fenêtrage avancées (`Window.partitionBy`) pour analyser les stratégies éditeurs, modélisation des revenus et matrices de compatibilité technique.

---

## 📊 Tableau Récapitulatif : Questions & Réponses Métier

| Question de l'énoncé | Approche technique (PySpark) | Ce que révèlent nos données brutes | Impact Stratégique (Ubisoft) |
| :--- | :--- | :--- | :--- |
| **Q1: Most prolific publisher?** | `groupBy("publisher")` + tri décroissant. | Identification des éditeurs leaders en volume de jeux sortis. | Cartographie fine de la présence de la concurrence sur la boutique. |
| **Q2: Best rated games?** | Ratio d'avis positifs pondéré avec un filtre de confiance `> 50` avis. | Les jeux de premier plan maintiennent des scores d'approbation très élevés. | La réputation algorithmique est le premier vecteur de découvrabilité. |
| **Q3: Impact of Covid-19?** | Extraction de l'année par regex + agrégation temporelle. | **Pic massif de sorties en 2020 (9 501 jeux) et 2021 (11 391 jeux)** contre environ 7 000 en 2018. | Le marché a subi une forte accélération de l'offre suite aux confinements. |
| **Q4: Price distribution & discounts?** | Segmentation par tranches de prix et analyse du champ `discount`. | **Une écrasante majorité de jeux sont à moins de 5$ ou Free-to-Play.** La présence de promotions est un standard. | Le catalogue Steam est saturé par des micro-produits à bas coût. |
| **Q5: Most represented languages?** | `groupBy("languages")` + décompte. | **L'anglais domine massivement le marché (plus de 41 000 jeux)**, suivi de loin par le chinois et le russe. | La localisation linguistique complète (Anglais/Chinois) est obligatoire pour performer. |
| **Q6: Prohibited for children (16/18)?** | Filtrage et extraction numérique sur `required_age`. | Les jeux avec restrictions strictes d'âge représentent une part minoritaire mais stable du catalogue. | Le marché adulte (16+/18+) reste viable pour les productions AAA complexes. |
| **Q7: Most represented genres?** | `F.explode(F.split(genre))` pour isoler les genres unitaires. | **Les catégories Indie (37 671), Action (19 862) et Adventure (17 072)** saturent l'offre de titres. | Les genres phares d'Ubisoft font face à la plus forte concurrence en volume. |
| **Q8: Better review ratio by genre?** | Moyenne de la note calculée groupée par genre unitaire. | Les genres de niche ciblés (**RPG, Simulation, Stratégie**) maintiennent des scores de satisfaction élevés et stables. | Les communautés ciblées sont plus fidèles si la promesse de gameplay est tenue. |
| **Q9: Publishers favorite genres?** | Fenêtrage analytique `Window` + filtrage sur le Rang 1. | Les grands éditeurs n'éparpillent pas leurs efforts : ils se spécialisent sur un genre maître. | Permet de détecter la couleur éditoriale et la spécialisation de chaque concurrent. |
| **Q10: Most lucrative genres?** | Calcul du chiffre d'affaires estimé : `(positive + negative) * price`. | **L'Action et l'Aventure écrasent financièrement le marché** (plus de 170M$ de CA estimé combiné). | Malgré le volume des jeux indépendants, les gros budgets d'Action captent l'essentiel de la valeur. |
| **Q11: Platform availability?** | Somme des variables binaires converties divisée par le total. | **Windows détient un monopole absolu (99,9%)**. Mac (11,4%) et Linux (7,4%) sont très marginaux. | Windows est le passage obligé. Ignorer Mac/Linux n'impacte pas la viabilité commerciale. |
| **Q12: Preferential OS availability?** | Taux de compatibilité des OS croisés par genre unitaire. | Les genres légers (*Casual/Indie*) s'exportent plus sur Mac/Linux. **Les jeux d'Action AAA restent exclusifs à Windows.** | Les investissements de portage sur les moteurs lourds propriétaires doivent viser Windows en priorité. |

---

## 🚀 Synthèse Décisionnelle : Les Facteurs de Ventes sur Steam

L'analyse de nos données met en évidence trois piliers fondamentaux pour maximiser les performances commerciales :

1. **La concentration de la valeur financière (Action / RPG) :** Bien que le catalogue de Steam soit saturé en volume par des milliers de productions indépendantes (`Indie`), la captation réelle de valeur financière se concentre sur les genres **Action** et **RPG**. Cela valide scientifiquement la stratégie d'Ubisoft de continuer à investir sur des productions Premium de grande envergure (AAA).
2. **La rationalisation technique de la R&D :** Windows centralise la quasi-totalité de l'écosystème. Les taux de compatibilité macOS et Linux sur les genres exigeants (Action/Aventure) étant extrêmement faibles, le coût de développement et d'optimisation d'un moteur lourd sur ces plateformes n'offre aucun retour sur investissement. L'effort doit rester focalisé sur l'environnement Windows.
3. **Le contrôle impératif de la réputation au Jour J :** Les genres grand public subissent une forte volatilité des avis. Comme le démontre l'analyse des scores, les barrières algorithmiques de Steam détruisent la visibilité des jeux subissant un lancement technique manqué. Une exécution et une phase de QA irréprochables au lancement sont indispensables pour préserver la dynamique des ventes.
