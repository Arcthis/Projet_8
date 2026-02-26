# Projet 8 - Pipeline de collecte et de stockage de données météorologiques

## Contexte et objectifs

Ce projet a été réalise en tant que Data Engineer pourl’entreprise GreenCorp. Le but est de mettre à disposition un environnement avec des données traitées et fiables.
Ces données sont des données météorologiques qui ont pour but de fournir des informations manquantes pourla société.
Voici les différentes étapes nécessaires pour traiter ce projet :

- Récupération des sources de données et intégrations de celles-ci dans un bucket S3 AWS avec l’aide d’AirByte
- Vérification, traitement et intégration des données dans une base de données MongoDB avec Docker
- Reproduction de l’environnement local dans une machine virtuelle EC2 associée à un cluster DocumentDB avec replica
- Monitoring et reporting de l’environnement final AWS

Techniquement parlant, voici les phases :

1. **Extraction des données** via l'outil Airbyte, qui se charge de connecter les sources et d'exporter les résultats au format JSONL.
2. **Transformation et harmonisation** des données à l'aide de scripts Python : unification des formats de colonnes, conversion des unités (Fahrenheit en Celsius, miles/heure en km/h, etc.) et nettoyage des valeurs aberrantes ou manquantes.
3. **Contrôle qualité** via un script dédié qui vérifie la cohérence des données (types, valeurs nulles, coordonnées géographiques, etc.).
4. **Chargement** du jeu de données unifié dans une base de données MongoDB.
5. **Déploiement conteneurisé** de l'ensemble de la stack grâce à Docker, permettant une reproductibilité totale de l'environnement.
6. **Déploiement sur AWS** : mise en place d'une infrastructure AWS avec un cluster DocumentDB, une machine virtuelle EC2 et un bucket S3.
7. **Monitoring et reporting** : mise en place d'outils de monitoring et de reporting pour suivre l'état de l'infrastructure et des données.

---

## Technologies utilisées

- **Airbyte** : orchestration de l'extraction des données depuis les APIs météorologiques.
- **Python** (avec pandas, pymongo, polars) : transformation, harmonisation et chargement des données.
- **MongoDB** : base de données NoSQL utilisée pour stocker les relevés météorologiques et les informations de stations.
- **Docker / Docker Compose** : conteneurisation de la base de données et du pipeline de traitement pour garantir un environnement reproductible.
- **JSONL** : format d'échange intermédiaire entre l'extraction et la transformation.
- **AWS** : infrastructure cloud avec un cluster DocumentDB, une machine virtuelle EC2 et un bucket S3.

---

## Installation et utilisation du projet

### Prérequis

- [Docker](https://www.docker.com/) et Docker Compose installés sur la machine.
- Python (si exécution locale des scripts hors Docker).

### Étapes

1. **Cloner le dépôt** :

   ```bash
   git clone <url-du-dépôt>
   cd Projet_8_Github
   ```

2. **Lancer la stack Docker** :

   Cette commande démarre le conteneur MongoDB et le conteneur Python qui s'occupent du chargement et de la restauration des données :

   ```bash
   docker-compose up --build
   ```

   Le conteneur Python attend que MongoDB soit prêt, puis restaure automatiquement les données via `mongorestore` à partir d'un dump préexistant situé dans le dossier `backup/`.

3. **Transformation des données (optionnel)** :

   Si les fichiers JSONL sources sont disponibles dans le dossier `json/`, le script de transformation peut être exécuté directement :

   ```bash
   python scripts/data_transform.py
   ```

   Cela produit un fichier `merged_weather_data.json` unifié dans le dossier `json/`.

4. **Import dans MongoDB (optionnel)** :

   Pour importer les données transformées dans la base :

   ```bash
   python scripts/data_import.py
   ```

5. **Contrôle qualité des données (optionnel)** :

   Pour vérifier l'intégrité des fichiers JSONL sources :

   ```bash
   python scripts/data_check.py json/belgique.jsonl json/france.jsonl json/info_climat.jsonl
   ```

6. **Déploiement sur AWS** :

   ```bash
   # Création du cluster DocumentDB
   aws docdb create-db-cluster --db-cluster-identifier projet-8-cluster --engine docdb --engine-version 4.0.0 --master-username admin --master-user-password <password>
   
   # Création de la machine virtuelle EC2
   aws ec2 run-instances --image-id ami-0c55b159cbfafe1f0 --instance-type t2.micro --key-name projet-8-key --security-group-ids sg-0123456789abcdef0 --subnet-id subnet-0123456789abcdef0
   
   # Création du bucket S3
   aws s3 mb s3://projet-8-bucket
   ```

---

## Résultats obtenus

Les données brutes, initialement dans des formats hétérogènes et des unités anglo-saxonnes (°F, mph, in Hg), ont été unifiées dans un format cohérent en unités métriques. Le contrôle qualité confirme l'absence de valeurs hors bornes sur les coordonnées géographiques et la validité du format des horodatages.

Le jeu de données final est prêt à être exploité pour des analyses ou de la visualisation, notamment pour comparer les conditions météorologiques entre les stations belges et françaises de la région transfrontalière.