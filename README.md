# MemoryDB - Base de données en mémoire optimisée pour les fichiers Parquet

MemoryDB est une application Spring Boot qui permet de charger des fichiers Parquet dans une base de données en mémoire optimisée pour les requêtes analytiques.

## Fonctionnalités

- **Chargement de fichiers Parquet** : Chargement de fichiers Parquet en mémoire avec optimisation du stockage
- **Limitation du chargement** : Possibilité de limiter le nombre de lignes à charger pour les grands fichiers
- **Inspection de fichiers Parquet** : Analyse de la structure d'un fichier Parquet avant chargement
- **Comptage des lignes** : Estimation ou comptage précis du nombre de lignes dans un fichier Parquet
- **Requêtes sur les données** : Exécution de requêtes sur les données en mémoire
- **Export CSV** : Export des données au format CSV
- **Pagination** : Affichage des données avec pagination

## API REST

### Gestion des fichiers Parquet

- **Inspecter un fichier Parquet** :  
  `GET /api/tables/inspect-parquet?filePath=/chemin/vers/fichier.parquet`

- **Compter le nombre de lignes dans un fichier Parquet** :  
  `GET /api/tables/count-rows?filePath=/chemin/vers/fichier.parquet&useFastCount=true`  
  Le paramètre `useFastCount` (par défaut à `true`) permet de choisir entre :
  - Une estimation rapide basée sur les métadonnées (`true`)
  - Un comptage précis mais plus lent (`false`)

- **Charger un fichier Parquet** :  
  `POST /api/tables/load-parquet?filePath=/chemin/vers/fichier.parquet&maxRows=1000000`  
  Le paramètre optionnel `maxRows` permet de limiter le nombre de lignes à charger.

### Gestion des tables

- **Lister les tables** :  
  `GET /api/tables/list`

- **Obtenir les informations d'une table** :  
  `GET /api/tables/{tableName}`

- **Obtenir un échantillon des données** :  
  `GET /api/tables/{tableName}/sample?limit=10`

- **Obtenir les données avec pagination** :  
  `GET /api/tables/{tableName}/data?limit=100&offset=0`

- **Exporter les données au format CSV** :  
  `GET /api/tables/{tableName}/export-csv?limit=1000`

- **Supprimer une table** :  
  `DELETE /api/tables/{tableName}`

## Architecture

L'application est structurée en plusieurs composants :

1. **Controllers** : Points d'entrée REST API
2. **Services** : Logique métier et traitement des données
3. **Storage** : Structures de données optimisées pour le stockage en mémoire
4. **Query** : Moteur de requêtes pour interroger les données

## Optimisations

- **Stockage colonnaire** : Les données sont stockées par colonnes pour optimiser les requêtes analytiques
- **Traitement parallèle** : Chargement et traitement des données en parallèle
- **Indexation** : Possibilité d'indexer certaines colonnes pour accélérer les requêtes
- **Chunking** : Les données sont stockées par chunks pour optimiser la mémoire
- **Chargement partiel** : Possibilité de ne charger qu'une partie d'un fichier très volumineux

## Exemple d'utilisation

1. Vérifier le nombre de lignes dans un fichier Parquet :
   ```bash
   # Estimation rapide
   curl -X GET "http://localhost:8080/api/tables/count-rows?filePath=/chemin/vers/fichier.parquet"
   
   # Comptage précis (plus lent)
   curl -X GET "http://localhost:8080/api/tables/count-rows?filePath=/chemin/vers/fichier.parquet&useFastCount=false"
   ```

2. Inspecter un fichier Parquet :
   ```bash
   curl -X GET "http://localhost:8080/api/tables/inspect-parquet?filePath=/chemin/vers/fichier.parquet"
   ```

3. Charger un fichier Parquet (limité à 1 million de lignes) :
   ```bash
   curl -X POST "http://localhost:8080/api/tables/load-parquet?filePath=/chemin/vers/fichier.parquet&maxRows=1000000"
   ```

4. Charger un fichier Parquet complet :
   ```bash
   curl -X POST "http://localhost:8080/api/tables/load-parquet?filePath=/chemin/vers/fichier.parquet"
   ```

5. Afficher les données :
   ```bash
   curl -X GET "http://localhost:8080/api/tables/matable/data?limit=10"
   ```

6. Exporter en CSV :
   ```bash
   curl -X GET "http://localhost:8080/api/tables/matable/export-csv" > export.csv
   ```

## Prérequis

- Java 17 ou supérieur
- Maven 3.6 ou supérieur

## Compilation et exécution

```bash
mvn clean package
java -jar target/MemoryDB.jar
```

![[RAQYD.png]](RAQYD.png)