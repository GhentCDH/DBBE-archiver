# DBBE Backups

This repository was developed to facilitate the periodic archival of data from the live DBBE instance to Zenodo in SQLite format. 
It integrates data originating from Elasticsearch with complementary information stored in PostgreSQL, producing a comprehensive and internally consistent SQLite database.

The resulting dataset is designed to serve multiple objectives:
- Long-term preservation: ensuring the durability and continued accessibility of DBBE data beyond the lifespan of the current production infrastructure.
- Research accessibility: providing linguists, philologists, and computational researchers with a well-structured dataset that can be easily transformed, queried, and adapted to diverse analytical workflows.
- Software sustainability: offering a stable foundation for the development of new tools and applications

By consolidating and normalizing data across heterogeneous storage systems, this project aims to future-proof the DBBE corpus while lowering the technical barrier for reuse, analysis, and further digital scholarship.

The repository contains the initial tests stored in a notebook, and the production-ready version of this code in the ```/app``` folder.

## Prerequisites
- Launch a virtual environment in Python3.11

## Running locally
- Clone the repository
- ```cd app```
- ```pip install .```
- ```python run_migration.py ```
- Resulting SQLite files are written to app/data

## Running from Docker
- ```docker build es-migration .```
- ```
docker run
--network host
--env-file .env
-v "$(pwd)/data:/app/data"  
es-migration
```