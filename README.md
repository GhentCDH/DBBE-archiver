# DBBE Archiver

This repository was developed to facilitate the periodic archival of data from the live DBBE instance to Zenodo in SQLite format. 
It integrates data originating from Elasticsearch with complementary information stored in PostgreSQL, producing a comprehensive and internally consistent SQLite database.

The resulting dataset is designed to serve multiple objectives:
- Long-term preservation: ensuring the durability and continued accessibility of DBBE data beyond the lifespan of the current production infrastructure.
- Research accessibility: providing linguists, philologists, and computational researchers with a well-structured dataset that can be easily transformed, queried, and adapted to diverse analytical workflows.
- Software sustainability: offering a stable foundation for the development of new tools and applications

By consolidating and normalizing data across heterogeneous storage systems, this project aims to future-proof the DBBE corpus while lowering the technical barrier for reuse, analysis, and further digital scholarship.

The repository contains the initial tests stored in a notebook, and the production-ready version of this code in the ```/app``` folder.

## Config
Use the .env file to configure the paths to the current Postgres and Elastic servers, and provide a key and URL for Zenodo uploads. The default configured in this repository uses the Zenodo sandbox URL, which should be replaced on production.

## Prerequisites
- Launch a virtual environment in Python3.11
- Make sure the DBBE services are running (https://github.com/GhentCDH/dbbe)

## Running locally
- Clone the repository
- ```cd app```
- ```pip install .```
- ```python run_migration.py ```
- Resulting SQLite files are written to app/data

## Running from Docker
- ```docker build es-migration .```
```
docker run
--network host
--env-file .env
-v "$(pwd)/data:/app/data"  
es-migration
```
- Resulting SQLite files are written to app/data
- Modify the .env file for running on machines other than localhost

----

## Database schema
This database is built from six primary Elasticsearch indices:

- verses
- occurrences
- types
- manuscripts
- persons
- bibliographies

These indices provide the foundational data. From these, we construct the relational database in SQLite, building supporting tables to handle relationships, metadata, and controlled vocabularies.

The database uses a normalized schema with many-to-many, many-to-one, and lookup tables to ensure data consistency and relational integrity.

### Core Tables

#### **1. Occurrences**

Stores individual occurrences (= short epigrams or poems, literally how they've been found in a manuscript)

Columns include:
```id, created, modified, public_comment, private_comment, incipit, text_stemmer, text_original, location_in_ms, date_floor_year, date_ceiling_year, palaeographical_info, contextual_info, manuscript_id, title, is_dbbe.```

Related tables:

- ```occurrence_person_roles```: Any possible role a person could play in the publication of this occurrence. Example: Scribe ( = historical person), transcriber (=modern person), contributor (=modern person)...
- ```occurrence_genres```: Genres attributed to this occurrence (Can be more than 1)
- ```occurrence_metres```: Metres attributed to this occurrence (Can be more than 1)
- ```occurrence_management```: Internal information. For example: To do's in the processing of this occurrence
- ```occurrence_acknowledgement```: Plain text shout out to people who helped in the publication of this occurrence. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', and add this to occurrence_person_roles._
- ```occurrence_text_statuses```
- ```occurrence_subject```
- ```occurrence_related_occurrences``` (linked via occurrence_relation_definitions)


#### **2. Types**

These are prototypes of occurrences. A lot of occurrences have a high level of similarity. DBBE proposes prototypes for every group of similar occurrences.

Related tables:

- ```Type_person_roles```
- ```Type_genre```
- ```Type_metre```
- ```Type_management```
- ```Type_acknowledgement```
- ```Type_text_statuses```
- ```Type_subject```
- ```Type_related_types (linked via type_relation_definitions)```
- ```Type_tags```
- ```Type_occurrences```
- ```Type_editorial_status```


#### **3. Manuscripts**

Contains metadata about manuscripts.

Related tables:

- ```Manuscript_acknowledgement```
- ```Manuscript_content```
- ```Manuscript_identification```
- ```Manuscript_management```
- ```Manuscript_origin```
- ```Manuscript_person_roles```


#### **4. Persons**

Contains metadata about persons involved (authors, editors, patrons, etc.).

Related tables:

- ```person_acknowledgement```
- ```person_identification```
- ```person_management```
- ```person_offices```
- ```person_self_designations```
- ```occurrence_person_roles```
- ```manuscript_person_roles```
- ```bibliography_person_roles```
- ```type_person_roles```


#### **5. Bibliographies**

Contains bibliographic entries.

Related tables:

- ```bibliography_management```
- ```bibliography_person_roles```

#### **6. Verses**

Contains verse-level data for occurrences and manuscripts.

Columns include ```id, occurrence_id, manuscript_id, text, order_in_occurrence, verse_group_id.```
verse_groups table allows grouping of related verses.

#### **Lookup / Metadata Tables**

- ```roles``` — defines role types for persons.
- ```text_statuses``` — textual status of occurrences or types.
- ```subjects``` — subjects for occurrences and types.
- ```tags``` — tags for types.
- ```metres``` — metre classification.
- ```genres``` — genre classification.
- ```management``` — administrative metadata.
- ```acknowledgements``` — acknowledgements linked to occurrences, manuscripts, types, or persons.
- ```editorial_statuses``` — editorial states for types.
- ```self_designations``` — self-descriptions for persons.
- ```offices``` — offices held by persons.
- ```origins``` — geographical or institutional origins for manuscripts.
- ```cities``` — city metadata.
- ```libraries``` — library metadata.
- ```collections``` — collection metadata.
- ```biblio_category``` — categories for bibliographies.
