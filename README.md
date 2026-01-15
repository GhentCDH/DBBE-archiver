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
This SQLite database is built from six primary Elasticsearch indices:

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

Stores individual occurrences (= short epigrams or poems, literally how they've been found in a manuscript, so including marks for gaps and missing text.)

Columns include:
```id, created, modified, public_comment, private_comment, incipit, text_stemmer, text_original, location_in_ms, date_floor_year, date_ceiling_year, palaeographical_info, contextual_info, manuscript_id, title, is_dbbe.```

Related tables:

- ```occurrence_person_roles```: Any possible role a person could play in the publication of this occurrence. Example: Scribe ( = historical person), transcriber (=modern person), contributor (=modern person)...
- ```occurrence_genres```: Genres attributed to this occurrence (Can be more than 1)
- ```occurrence_metres```: Metres attributed to this occurrence (Can be more than 1)
- ```occurrence_management```: Internal information. For example: To do's in the processing of this occurrence
- ```occurrence_acknowledgement```: Plain text shout out to people who helped in the publication of this occurrence. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', and add this to occurrence_person_roles._
- ```occurrence_text_statuses```: An occurrence text can be partially/completely (un)known
- ```occurrence_subject```: **This one is tricky:** Persons can have a Role "Subject" in an Occurrence, implying they are mentioned in that occurrence. However, an Occurrence also has a "Subject" , 
which could (but not has to be) be a person (in which case both the Subjects table and the Persons table refer to the same person). **Homer** for example appears both in the Subjects and in the Persons table. He has the same ID in both but they are not linked. To be analyzed (and discussed).
- ```occurrence_related_occurrences``` and ```occurrence_relation_definitions```: An occurrence can be related to other occurrences if they (a) some of their verses share verse groups or (b) they share types. The relationship type is defined in occurrence_relation_definitions


#### **2. Types**

These are prototypes of occurrences. A lot of occurrences have a high level of similarity. DBBE proposes prototypes for every group of similar occurrences.

Related tables:

- ```Type_person_roles```: Any possible role a person could play in the construction and publication of this Type. 
- ```Type_genre```: Genres attributed to this Type. More than 1 Genre can be attributed, and this is not necessarily an accumulation of the Genres of the linked Occurrences.
- ```Type_metre```: Metres attributed to this Type. More than 1 Metre can be attributed, and this is not necessarily an accumulation of the Genres of the linked Occurrences.
- ```Type_management```: Internal information. For example: To do's in the processing of this Type
- ```Type_acknowledgement```: Plain text shout out to people who helped in the publication of this Type. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', and add this to type_person_roles._
- ```Type_text_statuses```
- ```Type_subject```
- ```Type_related_types (linked via type_relation_definitions)```
- ```Type_tags```
- ```Type_occurrences```: Occurrences linked to this type
- ```Type_editorial_status```: editorial states for types. Currently only ```(not) a critical text```. This might become just a boolean value but since it's not sure yet, we stored it like this



#### **3. Manuscripts**

Contains metadata about manuscripts.

Related tables:

- ```Manuscript_acknowledgement```: Plain text shout out to people who helped in the publication of this Acknowledgement. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', and add this to manuscript_person_roles._
- ```Manuscript_content```
- ```Manuscript_identification```
- ```Manuscript_management```: Internal information. For example: To do's in the processing of this manuscript
- ```Manuscript_origin```
- ```Manuscript_person_roles```: Any possible role a person could play in the publication of this manuscript. Example: Patron ( = historical person), Illuminator  (=historical person), contributor (=modern person)...



#### **4. Persons**

Contains metadata about persons involved (authors, editors, patrons, etc.).

Related tables:

- ```Person_acknowledgement```: Plain text shout out to people who helped in the publication of the information on this (historical) person. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', although that would mean we'd need a person_person_roles table which would be confusing._
- ```person_identification```
- ```person_management```: Internal information. For example: To do's in the processing of this person
- ```person_self_designations```: How a scribe describes himself
- ```person_offices```: the official title of a person. **To do**:These are currently stored entirely separate from ```self designation```, even tho a person could describe himself using his official title too...


#### **5. Bibliographies**

Contains bibliographic entries. **Important to do**: Use the postgres to link bibliographical information to the entities they are linked to. 

Related tables:

- ```bibliography_management```: Internal information. For example: To do's in the processing of this bibliography
- ```bibliography_person_roles```

#### **6. Verses**

Contains verse-level data for occurrences and manuscripts.

Columns include ```id, occurrence_id, manuscript_id, text, order_in_occurrence, verse_group_id.```

Verse_groups table allows grouping of related verses. For now, we made a separate table for it because it seems like something that could receive additional metadata in a later phase (descriptions about this verse group?). However, currently verse groups have nothing else than IDs.

### **Lookup / Metadata Tables**

- ```roles``` — defines role types for persons.
- ```text_statuses``` — textual status of occurrences or types.
- ```subjects``` — subjects for occurrences and types.
- ```tags``` — tags for types: They seem to explain the function of the Type (ex: introducing a subject, making a comment on the content,...). From dbbe.ugent.be: More refined than "subject" and rather referring to recurring motifs, such as . Meant to enable specific thematic searches.
- ```metres``` — metre classification.
- ```genres``` — genre classification.
- ```management``` — administrative metadata.
- ```acknowledgements``` — acknowledgements linked to occurrences, manuscripts, types, or persons.
- ```editorial_statuses``` — editorial states for types. Currently only ```(not) a critical text```. This might become just a boolean value but since it's not sure yet, we stored it like this
- ```self_designations``` — how a scribe describes himself
- ```offices``` — the official title of a person. **To do**:These are currently stored entirely separate from ```self designation```, even tho a person could describe himself using his official title too...
- ```origins``` — geographical or institutional origins for manuscripts. **To do:** because of the elasticsearch, these are flattened. For example, an origin name could be ```Asia Minor < Turkey```, but also ```Turkey```. We might want to add some hierarchy here.
- ```cities``` — city metadata. **To do**: This seems conceptually related to origins. We should probably revise how geographical info is stored.
- ```libraries``` — library metadata. Note that a manuscript name is always ```City - library - collection - shelf``` (to be verified with dbbe)
- ```collections``` — collection metadata. Note that a manuscript name is always ```City - library - collection - shelf``` (to be verified with dbbe)
- ```biblio_category``` — categories for bibliographies.
- ```content``` - the content of a manuscript.  **To do**: This is currently flattened (ex. Biblica > Novum Testamentum > Evangeliarium), we might want to split this up and add a hierarchy.
