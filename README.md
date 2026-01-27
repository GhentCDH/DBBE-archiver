# Database of Byzantine Book Epigrams (DBBE) - Archiver

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
- Make sure the DBBE services are running (https://github.com/GhentCDH/dbbe)

## Config
Use the .env file to configure the paths to the current Postgres and Elastic servers, and provide a key and URL for Zenodo uploads. The default configured in this repository uses the Zenodo sandbox URL, which should be replaced on production.

## Running locally
- Clone the repository and ```cd``` to the repository root folder
- Generate a new virtual environment (>=3.11): ```python3.11 -m venv .venv``` and ```source .venv/bin/activate```
- ```pip install .```
- ```python -m app.run_migration```

Resulting SQLite files are written to app/data and published as draft to a new Zenodo deposit. 
Configure Zenodo uploads by setting these variables in your `.env` file for other behaviour:

- **`ENABLE_ZENODO_UPLOAD`** : Master switch for Zenodo upload functionality  (`"false"`,  `"true"`)
- **`PUBLISH_DRAFT`** : Controls whether to publish the (new version of the) deposition or leave as draft (`"false"`,  `"true"`)
- **`DEPOSITION_ID`**: ID of existing Zenodo deposition (for creating new versions)
    - `None`: Creates a brand new deposition
    - `<existing_id>`: Creates a new version of that deposition, deletes old `export_data.sqlite`, updates metadata, and uploads new file

## Running from Docker
- ```docker build dbbe-archive .```
```
docker run
--network host
--env-file .env
-v "$(pwd)/data:/app/data"  
dbbe-archive
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

These indices provide the foundational data, which are complemented with data from Postgres. 
From these, we construct the relational database in SQLite, building supporting tables to handle relationships, metadata, and controlled vocabularies.

For a full visual of the database schema, please visit 
<a href="https://www.yworks.com/yed-live/?file=https://gist.githubusercontent.com/PaulienLem/03b10297a226b54e4b09f34364cefb2e/raw/432961c90bd5cf0e3768773093e662499855a574/Imported%20Document">yEd live</a>.
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
- ```occurrence_related_occurrences``` and ```occurrence_relation_definitions```: An occurrence can be related to other occurrences if they (a) some of their verses share verse groups or (b) they share types. The relationship type is defined in occurrence_relation_definitions
- ```occurrence_keyword```: Keywords telling what the occurrence is about

#### **2. Types**

These are prototypes of occurrences. A lot of occurrences have a high level of similarity. DBBE proposes prototypes for every group of similar occurrences.

Related tables:

- ```Type_person_roles```: Any possible role a person could play in the construction and publication of this Type. 
- ```Type_genre```: Genres attributed to this Type. More than 1 Genre can be attributed, and this is not necessarily an accumulation of the Genres of the linked Occurrences.
- ```Type_metre```: Metres attributed to this Type. More than 1 Metre can be attributed, and this is not necessarily an accumulation of the Genres of the linked Occurrences.
- ```Type_management```: Internal information. For example: To do's in the processing of this Type
- ```Type_acknowledgement```: Plain text shout out to people who helped in the publication of this Type. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', and add this to type_person_roles._
- ```Type_text_statuses```: Type text can be either completely known or partially unknown
- ```Type_related_types (linked via type_relation_definitions)```: Groups of similar types
- ```Type_tags```:  They seem to explain the function of the Type (ex: introducing a subject, making a comment on the content,...). 
- ```Type_occurrences```: Occurrences linked to this type
- ```Type_editorial_status```: editorial states for types. Currently only ```(not) a critical text```. This might become just a boolean value but since it's not sure yet, we stored it like this
- ```type_keyword```: Keywords telling what the type is about



#### **3. Manuscripts**

Contains metadata about manuscripts.

Related tables:

- ```Manuscript_acknowledgement```: Plain text shout out to people who helped in the publication of this Acknowledgement. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', and add this to manuscript_person_roles._
- ```Manuscript_content```: Explains what the manuscript is about. Careful: This is a hierarchical table. For example, a manuscript can be about Biblica -> Novum Testamentum. In this table, the lowest leaf (Novum Testamentum) is stored. The parent_id column of the content table can be used to trace the full content. 
- ```Manuscript_identification```: Links a manuscript to one or more IDs that were used in canonical works to refer to this manuscript (ex: Diktyon)
- ```Manuscript_management```: Internal information. For example: To do's in the processing of this manuscript
- ```Manuscript_location```: The location where the manuscript was written. Careful: Location is a hierarchical table. If a manuscript was written in Brussels, it is linked to Brussels, but via the parent_id column of the locations table, you could also see that Brussels is in Belgium. In the original Elasticsearch, this was named "Origins" instead of "Locations"
- ```Manuscript_person_roles```: Any possible role a person could play in the publication of this manuscript. Example: Patron ( = historical person), Illuminator  (=historical person), contributor (=modern person)...



#### **4. Persons**

Contains metadata about persons involved (authors, editors, patrons, etc.).

Related tables:

- ```Person_acknowledgement```: Plain text shout out to people who helped in the publication of the information on this (historical) person. _This was stored as plain text in the original DBBE. Maybe in time we could have a role 'Acknowledged', although that would mean we'd need a person_person_roles table which would be confusing._
- ```person_identification```: Used to link persons to canonical IDs set by different authorities.
- ```person_management```: Internal information. For example: To do's in the processing of this person
- ```person_self_designations```: How a scribe describes himself
- ```person_offices```: the official title of a person. **To do**:These are currently stored entirely separate from ```self designation```, even tho a person could describe himself using his official title too...


#### **5. Bibliographies**

Bibliographies are modelled as concrete entity types, rather than a single table as in the original setup.

- ```article```
- ```book```
- ```book_chapter```
- ```blog_post```
- ```bib_varia```
- ```online_source```
- ```phd```

Each bibliographic entity has:
- its own table
- a corresponding *_person_roles table
- a corresponding *_managements table
- linking tables to the item the bibliography is about:
  - manuscripts (manuscript_*)
  - occurrences (occurrence_*)
  - persons (persons_*)
  - types (types_*)

Additional structures:
- journal and journal_issue: Articles may be linked to journal issues via article.journal_issue_id

Note that, for now, some of these bibliography tables were added for completeness sake: not every concept (Manuscript / Occurrence / Person / Type) has all types of bibliographies linked to it (online sources, PhDs, etc.)

#### **6. Verses**

Contains verse-level data for occurrences.

Columns include ```id, occurrence_id, manuscript_id, text, order_in_occurrence, verse_group_id.```

Verse_groups allow grouping of related verses.

### **Lookup / Metadata Tables**

- ```roles``` — defines role types for persons.
- ```text_statuses``` — textual status of occurrences or types.
- ```keywords``` — keywords for occurrences and types.
- ```tags``` — tags for types: They seem to explain the function of the Type (ex: introducing a subject, making a comment on the content,...). From dbbe.ugent.be: More refined than "subject" and rather referring to recurring motifs, such as . Meant to enable specific thematic searches.
- ```metres``` — metre classification.
- ```genres``` — genre classification.
- ```management``` — administrative metadata.
- ```acknowledgements``` — acknowledgements linked to occurrences, manuscripts, types, or persons.
- ```editorial_statuses``` — editorial states for types. Currently only ```(not) a critical text```. This might become just a boolean value but since it's not sure yet, we stored it like this
- ```self_designations``` — how a scribe describes himself
- ```offices``` — the official title of a person. **To do**:These are currently stored entirely separate from ```self designation```, even tho a person could describe himself using his official title too...
- ```locations``` - Locations that could be linked to manuscripts, libraries, persons,... . This is based upon the postgrs 'region' table. Note that a region used to have a flag is_city in the Postgres' Region table. I want to avoid keeping this approach so for now I did not add it. We might want to consider making this cleaner. 
- ```libraries``` — library name and location. Note that a manuscript name is always ```City - library - collection - shelf``` (to be verified with dbbe)
- ```collections``` — collection metadata. Note that a manuscript name is always ```City - library - collection - shelf``` (to be verified with dbbe)
- ```biblio_category``` — categories for bibliographies.
- ```content``` -  Used for storing manuscript content. Careful: This is a hierarchical table. For example, a manuscript can be about Biblica -> Novum Testamentum. In this table, the lowest leaf (Novum Testamentum) is stored. The parent_id column of the content table can be used to trace the full content. 
- ```identifications``` -canonical ways to refer to persons or manuscripts

----

## Zenodo

This repository uses the Zenodo API for the automatic publication of datasets. Full API docs can be found on <a href="https://developers.zenodo.org/#rest-api">https://developers.zenodo.org/#rest-api</a>. You can do all of these calls to <a href="https://sandbox.zenodo.org/">https://sandbox.zenodo.org/</a> as well. This environment functions in the exact same way as production so it's perfect for testing.  

In order to use the API, you need an access token, which you can generate by creating a Zenodo account and going to "My Account" > "Applications".