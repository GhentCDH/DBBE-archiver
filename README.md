
# Database of Byzantine Book Epigrams (DBBE) - Archiver

This repository was developed to facilitate the periodic archival of data from the live DBBE instance to Zenodo in SQLite format. 
It integrates data originating from Elasticsearch with complementary information stored in PostgreSQL, producing a comprehensive and internally consistent SQLite database.

The resulting dataset is designed to serve multiple objectives:
- **Long-term preservation**: Ensures durability and continued accessibility beyond the lifespan of the current production infrastructure.
- **Research accessibility**: Provides linguists, philologists, and computational researchers with a well-structured dataset ready for analysis and adaptation.
- **Software sustainability**: Offers a stable foundation for building new tools and applications.

By consolidating and normalizing data across heterogeneous storage systems, this project aims to future-proof the DBBE corpus while lowering the technical barrier for reuse, analysis, and further digital scholarship.

--- 

## Prerequisites
- Launch a virtual environment in Python3.11
- Make sure the DBBE services are running ([https://github.com/GhentCDH/dbbe](https://github.com/GhentCDH/dbbe))

--- 
## Configuration
Use the ```.env``` file to configure the paths to the current Postgres and Elastic servers, and provide a key and URL for Zenodo uploads. The default configured in this repository uses the Zenodo sandbox URL, which should be replaced on production.

If you don't want to upload to Zenodo and / or you have no API key for Zenodo, you can set ```ENABLE_ZENODO_UPLOAD``` to ```false```. Other Zenodo related variables will be ignored in that case.

---

## Running locally
1. Clone the repository and ```cd``` to the repository root folder
2. Generate a new virtual environment (>=3.11): 
    ```
    python3.11 -m venv .venv
    source .venv/bin/activate
    ```
3. Install requiired packages: ```pip install .```
4. Run application: ```python -m app.run_migration```

The resulting SQLite files are written to app/data and published to Zenodo if enabled in ``.env``.

---

## Running from Docker

1. ```cd``` to the root of the repository
2. Build the container: ```docker build -t dbbe-archive -f app/Dockerfile .```
3. Run the container
```
docker run
--network host
--env-file app/.env
-v "$(pwd)/data:/app/data"  
dbbe-archive
```

The resulting SQLite files are written to app/data and published to Zenodo if enabled in ``.env``.

Note: if you ran this locally earlier, you might already have a sqlite file in your datafolder. This might conflict if you rerun with Docker, because the script retries inserts that are already there. In other words: make sure app/data is empty. 

----

## Database schema
This SQLite database is built from six primary Elasticsearch indices:

- verses
- occurrence
- types
- manuscripts
- persons
- bibliographies

These indices provide the foundational data, which are complemented with data from Postgres. 
From these, we construct the relational database in SQLite, building supporting tables to handle relationships, metadata, and controlled vocabularies.

For a full visual of the database schema, please visit 
<a href="https://www.yworks.com/yed-live/?file=https://gist.githubusercontent.com/PaulienLem/03b10297a226b54e4b09f34364cefb2e/raw/432961c90bd5cf0e3768773093e662499855a574/Imported%20Document">yEd live</a>.
### Core Tables
<!-- BEGIN DB_SCHEMA -->
#### **1. Occurrences**

This table stores individual Occurrences (= short epigrams or poems, literally how they have been found in a manuscript, including marks for gaps and missing text.)

Columns include:
```id, created, modified, public_comment, incipit, text_stemmer, text_original, location_in_ms*, completion_date_floor, completion_date_ceiling, palaeographical_info, contextual_info, manuscript_id, title```

*Note that, in the current version, the location of occurrences within the manuscript is given as plain text (ex. p. 394-395 for pages or f. 18r-18v for folia). For manuscripts that have more than 1 way of numbering pages, the alternative location is marked as f. 14r -- (alt.) p. 27.

Related tables:

- `occurrence_person_role`: Links Occurrences to Persons, indicating which Role a Person plays in the given Occurrence. Example: Scribe ( = historical person), transcriber (=modern person), contributor (=modern person)...
- `occurrence_genre`: genre attributed to this Occurrence (Can be more than 1)
- `occurrence_metre`: metre attributed to this Occurrence (Can be more than 1)
- `occurrence_management`: Internal information. For example: To do's in the processing of this Occurrence
- `occurrence_acknowledgement`: Plain text acknowledgement of people who helped in the publication of this Occurrence.
- `occurrence_text_status`: An Occurrence text can have statuses like partially/completely (un)known
- `occurrence_related_occurrence` and `occurrence_relation_definition`: An Occurrence can be related to other Occurrence if (a) some of their verses share Verse Groups or (b) they share a Type. The relationship type is defined in `occurrence_relation_definition`. This works in one direction: if occurrenceA --> related to --> occurrenceB is set, then occurrenceB --> related to --> occurrenceA is not set.
- `occurrence_keyword`: Keywords telling what the Occurrence is about

#### **2. Verses**

This table contains verse-level information about an Occurrence.

Columns include `id, occurrence_id, manuscript_id, text, order_in_occurrence, verse_group_id.`

Verse Groups are groupings of similar verses across occurrences.

#### **3. Types**

This table contains prototypes of Occurrences. A lot of Occurrences have a high level of similarity. DBBE proposes prototypes for every group of similar Occurrences.

Related tables:

- `type_person_role`: Links Types to Persons, indicating which Role a Person plays in the given Type. Example: Creator, Translitor, Editor, Contributor, ...
- `type_genre`: genre attributed to this Type. More than 1 Genre can be attributed.
- `type_metre`: metre attributed to this Type. More than 1 Metre can be attributed.
- `type_management`: Internal information. For example: To do's in the processing of this Type
- `type_acknowledgement`: Plain text acknowledgement of people who helped in the publication of this Type.
- `type_text_status`: Type text can be either completely known or partially unknown
- `type_related_type`: Groups of similar Types. The relationship is defined in `type_relation_definition`. This works in one direction: if typeA --> related to --> typeB is set, then typeB --> related to --> typeA is not set.
- `type_tag`: Explains the function of the Type (ex: introducing a subject, making a comment on the content,...).
- `type_occurrence`: Occurrences linked to this Type. Note that this is a many-to-many relationship: one occurrence can be linked to several types, one type can have several occurrence linked to it.
- `type_editorial_status`: editorial states for types. Currently only critical text / not a critical text.
- `type_keyword`: Keywords telling what the type is about

#### **4. Manuscripts**

This table contains metadata about manuscripts.

Related tables:

- `manuscript_person_role`: Any possible role a Person could play in the publication of this manuscript. Example: Patron ( = historical person), Illuminator (=historical person), contributor (=modern person)...
- `manuscript_acknowledgement`: Plain text acknowledgement of people who helped in the publication of this Manuscript.
- `manuscript_content`: Explains what the manuscript is about. Careful: ```content``` is a hierarchical table. For example, a manuscript can be about Biblica -> Novum Testamentum. In this table, the lowest leaf (Novum Testamentum) is stored. The parent_id column of the `content` table can be used to trace the full content.
- `manuscript_identification`: Links a manuscript to one or more IDs that were used in canonical works to refer to this manuscript (ex: Diktyon)
- `manuscript_management`: Internal information. For example: To do's in the processing of this manuscript
- `manuscript_location`: The location where the manuscript was written. Careful: location is a hierarchical table. If a manuscript was written in Brussels, it is linked to Brussels, but via the parent_id column of the `location` table, you could also see that Brussels is in Belgium.

#### **5. Persons**

This table contains metadata about persons involved (authors, editors, patrons, etc.).

Related tables:

- `person_acknowledgement`: Plain text acknowledgement of people who helped in the publication of the information on this (historical) person.
- `person_identification`: Used to link persons to canonical IDs set by different authorities.
- `person_management`: Internal information. For example: To do's in the processing of this person
- `person_self_designation`: Used for scribes: How a scribe describes himself
- `person_office`: Used for scribes: The official title of a person.

#### **6. Bibliographies**

Bibliographies are modelled as concrete entity types, rather than a single table as in the original setup.

- `article`
- `book`
- `book_chapter`
- `blog_post`
- `bib_varia`: This table is usually avoided but contains entries for which no other bibliographical type exists.
- `online_source`
- `phd`

Each bibliographic entity has:

- its own table as mentioned above
- a corresponding `_person_role` table (ex.: article_person_role: could contain authors, contributors, reviewers, ... for a given article)
- tables linking to the item the bibliography is about:
    - manuscripts (ex: `manuscript_article`: contains articles about a given manuscript)
    - occurrence (ex: `occurrence_book`: contains books about a given Occurrences)
    - persons (ex: `person_article`: contains articles about - usually historical - persons)
    - type (ex: `type_article`: contains articles about given Types)

Additional structures: `journal` and `journal_issue`: Articles may be linked to journals and journal issues.

Note that, for now, some of these bibliography tables were added for completeness sake: not every concept (Manuscript / Occurrence / Person / Type) has all types of bibliographies linked to it (online sources, PhDs, etc.).

### **Lookup / Metadata Tables**

- `roles` — defines roles for persons (ex. Author, Scribe, Contributor, ...)
- `text_status` — textual status of Occurrence or Type. (ex. Text completely known, text partially unknown,...)
- `keywords` — keywords for Occurrence and Type (ex. Holy Trinity, Seven Sages, Last Judgement, ...)
- `tag` — tag for Type: Explains the function of the Type (ex: introducing a subject, making a comment on the content,...).
- `metre` — metre classification (ex. Dodecasyllable, Elegiacs,...)
- `genre` — genre classification. (ex. Scribe-related epigram, Text-related epigram, Reader-related epigram)
- `management` — administrative metadata. (ex. Bibliography to check)
- `acknowledgement` — acknowledgement linked to occurrence, manuscripts, type, or persons. (ex. Information on the manuscript courtesy of \<person x\>)
- `editorial_status` — editorial states for type. Currently only (not) a critical text.
- `self_designation` — how a scribe describes himself
- `office` — the official title of a person.
- `location` - location that could be linked to manuscripts, library, persons,... .
- `library` — library name and location. Note that a manuscript name is always City - library - collection - shelf
- `collection` — collection metadata. Note that a manuscript name is always City - library - collection - shelf
- `content` - Used for storing manuscript content. Careful: ```location``` is a hierarchical table. For example, a manuscript can be about Biblica -> Novum Testamentum. In this table, the lowest leaf (Novum Testamentum) is stored. The parent_id column of the content table can be used to trace the full content.
- `identification` - canonical ways to refer to persons or manuscripts (ex. Diktyon identifiers)
<!-- END DB_SCHEMA -->

----

## Tests

The tests in the testfolder start from the Postgres perspective: they go over every postgres table and check if all data in that table can be found in the SQLite db as well.,

----

## Zenodo

This repository uses the Zenodo API for the automatic publication of datasets. Full API docs can be found on <a href="https://developers.zenodo.org/#rest-api">https://developers.zenodo.org/#rest-api</a>. You can do all of these calls to <a href="https://sandbox.zenodo.org/">https://sandbox.zenodo.org/</a> as well. This environment functions in the exact same way as production so it's perfect for testing.  

In order to use the API, you need an access token, which you can generate by creating a Zenodo account and going to "My Account" > "Applications".

The database description part of this README is automatically synced to Zenodo so that database description only has to be maintained in one single place. 


----

## Demo

The demo folder contains a small example illustrating the type of application you could build using this dataset. The code was generated automatically by presenting the database DDL to ChatGPT and has not been checked for correctness. Its purpose is purely to demonstrate the ease of use and speed with which one can start building applications based on this dataset.

To run the demo locally, you can start a simple HTTP server with Python. Run the following command from the root of the repository. 

```
python -m http.server 8000
```

This uses Python’s built-in HTTP server module, so no additional packages are required. It serves files at http://localhost:8000/demo/byzantine-db.html
, allowing you to explore the demo in your browser. The demo HTML/JavaScript loads the SQLite file directly in the browser using sql.js
, which enables full client-side querying without a database server and is a capability essentially unique to SQLite’s single-file design. This setup is intended for testing and exploration only, not for production deployment.

---


## Next steps

- How to keep the docs and the db schema updated: 
