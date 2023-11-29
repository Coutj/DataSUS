# DATASUS Pipeline

A pipeline project to handle DATASUS SIM data.

_This project is a part of my learning process in the data engineering field. However, it was built as an exercise and is not intended for production use._

In this project we can extract data from the Brazilian Unified Healthcare System (SUS), load all this massive information into a data lake, and then execute the processing step to format the dataset (a classic ELT approach).

To get more details, check out my post on Medium.

## Key Components


The architecture of this project rests upon a combination of technologies, packages, and services that I would like to briefly discuss.

### PySUS
To fetch the required files from DATASUS, I used a Python package called PySUS. With this package, we can effortlessly download the files and save them in Parquet format for later processing. All it takes is specifying a few straightforward parameters: the City Abbreviation code, the year period, and the code for identifying diseases (such as the ICD-10 pattern).

### Azure Data Lake Gen2
ADLS marks the next step in our pipeline, where we can store all data previously downloaded and the processed data. To keep things simple, our container was divided into two different zones: raw and trusted. In the raw zone, we store the Parquet files directly obtained from PySUS, and in the trusted zone, we place all post-transformation files.

### Databricks
This block will serve as our platform to transform and enrich the raw data. To minimize costs, a Standard_F4 node with 8 GB and 4 cores was chosen to compose our compute cluster. With this configuration, it was possible to execute 2 transformation jobs in parallel.

### Azure Key Vault
To enable Databricks to retrieve and store files from ADLS, a SAS token was used and stored as a secret in the key vault. The same process was followed for the PostgreSQL connection, where the database credentials were stored as a new secret.

### PostgreSQL
To enable the use of our visualization tool, all transformed file data was stored in a table called "datasus". To take advantage of the free tier, I'm using the Azure Database for PostgreSQL flexible server service with a Standard_B1ms compute size and 32 GiB storage. This choice proved to be somewhat inefficient in terms of query speed but will serve our study purpose.

### Looker Studio
The choice of Looker as a visualization tool is simple: It's free, easy to use, can connect to plenty of sources, and is web-based. The intent with this block is to build a simple dashboard to explore our data. A short example will be shown at the end of this article.

### Airflow
Our pipeline tool was built using the Astronomer CLI approach, making tasks such as installing the Apache Airflow infrastructure easier. I just need to use two commands, "astro dev init" and "astro dev stop", and everything is done.
Our main objective is to extract all data from the past five years for each Brazilian state, load this information onto ADLS, and then store the transformed data.
