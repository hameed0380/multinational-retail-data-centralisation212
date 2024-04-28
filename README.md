 # Multinational Retail Data Centralisation

![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue) ![Postgresql](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white) ![AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white) ![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)  ![Git](https://img.shields.io/badge/GIT-E44C30?style=for-the-badge&logo=git&logoColor=white) ![VSCode](	https://img.shields.io/badge/VSCode-0078D4?style=for-the-badge&logo=visual%20studio%20code&logoColor=white)</div>

## Table of Contents
- [Multinational Retail Data Centralisation](#multinational-retail-data-centralisation)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Process](#process)
  - [Installation](#installation)
  - [Usage](#usage)
  - [ERD](#erd)
  - [File Structure](#file-structure)
  - [License](#license)

## Description
This project centers on the ETL (Extract, Transform, Load) process, with a specific focus on sales-related data. It involves extracting data from a diverse array of sources including APIs, AWS S3 buckets, CSV files, and PDFs. Subsequently, the data undergoes thorough preparation, encompassing cleaning and transformation tasks such as standardizing data types and ensuring uniform formatting across columns with similar data. The processed data is then loaded into a centralized PostgreSQL database.

The overarching goal of this project is to streamline data management by consolidating disparate data sources into a single, centralized repository. By establishing a unified source of truth for all sales-related data, the project facilitates easy access and comprehensive analysis, thereby fostering a data-driven decision-making culture within the organization.

![Overview of Project](Images/MRDC-GRAPH.png)

## Process

1. Extraction: Extract data from different sources, including CSV files, an API, AWS S3, and an AWS RDS database.

2. Transformation: transforming (and cleansing) the extracted data to make sure there consistency and it is compatible with a destination.

3. Loading: loading the cleansed data to the destination in this case sales_data PostgreSQL database.

## Installation
1. Clone the project repository from GitHub

   Clone the repository:

   ``` bash
   git clone https://github.com/hameed0380/multinational-retail-data-centralisation212.git
   cd multinational-retail-data-centralisation212
   ```
2. Install the dependencies

   Ensure you have necessary dependencies installed. You can install them using the following command:
   ```
   pip install -r requirements.txt
   ```
3. Set up database credentials:
   - Make sure PostgreSQL is set up and running.
   - Set up a database named sales_data.
   - Create a db_creds.yaml file with your PostgreSQL credentials.
## Usage

- **data_extraction.py:** Contains the DataExtractor class, responsible for extracting data from various sources such as CSV files, APIs, AWS S3, and databases.

- **data_cleaning.py:** Contains the DataCleaning class, which handles the cleaning and transformation of extracted data to ensure consistency and compatibility with the destination database.

- **database_utils.py:** Includes the DatabaseConnector class, which manages database connections and facilitates the uploading of data to the PostgreSQL database using pgAdmin.

- **queries.sql:** Contains SQL scripts for modifying and defining the database schema. Utilizes a star schema, where the orders_table serves as the fact table. Also includes queries to extract useful insights from the database.

- **viewdataframes.ipynb:** Main script providing a comprehensive view of the project. Enables analysis of data tables as dataframes, allowing thorough examination of the data in its original state and after any modifications have been applied.


Run the project using the instructions provided in the "Installation" section.
You can run the program via the viewdataframe.ipynb file

## ERD

Entity Relationship Diagram of database<br>

![Entity Relationship Diagram of Database](Images/ERDiagram.png)

## File Structure
    .
    ├── Images
    │   └── ERDiagram.png
    ├── README.md
    ├── data_cleaning.py
    ├── data_extraction.py
    ├── database_utils.py
    ├── db_creds.yaml
    ├── products.csv
    ├── queries.sql
    └── viewdataframes.ipynb

## License



[def]: #multinational-retail-data-centralisation