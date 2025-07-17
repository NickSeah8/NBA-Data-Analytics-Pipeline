# 🏀 NBA Data Analytics Pipeline

## 🚀 Mission Statement 

This project is an end-to-end data analytics pipeline built around publicly available NBA player and game data. It is designed to simulate an enterprise-grade ETL/ELT architecture and demonstrate the skills of ingestion, transformation, and consumption layers that follow the best practices of modern data engineering and cloud capabilities.

## 🎯 Goals of the Project

The goal was to consolidate my knowledge of data pipelines and the end-to-end data process using an area of personal interest. I particularly enjoy playing and watching sport, and it's this reason that I chose to ingest NBA data via the 'nba_api' client package, which [@swar](https://github.com/swar) compiled to utilize publicly available data the NBA provides.

After the data ingestion stage, the three main goals were to:
* Store the data in a structured cloud database
* Transform it for advanced analytical queries and store it in a big data capacity with relevant schema for querying
* Visualize key metrics and showcase dashboarding capability of database

By demonstrating the clear separation of data ingestion, transformation, and consumption layers, I hope to deepen both my own and the reader's understanding of data pipelines and ETL/ELT processes through engaging and relatable data (at least for sports fans)

Finally, I used different platforms at different stages of the project to broaden my knowledge of various cloud services and expand my insight and grasp on the capabilities and best practices of modern data analysis and storage architecture.

## 🎓 Prerequisites
If you wish to replicate the cloud components of this project, please ensure you have the following:
* Environment variables/secrets set up
    * Azure Storage account keys or service principal for data lake access
    * Azure SQL Database credentials for data storage
    * Snowflake account (trial is fine for temporary use)
    * Access to Microsoft Power BI
* Required Python packages
    * Main one is to install nba_api by running pip install nba_api
    * Other relevant packages can be found in relevant local scripts found in 'data ingestion' folder

## 🔧 Tech Stack

* Programming: Python, SQL
* Cloud Storage & Data Warehouse: Azure (Data Lake Storage, Blob Storage, Data Factory), Snowflake
* Data Processing: Snowflake (SQL & Snowpark), local Python scripts (using Pandas), AzCopy
* Visualisation: Power BI
* Version Control & Documentation: GitHub with structured README and notebooks

## 📝 Pipeline Workflow Steps

### Overall Architecture Flow
**Local Python Scripts** (Bronze ➔ Raw) ➔ **Azure Blob Storage** ➔ **ADLS Gen2** (Silver ➔ Processed) ➔ **Snowflake** (Gold ➔ Curated Warehouse) ➔ **Power BI** (Consumption)

### Step 0: Project Setup

Initialize GitHub repository with clear folder structure (see project scaffold.txt)

Developed with Python 3.13.1 in VSCode. Snowflake transformations executed using Snowpark notebooks.

### Step 1: Data Ingestion

Source: NBA Stats API Client Package via [nba_api](https://github.com/swar/nba_api).

[insert image]

The first step consists of gathering our relevant data. For this project, the focus is on gathering the data which we will then distribute into fact and dimension tables.

For any non-hardcore NBA fans, there are the traditional boxscores which track basic in-game stats such as points, rebounds, and assists. For the more invested fans, basketball analysts over the years have created advanced stats which help track a players impact who doesn't typically fill up the traditional boxscore. To fill our database and create our schema, I chose to gather some key advanced statistic areas that I thought would be interesting to analyse. They consist of the following:

|   Fact Tables |   Dimension Tables    |
|   --- |   --- |
|   Advanced_Processed_2024_25  |   Players_Processed_2024_25   |
|   Hustle_Processed_2024_25    |   Teams_Processed_2024_25 |
|   Playertrack_Processed_2024_25   |   Schedule_Processed_2024_25  |
|   Scoring_Processed_2024_25   |   |
|   Traditional_Processed_2024_25   |   |
|   Usage_Processed_2024_25 |   |

As you can see, there are multiple fact tables with denormalized dimension tables, meaning this is a constellation schema. This is done as we are looking at a number of different measures from different endpoints of the API that come from a particular player's performance in a particular game. Despite this potentially increasing storage redundancy, it will improve any query performance as we avoid the need for multiple joins. Additionally, we want this repetitive data in each of our fact tables to ensure we can see what game the statistics are from, so therefore we won't normalize our tables. The data dictionary.xlsx file holds more information on the structure of each table at the reporting stage.

Take a moment to go through the data ingestion folder of the project. The config.py file contains all the functions and classes used to create the logic for pulling the endpoints. The three RUN.py files pull player and team data (RUN_info.py), schedule data (RUN_games.py), and statistical game data found in the boxscore endpoints (RUN_boxscore.py). RERUN_off_checkpoints.py is used if at any time the RUN_boxscore.py script is stopped and you want to continue from the checkpoint files you have already ingested, and the appending_final_files.py script is for gathering the raw csv files back together in the format required for later steps.

After the scripts are run, the raw folder in data is populated as detailed below. These are the files we will be pushing through the pipeline.

[insert image - ingested files]

### Step 2: Data Storage Loading

With the extract stage of the project completed, the next step of simulating a data pipeline can be done in two different ways. While the path I took involved loading the raw data into storage, the goal of the project as detailed above was to simulate ETL/ELT architecture. Therefore, the transformations done in step 4 can also be done locally, and performed before steps 2 and 3. However, I will detail how I completed the load stage of the project before cleaning the data.

I chose to use the Azure Blob Storage service to understand the Microsoft Azure environment better, as well as use a service that handles large quantities of data well and has good scalability. The first thing needed to be done was create a trial account; this was followed by creating my subscription (used to hold and group all of your Azure resources) and my Azure Storage Account (which further groups all your resources within your subscription into resource groups). 

I then created my blob storage account, which is where the raw data will be ingested into. Ensure you have created the account as a StorageV2 account, or that hierarchical namespace is enabled, as this enables file/folder semantics required later for data lake functionality and operations. To further my understanding, I downloaded Azure Storage Explorer to get a feel for the UI and layout of how and where the data is stored, which can be seen below.

[insert image - Azure Storage Explorer]

While I could simply upload the raw data from my local machine onto the blob storage container with the click of a button, I wanted to handle the data loading process as if I was working with huge amounts of data. By downloading the AzCopy command-line utility to my terminal, I hoped to simulate what it would be like to handle the transfer of a large amount of files, as well as create automation with a single line of code able to be run whenever needed to upload a folder of files, as opposed to singular files.

[insert image - AzCopy code to load data]

### Step 3: Data Pipeline Loading

[insert image - loaded data blob storage]

With the data now in blob storage, it's time to upload to a data lake. The image below describes the best practice flow of a data pipeline using Azure Data Factory.

[insert image - ADF pipeline]

While you can push data straight from blob storage to a SaaS (Software as a Service) or PaaS (Platform as a Service), which does require fewer steps, this results in less architectural clarity and leaves you without a secure layer of raw data for future use. Furthermore, this aligns with best practices of modern data architecture, known as the Medallion Architecture, where there is a raw ingestion layer (bronze), transformation layer (silver), and a consumption layer (gold).

[insert image - lakehouse medallion architecture]

To upload to a data lake in Azure, we need to create another blob container, but we now need to enable hierarchical namespace to ensure our next blob storage container is a ADLS Gen2 (Azure Data Lake Storage Gen2) compatible.

Once this is done, we need to make an Azure Data Factory. To ensure data is moved between our two blob containers, we link them in the linked services area of the Azure Data Factory Studio. Without going into too much detail on privacy settings, you can either set account permissions for authentication and connection to the data lake, or you can generate an SAS key. For simplicity, I chose to create an SAS key.

[insert image - Linked Services]

From here, we now go to the author tab and create our pipeline. As we have 9 different files of different format, I used a ForEach activity to iterate over the blob dataset, with file names as an array. A copy data activity is used within the ForEach loop to push the data from blob storage to data lake. This is done since each of the files (data tables) have a different schema, and allows for easier error handling and per-file logging.

[insert image - running pipeline]

Below is how the loaded data in the lake now looks in Azure Storage Explorer.

[insert image - files in data lake]

### Step 4: Data Processing & Transformation

The next step is to transform the loaded data (going from the silver to gold layer) and ensure table structure and schema is clean and ready for reporting. This next step was done in Snowflake as I wanted to become more familiar with the tool, but in the future I may replicate it in Databricks to show the same process being done on a different service.

To load the data in Snowflake, you need the URL of the ADLS Gen2 container, followed by the SAS token (if using this security method). Go to add data for Microsoft Azure Blob Storage and follow the prompts to create a database for your project using your ADLS Gen2 authentication. Once this is done, you should have your data files in a stage.

[insert image - snowflake - raw stage]

This is where you begin your database management with DDL (Data Definition Language) and DML (Data Manipulation Language) queries to create and fill your data tables. 

[insert image - creating tables]

Additionally, you will need to use Snowflake's Snowpark feature to use Python for the data transformations. Snowpark is a framework designed for languages such as Python to be used within Snowflake. While the full scripts for the SQL queries can be found in the database folder, and the Python script can be found in the data cleaning folder, the screenshots below provide a glimpse of the process within the Snowflake UI, and the finalized tables.

[insert image - Snowflake - python transformations]

[insert image - Snowflake - creating primary and foreign keys]

### Step 5: Data Storage (Processed Layer)

[insert image - Snowflake - transformations and processed tables]

Above is how the finished tables will look once the data is all loaded and transformed into processed tables, followed by the DDL queries to create the primary and foreign keys. These tables in Snowflake is now your gold layer data warehouse, which is ready for DQL (Data Query Language) querying and reporting level analysis.

### Step 6: Data Reporting

[insert image - loading data from Snowflake into Power BI]

The final step is to now perform reporting analysis on the cleaned data. Snowflake has connectors with various softwares, and Microsoft Power BI is no exception. All you need to do is select get data in Power BI and input your server and warehouse details when prompted.

When connecting the Snowflake data to Power BI, to simulate a pipeline with scheduled refreshes and connection to live data, DirectQuery is the better mode to use. This mode allows for real-time updates and lower storage usage. However, since a Snowflake trial was used for this project, Import mode was used so as to provide a snapshot of the data and store it within the pbix file.

You can now build a report and find insights within the data.

### Step 7: Data Modelling

Now that we're connected, if we think back to the idea of our data being in a constellation schema, we can now see it visually in Power BI using its' model view feature. Below is how the tables relate to each other visually (use the data dictionary.xlsx file to supplement your understanding of column relationships).

[insert image - relationships in Power BI]

We can see that the model looks a little messy compared to a typical star schema. There are some key differences in our Power BI model compared to the gold layer reporting schema, such as key columns added for visuals (Full Name, Name + ID and Headshot URL in Players table, and Team Logo URL for Teams table), as well as the creation of a date table based off the range of dates from the Schedule_Processed_2024_25 table, since date tables are extremely important when using time intelligence functions in Power BI. Parameters for changing measures visualized in graphs were also created and are shown in the model.

Another thing to keep in mind is that Power BI does not support composite keys. To ensure model stability when creating a dashboard, use the following code in PowerQuery to make a primary key for the boxscore stats from GAME_ID and PLAYER_ID;
= Table.AddColumn(Source, "GamePlayerKey", each [GAME_ID] & "-" & [PLAYER_ID])

### Step 8: Dashboard and Visualization Insights

The main goal for our reporting stage was to provide any quick and actionable insights based off NBA player and team data, as well as to create an aesthetically pleasing and sensible dashboard with the wide range of features and tools in Power BI.

[insert image - Reporting Dashboard (1)]
Dashboard Image 1

The image above shows the main page of the report. There are a number of key metrics placed as cards above the bar chart on the left and the scatter plot on the right.

The bar chart can be selected to filter some of the key metrics to that particular player's stats. We can see that Nikola Jokić is the only player who is in the top 10 for all of points, assists, and rebounds.

You can also hold the mouse over the bar to see the net, offensive, and defensive ratings of the player in the tooltip. Finally, there is a dropdown box to choose what measure to see on the bar chart (based off some of the main traditional stats). These can be seen in dashboard images 2 and 3.

[insert image - Reporting Dashboard (2)]
Dashboard Image 2

[insert image - Reporting Dashboard (3)]
Dashboard Image 3

The scatter plot lists all 30 NBA teams based on their average offensive and defensive ratings for the season. Fitting that we can see OKC (the team who won the 2024-25 NBA championship) has the best defensive rating while also being top 3 in offensive rating, which actually gives them the best net rating. 

When you select a team's point, the bar chart and key metrics filters for that particular team. You can also hover over the point to see the team's ratings in the tooltip just like the bar chart.

[insert image - Reporting Dashboard (4)]
Dashboard Image 4

When you right click on a player in the bar chart, there is an option to drill through to another page. This page shows that particular player's rankings among the traditional stats on the bottom left, as well as a number of key advanced statistics on cards. There is also a pie chart that shows either the distribution percentage of field goal attempts across 2-point and 3-point attempts, or the distribution of points across 2-pointers, 3-pointers, and free throws. Finally, the line chart shows the running total of the traditional stats across the season.

[insert image - Reporting Dashboard (5)]
Dashboard Image 5

[insert image - Reporting Dashboard (6)]
Dashboard Image 6

## 💭 Final Thoughts

This project allows us to fully demonstrate the end-to-end process of ingesting, transforming, and analysing data through various services such as Azure, Snowflake, and Power BI.

We are able to see the way data is gathered (in this case, via a Python API client package), how it's stored and transformed, and finally, how it's used to create reports and visualized with the hope of understanding the data and deriving any insights.

While there are limitations in crafting a pipeline example, such as having to use trial accounts for cloud services, hopefully this project has helped whoever is reading this to improve their understanding of data pipelines and the process of devising a full end-to-end data solution.

## 🌱 Future Improvements

* Edit dashboard visualization and find more insights from the data + add a team analysis and comparison section
* Set up Power BI scheduled data refresh trigger (only available on Power BI Service)
* Implement a Databricks Spark-based processing pipeline as an alternative to Snowflake for transformation scalability
* Replicate pipeline process and final data reporting tables and schema in Azure Synapse Analytics
* Increase CI/CD capabilities by integrating Azure DevOps Pipelines or more advanced GitHub Actions workflows
* Integrate dbt for SQL-based transformations and data lineage tracking
* Build a monitoring dashboard for pipeline runs and performance

## 👤 Author & Contact

**Nicholas Seah**
* GitHub: [NickSeah8](https://github.com/NickSeah8)
* LinkedIn: [Nicholas Seah](https://www.linkedin.com/in/nicholas-seah8/)
* Email: nseah8@gmail.com

## 📡 Data Source & Attribution

NBA data is accessed using the [nba_api](https://pypi.org/project/nba_api/1.1.5/) package, developed by [@swar](https://github.com/swar). Licensed under MIT.

---

*This project is built for learning and portfolio demonstration purposes.*

