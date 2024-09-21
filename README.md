# Energy Trend Data ETL Pipeline

## Table of Contents
- [Overview](#overview)
- [Task Requirements](#task-requirements)
- [Technical Requirements](#technical-requirements)
- [Pre-requisites](#pre-requisites)
- [Installation and Setup](#installation-and-setup)
- [Conclusion](#conclusion)

## Overview

The data analysis team requires a new data pipeline for downloading and cleaning energy trend data. The data, available on the UK government's website, contains information about the supply and use of various energy sources. Your job is to develop a script that can automatically download, clean, validate the quality of this data, and save it as a CSV file. The script should be scheduled to run daily to check for any new datasets, ensuring that the latest data is always available to the analyst.

The analyst wants to start with the dataset "Supply and use of crude oil, natural gas liquids, and feedstocks" from the UK government's website: [UK Government Energy Trends](https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends). The data comes as an Excel spreadsheet with multiple tabs, but the analyst is only interested in the “Quarter” tab.

## Task Requirements

Your task is to perform the following operations:

1. **Download New Data**: Write a script that can check for new data and, if a new dataset is detected, download the new Excel file. The download should handle network errors and include retry mechanisms.
   
2. **Data Cleaning**: Clean the data to remove any unnecessary information and ensure that the data is in a consistent and well-structured format.
   - Missing values should be left blank.
   - Ensure that any dates and timestamps are converted into a standard date format of `yyyy-MM-dd` and `yyyy-MM-dd HH:mm:ss` for timestamps.
   - Retain information about when the data was processed and the original filename.

3. **Data Validation**: Implement basic checks that ensure the integrity of the downloaded file, including:
   - A threshold for the number of rows.
   - Checking the number of missing values.
   - Verifying that the file contains key columns.

4. **Data Saving**: Save the resulting DataFrame to one of the following formats (depending on whether you use Pandas or Spark):
   - CSV file in a format that can be easily ingested into a data lake.
   - Delta table.

5. **Delta Table Considerations**: Regardless of whether you choose Pandas or Spark, consider the following aspects:
   - **Read Patterns**: Describe the typical read queries you expect (e.g., time-range queries, filtering by specific columns) and how your design optimizes for these patterns.
   - **Write Patterns**: Explain how frequently new data will be written to the table and any strategies to handle high write throughput (e.g., batch writes, upserts).
   - **Concurrency**: Discuss how you would handle concurrent reads and writes, ensuring data consistency and performance.
   - **Deduplication and Upserts**: Explain your approach to managing deduplication and upserts.

## Technical Requirements

- Must be implemented as a Python PIP Package using Python version 3.10.
- Must be implemented in either Pandas or PySpark.
- Must include appropriate unit and integration tests.
- The location of the output file/table should be a configurable parameter of the package with a sensible default.
- The solution must provide adequate logging for production support to diagnose any issues.
- Include a `README.md` with instructions about the solution and how to run it.

## Pre-requisites

Before you begin, ensure you have the following installed:

1. **Python (3.10)**: This project requires Python version 3.10. You can use tools like [pyenv](https://github.com/pyenv/pyenv#installation) or [conda](https://docs.anaconda.com/miniconda/miniconda-install/) (This guide will use `conda`) to manage your Python versions locally. Install `pyenv` or `conda` if you haven't already and set Python 3.10 as the local environment.

2. **Poetry**: Poetry is used for dependency management and packaging in this project. Make sure you have Poetry installed. You can install it by following the instructions at [Poetry's official website](https://python-poetry.org/docs/#installation).

## Installation and Setup

To set up the project environment and dependencies, follow these steps:

1. **Clone the Repository**

   Start by cloning the repository to your local machine:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
    ```

2. **Set Up Python Environment with Conda**

   Ensure you are using Python 3.10. Set up a virtual environment using `conda`:

   ```bash
   conda create -n energytrend-env poetry python=3.10
   conda activate energytrend-env
   ```

3. **Install Project Dependencies**

   Use `Poetry` to install all necessary dependencies for the project:

   ```bash
   poetry install
   ```
   Make sure you are in the root directory of the repository.

4. **Build and Install the Project**

   After installing the dependencies, build the project using `Poetry`:

   ```bash
   poetry build
    ```
    Make sure you are in the root directory of the repository.
5. **Install the Package**

   Once you have built the project, you can install the package using the following command:

   ```bash
   pip install .
    ```
    Make sure you are in the root directory of the repository.

6. **Run the Installed Package**

   After successfully installing the package, you can run it directly using the command line. If your package includes an entry point or script, execute it as follows:

   ```bash
   python -m energytrend_etl.main --output-path ./output
    ```

7. **Schedule the ETL Pipeline**
   
   To automate the ETL pipeline, this project utilizes [Prefect](https://docs.prefect.io/latest/getting-started/quickstart/), a modern workflow orchestration tool. Prefect provides robust features for scheduling, monitoring, and managing workflows, making it an ideal choice for orchestrating the ETL processes in this project.

    - To schedule the ETL pipeline, you can use the deploy_daily.py script, which sets up a deployment for the ETL flow to run at midnight every day:

        ```bash
        poetry run python deploy_daily.py
        ```

        When using Prefect on a local server, you might encounter the following warning:

        ```txt
        23:13:41.808 | WARNING | prefect.runner - Cannot schedule flows on an ephemeral server; run prefect server start to start the scheduler.
        ```

        This warning indicates that scheduling on a local ephemeral server is not supported, and you need to start a local Prefect server using `prefect server start` to manage the scheduler. However, for a more robust and scalable solution, it is recommended to use Prefect Cloud with a dedicated remote server to handle scheduling and execution more efficiently.

    - Once the deployment is created, you have a couple of options:
        - Execute the Deployment Immediately: You can trigger the ETL pipeline deployment right away using the following command:

        ```bash
        prefect deployment run 'Energy Trend Data ETL/Daily Energy Trend Data ETL Deployment'
        ```

        - View and Manage the Deployment on the Prefect Server: To view the deployment or manage it interactively, you can start a local Prefect server:

        ```bash
        prefect server start
        ```

        Running this command initiates a local Prefect server, providing a web-based interface to monitor and manage your ETL workflows. This is particularly useful for development and testing purposes.

## Conclusion
![Screenshot 2024-09-21 224220](https://github.com/user-attachments/assets/d40dd389-dc8f-4f7d-b71a-1a9e9458e41b)
This project provides a comprehensive ETL pipeline for ingesting, preprocessing, validating, and analyzing energy trend data. It also include data consistency and profiling reports to provide a bit of analytics around the cleaned data. By leveraging Python and various libraries, we ensure that the process is efficient, reliable, and automated.

Prefect serves as the workflow orchestrator, providing scheduling capabilities to automate the ETL tasks. By deploying this pipeline, users can consistently manage and analyze energy datasets, making it ideal for data scientists, analysts, and professionals in the energy sector.

By following the installation and setup instructions, you can easily get the ETL pipeline up and running, enabling you to handle energy data more effectively and derive actionable insights with minimal manual intervention.