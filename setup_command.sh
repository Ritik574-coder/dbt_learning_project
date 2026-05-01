####################################################################################
# PROJECT SETUP: requirement tools for data pipeline (Docker + Python + SQL Server)
# Author: Ritik
# Purpose: Setup virtual environment, install dependencies, configure database
#          credentials, and run the pipeline script.
####################################################################################



################################################################################
################### DOCKER INSTRALLITION AND SETUP #############################
################################################################################
#Install dependencies
sudo apt update
sudo apt install ca-certificates curl gnupg lsb-release

#Adding Docker official GPG key
sudo mkdir -p /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Adding Docker repository
echo \
"deb [arch=$(dpkg --print-architecture) \
signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu \
$(lsb_release -cs) stable" | \
sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt update

sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Starting and enable Docker
sudo systemctl start docker
sudo systemctl enable docker

#Fixing permission
sudo usermod -aG docker $USER

# Verify versions
docker --version
docker compose version

################################################################################
################### MSSQL INSTRALLITION AND SETUP ##############################
################################################################################
# Pulling SQL Server image
docker pull mcr.microsoft.com/mssql/server:2022-latest

# creting and running container
docker run -e "ACCEPT_EULA=Y" \
    -e "MSSQL_SA_PASSWORD=Ritik@843313" \
    -p 1433:1433 \
    --name sqlserver \
    -v sql_data:/var/opt/mssql \
    -d mcr.microsoft.com/mssql/server:2022-latest

# Check running containers
docker ps

# Check all containers 
docker ps -a

# Check downloaded images
docker images

# Check Docker volumes 
docker volume ls

# Start container 
docker start sqlserver

# Stop container
docker stop sqlserver

# Access container terminal
docker exec -it -u root sqlserver bash


# Add Microsoft repo
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Update
sudo apt update

# Install ODBC Driver 18 
sudo ACCEPT_EULA=Y apt install msodbcsql18

# Optional but useful tools
sudo apt install unixodbc-dev

# This opens SQL CLI (sqlcmd)
docker exec -it sqlserver /opt/mssql-tools/bin/sqlcmd 
-S localhost -U sa -P "Ritik@843313"

# moving data 
docker cp /workspaces/dbt_learning_project/script/dataset sqlserver:/data/

# access SQL serve cli
docker exec -it -u root sqlserver bash

#chage file permition 
chmod -R 777 /workspaces/dbt_learning_project/script/dataset

################################################################################
######################## DATABASE AND PYTHON SETUP #############################
################################################################################

# creating python virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install pyodbc for SQL Server connection
pip install pyodbc

# Set environment variables for database connection
nano ~/.bashrc

# Add the following lines at the end of the file
export DB_USER=sa
export DB_PASSWORD=Ritik@843313

# Load variables from ~/.bashrc
source ~/.bashrc

# Verify environment variables
echo $DB_USER
echo $DB_PASSWORD

################################################################################
########################### AIRFLOW AND DAGS SETUP #############################
################################################################################
# Creating a filder for airflow 
mkdir airflow_init

# Create Required Directories
mkdir ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

# Fetch the Official Docker Compose File
curl -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

# Initialize Airflow
docker compose up airflow-init

#  Start Airflow
docker compose up -d

## info 
# URL: http://localhost:8080
# Username: airflow
# Password: airflow

# Exit immediately if any command fails
set -e

# user for stoping docker compose
docker compose stop

# Use for starring docker compose
docker compose start

################################################################################
############################## DBT PROJECT SETUP ################################
################################################################################

# Create project folder
mkdir dbt_project
cd dbt_project

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate   # Linux/Mac
# venv\Scripts\activate    # Windows

# Install dbt (choose adapter)
# pip install dbt-postgres
pip install dbt-sqlserver
# pip install dbt-bigquery
# pip install dbt-snowflake

# Verify installation
dbt --version


################################################################################
############################ INITIALIZE DBT PROJECT #############################
################################################################################

# Initialize new dbt project
dbt init my_dbt_project

cd my_dbt_project

# This creates:
# models/
# dbt_project.yml
# profiles.yml (in ~/.dbt/)


################################################################################
############################ DATABASE CONNECTION ###############################
################################################################################

# Edit profiles.yml
nano ~/.dbt/profiles.yml

# Example (PostgreSQL):
# my_dbt_project:
#   target: dev
#   outputs:
#     dev:
#       type: postgres
#       host: localhost
#       user: your_user
#       password: your_password
#       port: 5432
#       dbname: your_db
#       schema: public

# Test connection
dbt debug


################################################################################
############################## CORE DBT COMMANDS ################################
################################################################################

# Run models
dbt run

# Run specific model
dbt run --select model_name

# Test models
dbt test

# Seed CSV data into database
dbt seed

# Snapshot (for slowly changing data)
dbt snapshot

# Generate documentation
dbt docs generate

# Serve docs locally
dbt docs serve
# URL: http://localhost:8080


################################################################################
############################## DEVELOPMENT FLOW #################################
################################################################################

# Compile SQL without running
dbt compile

# Run + Test together (most used)
dbt build

# Run only modified models
dbt run --select state:modified

# Run downstream dependencies
dbt run --select model_name+

# Run upstream dependencies
dbt run --select +model_name

# Full refresh (rebuild tables)
dbt run --full-refresh


################################################################################
############################## PROJECT STRUCTURE ################################
################################################################################

# Create model folders
mkdir models/staging
mkdir models/marts

# Example model file
touch models/staging/stg_orders.sql

# Example SQL:
# SELECT * FROM raw.orders


################################################################################
############################## PACKAGE MANAGEMENT ###############################
################################################################################

# Install packages
dbt deps

# packages.yml example:
# packages:
#   - package: dbt-labs/dbt_utils
#     version: 1.1.1


################################################################################
############################## ENVIRONMENT CONTROL ##############################
################################################################################

# Use target environment
dbt run --target dev
dbt run --target prod

# Use variables
dbt run --vars '{"key": "value"}'


################################################################################
############################## CLEANUP ##########################################
################################################################################

# Clean compiled artifacts
dbt clean


################################################################################
############################## STOP / EXIT ######################################
################################################################################

# Deactivate virtual environment
deactivate