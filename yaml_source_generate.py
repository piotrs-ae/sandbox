# SCRIPT DESCRIPTION

# Below script will allow to read all of the tables names from a specified schema
# once saved locally as a CSV, python function will read this CSV file based on its path 
# and the contents can be parsed as a list of the tables into a source.yml create function

# END OF SCRIPT DESCRIPTION

# SQL for tables' names retrieval

# SELECT '"' || TABLE_NAME || '"' AS quoted_table_name
# FROM INFORMATION_SCHEMA.TABLES
# WHERE TABLE_SCHEMA = '<schema_name>'
#   AND TABLE_CATALOG = '<database_name>'
#   AND TABLE_TYPE = 'BASE TABLE'
# ORDER BY TABLE_NAME;

# end of SQL

# Python for reading CSV and returning list of tables

import csv

def read_csv_tables(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        tables = [row[0] for row in reader]
    return tables

# end of pythong for returning list of tables

# Optional install if you have not done so locally
# pip install PyYAML

import yaml
import argparse

# you need to provide the name of the source, database, schema and list of the tables
# list of the tables should have tables' names in quotation marks, separated by coma
# it should be the actual names only, without the full path
# example: tables_list ['DIM_USER', 'DIM_SESSION_USER', [...]]
# please double check that this is the format returned by the python function above
# alternatively you can generate the list yourself and just apply to the function

def create_source_yaml(source_name, database_name, schema_name, tables):
    source = {
        'version': 2,
        'sources': [
            {
                'name': source_name,
                'database': database_name,
                'schema': schema_name,
                'tables': [{'name': table} for table in tables]
            }
        ]
    }

    with open('source.yml', 'w') as outfile:
        yaml.dump(source, outfile, default_flow_style=False)

# This, below, when executed, will create a new YAML file in the current working repository 
# Or replace an existing one
# create_source_yaml(source_name, database_name, schema_name, tables) 

