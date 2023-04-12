# pip install PyYAML

import yaml
import argparse

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a YAML file for DBT source.')
    parser.add_argument('source_name', help='Name of the source')
    parser.add_argument('database_name', help='Name of the database')
    parser.add_argument('schema_name', help='Name of the schema')
    parser.add_argument('tables', nargs='+', help='List of table names separated by space')

    args = parser.parse_args()

    create_source_yaml(args.source_name, args.database_name, args.schema_name, args.tables)
