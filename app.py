import awsgi
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from google.cloud import secretmanager
from openai import OpenAI
import json
import re
from io import StringIO
import lkml
import yaml
from datetime import datetime
from github import Github


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
app = Flask(__name__)

CORS(app)
# Function to fetch API key from Google Secret Manager
def fetch_api_key():
    client = secretmanager.SecretManagerServiceClient()
    secret_name = "projects/462434048008/secrets/openai_api_key/versions/1"  # Replace with your project ID
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8").strip()

# Initialize OpenAI client with fetched API key
api_key = fetch_api_key()
print
apikey_dict = json.loads(api_key)
api_key_main = apikey_dict['api_key']
client = OpenAI(api_key=api_key_main)


def process_lookml(repo_link,github_token):

    def search_column_in_view(view, column_name):
        """Search for a column name in dimensions, measures, and transformed dimension groups of a specific view."""
        # Search in dimensions
        for dimension in view.get('dimensions', []):
            if dimension.get('name') == column_name:
                transformed_sql = dimension['sql'].replace('${TABLE}.', '') + f" as {dimension['name']}"
                return {
                    "view_name": view['name'],
                    "type": "dimension",
                    "name": column_name,
                    "sql": transformed_sql
                }

        # Search in measures
        for measure in view.get('measures', []):
            if measure.get('name') == column_name:
                if 'sql' in measure:
                    transformed_sql = measure['sql'].replace('${TABLE}.', '')
                    transformed_sql = f"{measure['type']}({transformed_sql}) as {measure['name']}"
                else:
                    transformed_sql = f"{measure['type']}(*) as {measure['name']}"
                return {
                    "view_name": view['name'],
                    "type": "measure",
                    "name": column_name,
                    "sql": transformed_sql
                }

        # Search in transformed dimension groups
        for dim_group in view.get('dimension_groups', []):
            if dim_group.get('type') == 'duration':
                intervals = dim_group.get('intervals', [])
                sql_start = dim_group.get('sql_start', '').strip()
                sql_end = dim_group.get('sql_end', '').strip()

                if '::' in sql_start:
                    sql_start = sql_start.replace('${', '').replace('}', '')
                    first_part, second_part = sql_start.split('::', 1)
                    sql_start = f"CAST({first_part} AS {second_part.upper()})"
                elif '${' in sql_start:
                    sql_start = sql_start.replace('${', '').replace('}', '')

                if '::' in sql_end:
                    sql_end = sql_end.replace('${', '').replace('}', '')
                    first_part, second_part = sql_end.split('::', 1)
                    sql_end = f"CAST({first_part} AS {second_part.upper()})"
                elif '${' in sql_end:
                    sql_end = sql_end.replace('${', '').replace('}', '')

                for interval in intervals:
                    alias_name = f"{dim_group['name']}_{interval}"
                    if alias_name == column_name:
                        transformed_sql = f"TIMESTAMPDIFF({interval}, {sql_end}, {sql_start}) as {alias_name}"
                        return {
                            "view_name": view['name'],
                            "type": "dimension_group",
                            "name": column_name,
                            "sql": transformed_sql
                        }

            elif dim_group.get('type') == 'time':
                timeframes = dim_group.get('timeframes', [])
                sql = dim_group.get('sql', '').replace('${TABLE}.', '')
                for timeframe in timeframes:
                    alias_name = f"{dim_group['name']}_{timeframe}"
                    if alias_name == column_name:
                        transformed_sql = f"EXTRACT({timeframe.upper()} FROM {sql}) as {alias_name}"
                        return {
                            "view_name": view['name'],
                            "type": "dimension_group",
                            "name": column_name,
                            "sql": transformed_sql
                        }

        return None


    def column_search(repo,view_file_path):
        yaml_data=get_all_view(repo,view_file_path)
        # Extract views
        views = yaml_data.get('views', [])
        view_name=[view['name'] for view in views]

        column_name = input('Enter column name: ')

        if not isinstance(views,list):
            views=[views]
        for selected_view in views:
            search_result = search_column_in_view(selected_view, column_name)
            if search_result:
                print("Search Result:", search_result['sql'],'\n','view_name:',search_result['view_name'])
                break
            else:
                print(f"Column '{column_name}' not found in view '{selected_view['name']}'.")

    def get_all_view(repo,folder_path):

        contents = repo.get_contents(folder_path)
        
        concatenated_content = ""
        if not isinstance(contents,list):
            contents=list([contents])
        # Filter and process files
        for content_file in contents:
            if content_file.type == "file" and (content_file.name.endswith(".lkml") or content_file.name.endswith(".lkml")):
                #if any(filter_string in content_file.name for filter_string in filter_strings):
                print(f"Fetching file: {content_file.name}")
                # Decode file content
                file_content = content_file.decoded_content.decode("utf-8").replace('\r','')
                concatenated_content += file_content + "\n"  # Add content with a newline for separation
        return lkml.load(concatenated_content)
        
    def get_repo_details(repo_input):
        if "github.com" in repo_input:
            parts = repo_input.rstrip("/").split("/")
            username, repo_name = parts[-2], parts[-1]
        elif "/" in repo_input:
            username, repo_name = repo_input.split("/", 1)
        else:
            username, repo_name = None, repo_input
        return username, repo_name

    # Authenticate with GitHub
    g = Github(github_token)
    repo_input = repo_link
    username, repo_name = get_repo_details(repo_input)

    if username is None:
        username = g.get_user().login

    try:
        repo = g.get_repo(f"{username}/{repo_name}")
        print(f"Repository: {username}/{repo_name}")
    except Exception as e:
        print(f"Error fetching repository: {e}")
        exit()

    def find_files(repo, path="", keyword="dashboard.lkml"):
        found_files = []
        try:
            contents = repo.get_contents(path)
            for content in contents:
                if content.type == "dir":
                    found_files.extend(find_files(repo, content.path, keyword))
                elif keyword in content.name:
                    found_files.append(content)
        except Exception as e:
            print(f"Error accessing {path}: {e}")
        return found_files
        
    def model_find_files(repo, path="", keyword=["model.lkml", ".explore.lkml"]):
        found_files = []
        try:
            contents = repo.get_contents(path)
            for content in contents:
                if content.type == "dir":
                    found_files.extend(model_find_files(repo, content.path, keyword))
                elif keyword in content.name:
                    found_files.append(content)
        except Exception as e:
            print(f"Error accessing {path}: {e}")
        return found_files


    def view_find_files(repo, path="", keyword=""):
        found_files = []
        try:
            contents = repo.get_contents(path)
            for content in contents:
                if content.type == "dir":
                    found_files.extend(view_find_files(repo, content.path, keyword))
                elif keyword in content.name:
                    found_files.append(content.path)  # Append only the file path
        except Exception as e:
            print(f"Error accessing {path}: {e}")
        return found_files


    def parse_dashboard(dashboard):
        details = {"dashboard_title": "", "elements": [], "filters": []}
        if isinstance(dashboard, list):
            for item in dashboard:
                details = parse_dashboard(item)
                return details
        elif isinstance(dashboard, dict):
            details["dashboard_title"] = dashboard.get("title", "")
            if "elements" in dashboard:
                for element in dashboard["elements"]:
                    details["elements"].append({
                        "title": element.get("title", ""),
                        "name": element.get("name", ""),
                        "model": element.get("model", ""),
                        "explore": element.get("explore", ""),
                        "fields": element.get("fields", []),
                        "filters": element.get("filters", {}),
                        "sorts": element.get("sorts", []),
                        "limit": element.get("limit", ""),
                    })
            if "filters" in dashboard:
                for filter_item in dashboard["filters"]:
                    details["filters"].append({
                        "name": filter_item.get("name", ""),
                        "title": filter_item.get("title", ""),
                        "type": filter_item.get("type", ""),
                        "default_value": filter_item.get("default_value", ""),
                        "allow_multiple_values": filter_item.get("allow_multiple_values", False),
                        "required": filter_item.get("required", False),
                        "field": filter_item.get("field", ""),
                    })
        return details

    def ensure_folders(repo):
        base_folder = "Processed_files/Converted_Dashboard"
        archive_folder = f"{base_folder}/archive"

        try:
            repo.get_contents(base_folder)
        except Exception:
            repo.create_file(f"{base_folder}/.placeholder", "Create folder", "")

        try:
            repo.get_contents(archive_folder)
        except Exception:
            repo.create_file(f"{archive_folder}/.placeholder", "Create archive folder", "")

        return base_folder, archive_folder

    #     return SQL
    def generate_sql(element):
        # Skip elements without a model or title
        if not element.get("model") or not element.get("title"):
            return None

        # Start building the SELECT statement
        sql = f"\n"
        # Process filters with '-' prefix as NOT EQUAL
        filters = []
        for k, v in element.get("filters", {}).items():
            if str(v).startswith("-"):
                filters.append(f"{k} != '{v[1:]}'")  # Remove '-' prefix for NOT EQUAL
            else:
                filters.append(f"{k} = '{v}'")

        # Add WHERE clause if filters exist
        if filters:
            sql += "WHERE " + " AND ".join(filters) + "\n"

        # Add ORDER BY clause if sorts exist
        if element.get("sorts"):
            sql += "ORDER BY " + ", ".join(element["sorts"]) + ";"


        return sql



    def archive_old_files(repo, base_folder, archive_folder):
        """
        Moves existing files from the base folder to the archive folder.
        """
        print()
        try:
            # Check if the archive folder exists; create it if not
            try:
                repo.get_contents(archive_folder)
            except Exception as e:
                print(f"Archive folder does not exist. Creating it. Details: {e}")
                repo.create_file(f"{archive_folder}/.placeholder", "Create archive folder", "")
                print(f"Archive folder created: {archive_folder}")

            # Get contents of the base folder
            contents = repo.get_contents(base_folder)
            for content in contents:
                if content.type == "file" and not content.path.endswith(".placeholder"):
                    # Get the SHA of the file
                    sha = content.sha
                    print(f"Found file: {content.path} with SHA: {sha}")

                    # Generate archive file name with current datetime
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    archive_path = f"{archive_folder}/{timestamp}_{content.name}"

                    # Move the file to the archive folder
                    repo.create_file(archive_path, f"Archived old file {content.path}", content.decoded_content.decode())
                    print(f"Archived: {content.path} -> {archive_path}")

                    # Delete the original file
                    repo.delete_file(content.path, f"Moved {content.path} to archive", sha)
                    print(f"Deleted: {content.path}")
        except Exception as e:
            print(f"Error archiving files: {e}")




    def write_dashboard_to_file(repo, base_folder, dashboard_title, content):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{dashboard_title}_{timestamp}.txt"
        file_path_in_repo = f"{base_folder}/{file_name}"
        try:
            repo.create_file(file_path_in_repo, f"Add {file_name}", content)
            print(f"Created file: {file_name}")
        except Exception as e:
            print(f"Error creating file {file_path_in_repo}: {e}")




    def load_lookml_model(file_path):
        """Load the LookML model file and return the parsed YAML data."""
        
        try:
            
            file_content = repo.get_contents(file_path)
            lookml_content = file_content.decoded_content.decode("utf-8")
            # Parse LookML content
            parsed_data = lkml.load(lookml_content)
            return yaml.safe_load(yaml.dump(parsed_data, sort_keys=False))
        except Exception as e:
            print(f"Error loading LookML model from {file_path}: {e}")
            return None


    def column_search(repo,view_file_path):
        yaml_data=get_all_view(repo,view_file_path)
        # Extract views
        views = yaml_data.get('views', [])
        view_name=[view['name'] for view in views]
        

        column_name = input('Enter column name: ')
        
        if not isinstance(views,list):
            views=[views]
        for selected_view in views:
            search_result = search_column_in_view(selected_view, column_name)
            if search_result:
                print("Search Result:", search_result['sql'],'\n','view_name:',search_result['view_name'])
                break
            else:
                print(f"Column '{column_name}' not found in view '{selected_view['name']}'.")



    def generate_sql_and_trigger_info(explore, datagroups):
        """Generate a well-formatted SQL query for a given explore and trigger information."""
        base_query = f"\nFROM {explore['name']}"
        for join in explore.get('joins', []):
            join_type = join['type'].upper()
            join_table = join['name']
            sql_on = join['sql_on'].replace('${', '').replace('}', '')
            base_query += f"\n\t{join_type} JOIN {join_table} ON {sql_on}"
        
        trigger_info = "No trigger defined"
        if 'persist_with' in explore:
            datagroup_name = explore['persist_with']
            if datagroup_name in datagroups:
                trigger_info = f"Trigger Type: {datagroup_name} (Persist for {datagroups[datagroup_name]})"
        
        return {
            "explore": explore['name'],
            "sql_query": base_query ,
            "trigger_info": trigger_info
        }
        

    def get_sql_and_trigger_info(file_path, explore_name=None):
        """Generate SQL queries and trigger info for explores from a GitHub LookML file."""
        model_data = load_lookml_model(file_path)
        if not model_data:
            return None
        
        datagroups = {dg['name']: dg['persist_for'] for dg in model_data.get('datagroups', [])}
        if explore_name:
            explore = next((e for e in model_data['explores'] if e['name'] == explore_name), None)
            if explore:
                return generate_sql_and_trigger_info(explore, datagroups)
            else:
                return f"Explore '{explore_name}' not found in the model."
        
        result = []
        for explore in model_data.get('explores', []):
            result.append(generate_sql_and_trigger_info(explore, datagroups))
        return result

    # User input handling for both SQL query and trigger info
    def user_input(model_files,explore = None):

        file_path = model_files
        explore_name = explore
        model_details = []

        if explore_name:
            result = get_sql_and_trigger_info(file_path, explore_name)
            if isinstance(result, dict):  # If the result is a dict, it's a single explore's info
                sql_query = f"{result['sql_query']}"
                trigger_info = f"\nTrigger Information: {result['trigger_info']}"
                model_details.append(sql_query)
                model_details.append(trigger_info)
                # print(f"\nSQL Query for explore '{explore_name}':\n{result['sql_query']}")
                # print(f"\nTrigger Information: {result['trigger_info']}")
                
                return model_details
            else:
                print(result)  # Explore not found
        else:
            result = get_sql_and_trigger_info(file_path)
            return result

    def generate_sql_queries(views):
        """Generate SQL queries for each view."""
        queries = []
        for view in views:
            dimensions = view.get('dimensions', [])
            measures = view.get('measures', [])
            dimension_groups = view.get('dimension_groups', [])
            table_name = view.get('sql_table_name', 'unknown_table').replace('`','')  # Default if no table name

            # Check if the view has a derived_table
            dt = []
            derived_table = view.get('derived_table', None)
            if derived_table:
                # Construct the derived table query
                derived_sql = derived_table.get('sql', '')  # Get SQL of the derived table
                dt = derived_table
                derived_query = f"({derived_sql})"
                query_base = f"WITH cte_{view['name']} AS {derived_query}"
                # Set the table name to be the view name for the SELECT statement
                table_name = 'cte_'+view['name']
            else:
                query_base = ""

            # Transform dimensions, measures, and dimension groups SQL
            transformed_dimensions = [
                dim['sql'].replace('${TABLE}.', '') + f" as {dim['name']}" for dim in dimensions if 'sql' in dim
            ]
            transformed_measures = [
                (f"{measure['type']}({measure['sql'].replace('${TABLE}.', '')}) as {measure['name']}" if 'sql' in measure
                else f"{measure['type']}(*) as {measure['name']}")
                for measure in measures
            ]

            transformed_dimension_groups = []
            for dim_group in dimension_groups:
                if dim_group.get('type') == 'duration':
                    intervals = dim_group.get('intervals', [])
                    sql_start = dim_group.get('sql_start', '').strip()
                    sql_end = dim_group.get('sql_end', '').strip()

                    if '::' in sql_start:
                        sql_start = sql_start.replace('${', '').replace('}', '')
                        first_part, second_part = sql_start.split('::', 1)
                        sql_start = f"CAST({first_part} AS {second_part.upper()})"
                    else:
                        if '${' in sql_start:
                            sql_start = sql_start.replace('${', '').replace('}', '')

                    if '::' in sql_end:
                        sql_end = sql_end.replace('${', '').replace('}', '')
                        first_part, second_part = sql_end.split('::', 1)
                        sql_end = f"CAST({first_part} AS {second_part.upper()})"
                    else:
                        if '${' in sql_end:
                            sql_end = sql_end.replace('${', '').replace('}', '')

                    for interval in intervals:
                        transformed_dimension_groups.append(
                            f"TIMESTAMPDIFF({interval}, {sql_end}, {sql_start}) as {dim_group['name']}_{interval}"
                        )
                elif dim_group.get('type') == 'time':
                    timeframes = dim_group.get('timeframes', [])
                    sql = dim_group.get('sql', '').replace('${TABLE}.', '')
                    for timeframe in timeframes:
                        transformed_dimension_groups.append(
                            f"EXTRACT({timeframe.upper()} FROM {sql}) as {dim_group['name']}_{timeframe}"
                        )

            # Generate SQL query
            select_clause = ",\n\t".join(transformed_dimensions + transformed_dimension_groups + transformed_measures)
            dimension_and_group_count = len(transformed_dimensions + transformed_dimension_groups)
            group_by_clause = ", ".join(str(i) for i in range(1, dimension_and_group_count + 1))
            query = f"{query_base} SELECT \n\t{select_clause} \nFROM {table_name}"
            if group_by_clause:
                query += f"\n GROUP BY {group_by_clause}"
            queries.append({"view_name": view['name'], "query": query, 'dt': dt})

        return queries

    def generate_sql_query(explore):

        """Generate a well-formatted SQL query for a given explore."""

        # Determine the base table name

        base_table = explore.get('name')
        tables=[]
        #tables.append(explore['name'])
        # Check for specific keywords to customize table name

        if 'from' in explore:

            base_table = explore['from']

            base_table_alias = explore['name']
            #tables.append(base_table)

            base_query = f"SELECT *\nFROM {base_table} AS {base_table_alias}"

        elif 'view_name' in explore:

            base_table = explore['view_name']
            #tables.append(base_table)

            base_query = f"SELECT *\nFROM {base_table}"

        else:

            base_query = f"SELECT *\nFROM {base_table}"
        tables.append(base_table)
    
        # Add JOIN clauses

        for join in explore.get('joins', []):

            join_type = join['type'].upper()

            join_table = join['name']
            tables.append(join['name'])

            sql_on = join['sql_on']
    
            # Remove `${}` to make it compatible with MySQL

            sql_on = sql_on.replace('${', '').replace('}', '')
    
            # Construct JOIN clause with indentation

            base_query += f"\n  {join_type} JOIN {join_table} ON {sql_on}"
    
        # need to check for the code 
        
        return base_query + ";",tables

    def get_model_view(view_name,f):
        views_name = view_name
        if isinstance(view_name, dict):
            # Wrap the dictionary in a list
            views_name=list([view_name])
        elif isinstance(view_name, list):
            # Return the input as is, assuming it is already a list of dictionaries
            views_name=view_name
            
        sql_queries = generate_sql_queries(views_name)
        
        DT_keys = ['sql_trigger_value', 'interval_trigger', 'datagroup_trigger', 'persist_for']
        flag = 1
        for query in sql_queries:
            comment=''
            if query['dt']:
                for key in DT_keys:
                    if key in query['dt']:
                        comment+=f"-- This is PDT and {key}: {query['dt'][key]}"
            if f==0:
                return f"Explore: {query.get('view_name','')}\n\n with {query.get('view_name','')} as (\n{comment}\n{query['query']}\n)"

            else:
                return f'{query.get('view_name','')} as (\n{comment}\n{query['query']}\n)'
        
    

    def get_model_view_qry(model_file_path, v_data, repo):
        data = get_all_view(repo, model_file_path)
        if not data:
            return

        # Initialize the view data (empty initially)
        views_names = []
        views_keys = {}
        explores_names = []
        explores_keys = {}

        # Fetch the explore names and their data
        for explore_name in data.get('explores', []):

            explores_names.append(explore_name.get('name', []))
            explores_keys[explore_name.get('name', '')] = explore_name
        
        
        # Ensure v_data is initialized and contains valid view information
        if not v_data:
            print("No valid view data found.")
            return
            
        # Get the view names and keys
        for view in v_data.get('views', []):
            views_names.append(view.get('name', []))
            views_keys[view.get('name', '')] = view

        result_list = []
        er = []
        er1 = []
        for ex_name in list(set(explores_names)):
            sql_query, table_list = generate_sql_query(explores_keys.get(ex_name))
            table_list, relationships = extract_relationships(sql_query)
            dot_output = generate_dot(table_list, relationships)
            mermaid_er = generate_mermaid_er(table_list, relationships)
            er.append( {'exp_name': ex_name,'er':dot_output})
            er1.append( {'exp_name': ex_name,'er':mermaid_er})
            result = ''
            for i in range(len(table_list)):
                cte = get_model_view(views_keys.get(table_list[i], ''), i)
                if cte is not None:
                    if i == len(table_list) - 1:
                        result += cte
                    else:
                        result += cte + ',\n'

            # Append the result with the SQL query
            result_list.append(f'{result}\n{sql_query}')

        return result_list,er,er1


    def model_archive_old_files(repo, base_folder, archive_folder):
        """
        Moves existing files from the base folder to the archive folder.
        """
        try:
            # Check if the archive folder exists; create it if not
            try:
                repo.get_contents(archive_folder)
            except Exception as e:
                repo.create_file(f"{archive_folder}/.placeholder", "Create archive folder", "")

            # Get contents of the base folder
            contents = repo.get_contents(base_folder)
            for content in contents:
                if content.type == "file" and not content.path.endswith(".placeholder"):
                    # Get the SHA of the file
                    sha = content.sha
                    # Generate archive file name with current datetime
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    archive_path = f"{archive_folder}/{timestamp}_{content.name}"

                    # Move the file to the archive folder
                    repo.create_file(archive_path, f"Archived old file {content.path}", content.decoded_content.decode())

                    # Delete the original file
                    repo.delete_file(content.path, f"Moved {content.path} to archive", sha)
        except Exception as e:
            print(f"Error archiving files: {e}")

    def model_ensure_folders(repo):
        base_folder = "Processed_files/Extracted_Model"
        archive_folder = f"{base_folder}/archive"

        try:
            repo.get_contents(base_folder)
        except Exception:
            repo.create_file(f"{base_folder}/.placeholder", "Create folder", "")

        try:    
            repo.get_contents(archive_folder)
        except Exception:
            repo.create_file(f"{archive_folder}/.placeholder", "Create archive folder", "")

        return base_folder, archive_folder

    def mod_model_find_files(repo, path="", keyword="model.lkml"):
        print("entered into model search")
        found_files = []
        try:
            print("path",path)
            contents = repo.get_contents(path)
            for content in contents:
                if content.type == "dir":
                    found_files.extend(mod_model_find_files(repo, content.path, keyword))
                elif keyword in content.name:
                    found_files.append(content.path)  # Append only the file path
        except Exception as e:
            print(f"Error accessing {path}: {e}")
        return found_files

    def mod_view_find_files(repo, path="", keyword="view.lkml"):
        found_files = []
        try:
            contents = repo.get_contents(path)
            for content in contents:
                if content.type == "dir":
                    found_files.extend(mod_view_find_files(repo, content.path, keyword))
                elif keyword in content.name:
                    found_files.append(content.path)  # Append only the file path
        except Exception as e:
            print(f"Error accessing {path}: {e}")
        return found_files



    def extract_relationships(sql):
        tables = re.findall(r'from\s+(\w+)|join\s+(\w+)', sql, re.IGNORECASE)
        relationships = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', sql, re.IGNORECASE)
        
        table_list = [tbl[0] if tbl[0] else tbl[1] for tbl in tables]
        return table_list, relationships
    def generate_mermaid_er(table_list, relationships):
        mermaid_er = ["erDiagram"]

        # Create a dictionary to store columns for each table
        table_columns = {table.capitalize(): set() for table in table_list}
        
        # Populate columns from the relationships
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            source_column = rel[1]
            target_column = rel[3]
            try:
                table_columns[source_table].add(source_column)
                table_columns[target_table].add(target_column)
            except KeyError:
                print(f"Warning: Source table '{source_table}' not found in table list.")
                
        # Generate table definitions
        for table, columns in table_columns.items():
            columns_str = "\n        ".join(f"string {col}" for col in columns)  # Assuming all columns as strings
            mermaid_er.append(f"    {table} {{\n        {columns_str}\n    }}")

        # Adding relationships with the appropriate notation
        mermaid_er.append("\n")
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            source_field = rel[1]
            target_field = rel[3]
            
            # Determine cardinality (assuming many-to-one or one-to-many for simplicity)
            if source_field.endswith("_id"):
                cardinality = "||--o{"
            else:
                cardinality = "o{--||"
            
            mermaid_er.append(f"    {source_table} {cardinality} {target_table} : {source_field}")

        return "\n".join(mermaid_er)

    def generate_dot(table_list, relationships):
        dot = ['digraph ERDiagram {', '        node [shape=record, style=filled, fillcolor=lightblue, color=black, fontname=Helvetica, fontsize=10]; \n        edge [color=black, arrowhead=vee, penwidth=1.0, fontname=Helvetica, fontsize=10];', '']        
        
        # Create a dictionary to store columns for each table
        table_columns = {table.capitalize(): set() for table in table_list}
        
        # Populate columns from the relationships
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            source_column = rel[1]
            target_column = rel[3]
            try:
                table_columns[source_table].add(source_column)
                table_columns[target_table].add(target_column)
            except KeyError:
                print(f"Warning: Source table '{source_table}' not found in table list.")
        # Generate the DOT nodes with dynamic columns
        for table, columns in table_columns.items():
            columns_str = '|'.join(f'{col}' for col in columns)
            dot.append(f'    {table} [label="{{{table}|{columns_str}}}"];')
        
        dot.append('\n    ')
        
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            field = rel[1]
            dot.append(f'    {source_table} -> {target_table} [label="{field}", arrowtail="crow", arrowhead="dot", dir="both"];')
        
        dot.append('}')
        
        return '\n'.join(dot)


    def write_model_to_file(repo, f_name, content):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"Sql_query_for_{f_name}_{timestamp}.txt"
        file_path_in_repo = f"Processed_files/Extracted_Model/{file_name}"

        # Prepare the content to be written
        exp_content = "\n"
        for exp in content:
            exp_content += f"\n{exp}\n"
        
        # Create the new file in the main folder
        try:
            repo.create_file(file_path_in_repo, f"Add {file_name}", exp_content)
            print(f"Created file: {file_name}")
        except Exception as e:
            print(f"Error creating file {file_path_in_repo}: {e}")


        
    def convert_lkml_to_yaml(repo,lkml_contents):
        """Convert LKML content to YAML format."""
        try:
            file_content = repo.get_contents(lkml_contents)
            lookml_content = file_content.decoded_content.decode("utf-8").replace('\r','')
            parsed_lkml = lkml.load(lookml_content)
            return parsed_lkml
        except Exception as e:
            print(e)
            return
            
    # Main processing
    base_folder, archive_folder = ensure_folders(repo)
    archive_old_files(repo, base_folder, archive_folder)

    files = find_files(repo, keyword="dashboard.lkml")
    files=[]

    if not files:
        print("No files with 'dashboard.lkml' found.")
    else:
        for file in files:
            file_path = file.path
            content = file.decoded_content.decode()

            try:
                dashboard = yaml.safe_load(content)
                dashboard_details = parse_dashboard(dashboard)

                dashboard_content = f"Dashboard Title: {dashboard_details['dashboard_title']}\n"
                dashboard_content += f"Source File Path: {file_path}\n\n"
                for element in dashboard_details["elements"]:
                    sql_query = generate_sql(element)
                    if sql_query:  
                        # distinct_model_names.add(element['model'])
                        # distinct_explore_names.add(element['explore'])
                        model_file = model_find_files(repo, keyword = f"{element['model']}.model.lkml")
                        if not model_file:
                            print("No files with 'model.lkml' found.")
                        else:
                            for file in model_file:
                                file_path = file.path
                            model_return = user_input(file_path,element['explore'])
                            sql_model = model_return[0]
                            trigger_details = model_return[-1]

                        
                        # dashboard_content += f" This Dashboard is built on this Model: {element['model']}\n"
                        dashboard_content += (
                            "\n\n\n***************************************************************\n"
                            f"  Title: {element['title']}\n"
                            f"  Name: {element['name']}\n"
                            f"  Explore: {element['explore']}\n"
                            f"  Fields: {', '.join(element['fields']) if element['fields'] else 'None'}\n"
                            f"  Filters: {', '.join([f'{k}: {v}' for k, v in element['filters'].items()]) if element['filters'] else 'None'}\n"
                            f"  Sorts: {', '.join(element['sorts']) if element['sorts'] else 'None'}\n"
                            "  SQL Query:\n"
                            f"    SELECT {', '.join(element.get('fields', [])) if element.get('fields') else '*'}\n"
                            f"    {sql_model.strip()}\n"
                            f"    {sql_query.strip()}\n"
                        )
                        print("dashboard_content:",dashboard_content)
                
                
                
                write_dashboard_to_file(repo, base_folder, dashboard_details["dashboard_title"], dashboard_content)

            except Exception as e:
                print(f"Error parsing LookML content from {file_path}: {e}")

    model_file_paths = mod_model_find_files(repo, keyword="model.lkml")
    view_file_paths = mod_view_find_files(repo, keyword="view.lkml")


    base_folder1, archive_folder1 = model_ensure_folders(repo)
    model_archive_old_files(repo, base_folder1, archive_folder1)

    er_digram = []
    v_check=0
    v_data=''
    er_file_path = f"Processed_files/Extracted_Model/"
    for view_file_path in view_file_paths:
        current_v_data = get_all_view(repo, view_file_path)
        if current_v_data:
            if v_check==0:
                v_check=1
                v_data = current_v_data  # Update v_data only if view data is found
            else:
                v_data.get('views',[]).extend(current_v_data.get('views',[]))
        else:
            print(f"Warning: No data found for view file '{view_file_path}'")

    for model_file_path in model_file_paths:
        print(model_file_path)
        try:
            QRY,er,er1=get_model_view_qry(model_file_path,v_data,repo)
            match = re.search(r'([^/]+)\.model\.lkml$', model_file_path)
            match = match.group(1)
            model_result = {'Name': match,'file_path':f"{er_file_path}{match}",'er':er, 'er1': er1}
            er_digram.append(model_result)
            write_model_to_file(repo,match,QRY)
        
        except Exception as e:
            print(f" Error on this file {model_file_path}: {e}")

    return er_digram,github_token,repo_link


@app.route('/process_file', methods=['POST'])
def process_file():
    try:
        repo_link = request.form.get('repo_link')
        github_token = request.form.get('github_token')

        if not repo_link or not github_token:
            return jsonify({'error': 'GitHub Repo Link and Token are required'}), 400


        er_digram,github_token,repo_link = process_lookml(repo_link,github_token)

        return jsonify({
            'er_digram': er_digram,
            'github_token': github_token,
            'repo_link': repo_link
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GitHub Details</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f4f4f9;
            margin: 0;
            padding: 0;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
        }
        .form-container {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            width: 400px;
        }
        h1 {
            text-align: center;
            color: #333;
        }
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: bold;
            color: #555;
        }
        input[type="text"],
        input[type="file"],
        button {
            width: 100%;
            padding: 10px;
            margin-bottom: 15px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 16px;
        }
        input[type="text"]:focus,
        input[type="file"]:focus {
            border-color: #007bff;
            outline: none;
        }
        button {
            background-color: #007bff;
            color: #fff;
            border: none;
            cursor: pointer;
        }
        button:hover {
            background-color: #0056b3;
        }
        .response-container {
            margin-top: 20px;
            padding: 10px;
            background-color: #f9f9f9;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            color: #333;
            display: none;
            text-align: center;
        }
        .loading {
            display: none;
            margin: 20px 0;
        }
        .loading img {
            width: 50px;
            height: 50px;
        }
    </style>
</head>
<body>
    <div class="form-container">
        <h1>Submit GitHub Details</h1>
        <form id="github-form">
            <label for="repo-link">GitHub Repo Link:</label>
            <input type="text" id="repo-link" name="repo_link" placeholder="Enter GitHub repository link" required>

            <label for="github-token">GitHub Token:</label>
            <input type="text" id="github-token" name="github_token" placeholder="Enter GitHub token" required>


            <button type="button" id="submit-btn">Submit</button>
        </form>
        <div class="loading" id="loading">
            <img src="https://i.gifer.com/YCZH.gif" alt="Loading...">
            <p>Processing your request...</p>
        </div>
        <div class="response-container" id="response-container"></div>
    </div>

    <script>
        document.getElementById('submit-btn').addEventListener('click', async function () {
            const form = document.getElementById('github-form');
            const formData = new FormData(form);

            const loading = document.getElementById('loading');
            const responseContainer = document.getElementById('response-container');

            // Show the loading animation
            loading.style.display = 'block';
            responseContainer.style.display = 'none';

            try {
                const response = await fetch('/process_file', {
                    method: 'POST',
                    body: formData,
                });

                const data = await response.json();

                // Hide the loading animation and display the result
                loading.style.display = 'none';
                responseContainer.style.display = 'block';

                if (response.ok) {
                    responseContainer.style.color = 'green';
                    responseContainer.textContent = JSON.stringify(data, null, 2);
                } else {
                    responseContainer.style.color = 'red';
                    responseContainer.textContent = JSON.stringify(data, null, 2);
                }
            } catch (error) {
                // Hide the loading animation and display the error
                loading.style.display = 'none';
                responseContainer.style.display = 'block';
                responseContainer.style.color = 'red';
                responseContainer.textContent = 'An error occurred: ' + error.message;
            }
        });
    </script>
</body>
</html>

    '''


def handler(event, context):
    return awsgi.response(app, event, context)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
