import awsgi
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from google.cloud import secretmanager
from openai import OpenAI
import json
import re

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
apikey_dict = json.loads(api_key)
api_key_main = apikey_dict['api_key']
client = OpenAI(api_key=api_key_main)


def process_powerbi(file_content):
    def schema_chk(uniquecount, org_tbl):
        if uniquecount > 1:
            return snowflake(org_tbl)
        else:
            return star(org_tbl)
    def extract_relationships(sql):
        tables = re.findall(r'from\s+(\w+)|join\s+(\w+)', sql, re.IGNORECASE)
        relationships = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', sql, re.IGNORECASE)
        
        table_list = [tbl[0] if tbl[0] else tbl[1] for tbl in tables]
        return table_list, relationships

    def generate_dot(table_list, relationships):
        dot = ['digraph ERDiagram {', '    node [shape=record];', '']
        
        # Create a dictionary to store columns for each table
        table_columns = {table.capitalize(): set() for table in table_list}
        
        # Populate columns from the relationships
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            source_column = rel[1]
            target_column = rel[3]
            
            table_columns[source_table].add(source_column)
            table_columns[target_table].add(target_column)
        
        # Generate the DOT nodes with dynamic columns
        for table, columns in table_columns.items():
            columns_str = '|'.join(f'{col}' for col in columns)
            dot.append(f'    {table} [label="{{{table}|{columns_str}}}"];')
        
        dot.append('\n    // Relationships')
        
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            field = rel[1]
            dot.append(f'    {source_table} -> {target_table} [label="{field}"];')
        
        dot.append('}')
        
        return '\n'.join(dot)

        dot = ['digraph ERDiagram {', '    node [shape=record];', '']
        
        for table in table_list:
            dot.append(f'    {table.capitalize()} [label="{{{table.capitalize()}|id: int}}"];')
        
        dot.append('\n    // Relationships')
        
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            field = rel[1]
            dot.append(f'    {source_table} -> {target_table} [label="{field}"];')
        
        dot.append('}')
        
        return '\n'.join(dot)


    def snowflake(org_tbl):
        global result_df
        result_df = pd.DataFrame({'group_value': org_tbl.groupby('fromTable').size().index,
                                  'count': org_tbl.groupby('fromTable').size().values})
        result_df = result_df.sort_values(by=['count'], ascending=False)
        result_df['row_number'] = range(1, len(result_df) + 1)
        filtered_data = result_df[result_df['row_number'] == 1]
        tbl_name = filtered_data['group_value'].iloc[0]
        return sql(tbl_name, 1)

    def star(org_tbl):
        result_df = pd.DataFrame({'group_value': org_tbl.groupby('fromTable').size().index})
        tbl_name = result_df['group_value'].iloc[0]
        return sql(tbl_name.strip().replace(' ', '_'))

    def sql(fact_name, i=0):
        global result_df
        join_exp1 = ""
        if i == 1:
            fromTablename = fact_name.strip().replace(' ', '_')
            exp_v2 = sql_exp_begin(fromTablename)
            for i in result_df['row_number'].iloc[:-1]:
                filtered_data = result_df[result_df['row_number'] == i]
                tbl_name = filtered_data['group_value'].iloc[0]
                tbl_detail = org_tbl[org_tbl['fromTable'].str.contains(tbl_name)]
                tbl_detail = tbl_detail.reset_index(drop=True)
                for j in range(len(tbl_detail)):
                    mfromTablename = tbl_detail['fromTable'][j]
                    toTablename = tbl_detail['toTable'][j]
                    fromColumnname = tbl_detail['fromColumn'][j]
                    toColumnname = tbl_detail['toColumn'][j]
                    if fromTablename == toTablename:
                        toTablename = toTablename + "_temp_alias"
                    join_exp = sql_join(mfromTablename.strip().replace(' ', '_'), toTablename.strip().replace(' ', '_'),
                                        fromColumnname.strip().replace(' ', '_'),
                                        toColumnname.strip().replace(' ', '_'))
                    join_exp1 += join_exp
            return exp_v2 + join_exp1 + ";\n"
        else:
            tbl_detail = org_tbl[org_tbl['fromTable'].str.contains(fact_name)]
            tbl_detail = tbl_detail.reset_index(drop=True)
            exp_v2 = sql_exp_begin(fact_name)
            for i in range(len(tbl_detail)):
                fromTablename = tbl_detail['fromTable'][i]
                toTablename = tbl_detail['toTable'][i]
                fromColumnname = tbl_detail['fromColumn'][i]
                toColumnname = tbl_detail['toColumn'][i]
                join_exp = sql_join(fromTablename.strip().replace(' ', '_'), toTablename.strip().replace(' ', '_'),
                                    fromColumnname.strip().replace(' ', '_'), toColumnname.strip().replace(' ', '_'))
                join_exp1 += join_exp
            return exp_v2 + join_exp1 + ";\n"

    def sql_exp_begin(fromTablename):
        return f"select * from {fromTablename}"

    def sql_join(mfromTablename, toTablename, fromColumnname, toColumnname):
        if '_temp_alias' in toTablename:
            join_v1 = f"\n left join {toTablename} \t as {toTablename.replace('_temp_alias', '')}\t  # Note.. it only happens only on self join \n  on {mfromTablename}.{fromColumnname} = {toTablename}.{toColumnname}"
        else:
            join_v1 = f"\n left join {toTablename} \n on {mfromTablename}.{fromColumnname} = {toTablename}.{toColumnname}"
        return join_v1

    def dax_sql(text):
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "you are a helpful assistant."
                },
                {
                    "role": "user",
                    "content": f"Convert the respective PowerBI measure to MySQL statement.{text} Provide me only the sql Query as output."
                }
            ]
        )
        summary = response.choices[0].message.content
        return summary

    def measure_fetch(cleaned_measures):
        measure_list = []
        for i in cleaned_measures:
            for j in i:
                measure_list.append(j)
        df1 = pd.DataFrame(measure_list).reset_index(drop=True)
        measure = df1[df1.columns[0:2]]
        measure = measure.copy()  
        measure['mysql_query'] = measure['expression'].apply(dax_sql)
        return measure

    df = pd.read_json(file_content)
    relationship = pd.DataFrame(df.loc['relationships']['model'])
    org_tbl = relationship[~relationship['toTable'].str.contains('LocalDate')].reset_index(drop=True)
    fct_tbl_count = org_tbl['fromTable'].nunique()
    cal_measures = pd.DataFrame(df.loc['tables']['model'])
    cleaned_measures = cal_measures['measures'].dropna().reset_index(drop=True)
    measure_result = measure_fetch(cleaned_measures)
    measure_result = measure_result.to_dict()
    model_result = schema_chk(fct_tbl_count,org_tbl)
    sql_query = model_result
    table_list, relationships = extract_relationships(sql_query)
    dot_output = generate_dot(table_list, relationships)
    model_result = {'modelquery': model_result,'er':dot_output}
    main_op = []
    main_op.append(model_result)
    main_op.append(measure_result)
    return (main_op)


def process_lookml(file_content):
    def extract_relationships(sql):
        tables = re.findall(r'from\s+(\w+)|join\s+(\w+)', sql, re.IGNORECASE)
        relationships = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', sql, re.IGNORECASE)
        
        table_list = [tbl[0] if tbl[0] else tbl[1] for tbl in tables]
        return table_list, relationships

    def generate_dot(table_list, relationships):
        dot = ['digraph ERDiagram {', '    node [shape=record];', '']
        
        # Create a dictionary to store columns for each table
        table_columns = {table.capitalize(): set() for table in table_list}
        
        # Populate columns from the relationships
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            source_column = rel[1]
            target_column = rel[3]
            
            table_columns[source_table].add(source_column)
            table_columns[target_table].add(target_column)
        
        # Generate the DOT nodes with dynamic columns
        for table, columns in table_columns.items():
            columns_str = '|'.join(f'{col}' for col in columns)
            dot.append(f'    {table} [label="{{{table}|{columns_str}}}"];')
        
        dot.append('\n    // Relationships')
        
        for rel in relationships:
            source_table = rel[0].capitalize()
            target_table = rel[2].capitalize()
            field = rel[1]
            dot.append(f'    {source_table} -> {target_table} [label="{field}"];')
        
        dot.append('}')
        
        return '\n'.join(dot)

    lookml_content = file_content.decode('utf-8')
    lookml_content = lookml_content.replace('{','').replace('}','').replace('$','').replace(';;','')
    # Splitting the content into lines
    lines = lookml_content.strip().split('\n')
    # Initializing a dictionary to hold the result
    result = {}
    data = []
    current_value = []
    model = []
    views = []
    base = ''
    # Iterating over each line and extracting key-value pairs
    join_type_ls= []
    sql_on_ls = []
    join_ls = []
    join_type= ''
    sql_on = ''
    join = ''
    view = []
    name = []
    types = []
    calculation = []
    explore=[]
    sql_conversion = []
    for line in lines:
        if len(line.strip()) != 0:
            check = line.split(': ')[0].strip()
            value = line.split(': ')[1].strip()
            if check == 'view':
                view_name = value
            if check == 'measure':
                name.append(value)
                view.append(view_name)
            if check == 'explore':
                for i in range(0,len(join_ls)):
                    base =  base + join_type_ls[i] + join_ls[i] + sql_on_ls[i]
                model.append(base)
                join_type_ls= []
                sql_on_ls = []
                join_ls = []
                select =  f"select * from {value}"
                explore.append(value)
                base = select
            if check == 'join':
                join =  f" {value}"
                join_ls.append(join)
            if check == 'sql_on':
                sql_on =  f" on {value}"
                sql_on_ls.append(sql_on)
            if check == 'type':
                if value == 'inner':
                    join_type =  f"\n {value} join"
                if value == 'full_outer':
                    join_type =  f"\n full join"
                if value == 'left_outer':
                    join_type =  f"\n left join"
                if value =='sum':
                    types.append(value)
                if value == 'count':
                    types.append(value)
                    calculation.append("*")
                    a = "count(*)"
                    sql_conversion.append(a)
                if value == 'average':
                    types.append(value)
                if value == 'count_distinct':
                    types.append(value)
                join_type_ls.append(join_type)
            if check == 'sql':
                if len(types) > 0:
                    if types[-1] =='sum':
                        calculation.append(value)
                        a = "sum("+value+")"
                        sql_conversion.append(a)
                    if types[-1] == 'count':
                        calculation.append(value)
                        a = "count("+value+")"
                        sql_conversion.append(a)
                    if types[-1] == 'average':
                        calculation.append(value)
                        a = "avg("+value+")"
                        sql_conversion.append(a)
                    if types[-1] == 'count_distinct':
                        calculation.append(value)
                        a = "count( Distinct "+value+")"
                        sql_conversion.append(a)
                
            
    for i in range(0,len(join_ls)):
        base =  base + join_type_ls[i] + join_ls[i] + sql_on_ls[i]
    model.append(base)

    dict = {'Table': view,'measure_name': name, 'type': types, 'calculation': calculation, 'sql_conversion': sql_conversion} 
    looker_measure = pd.DataFrame(dict)
    model = model[1:]
    mod = []
    for mod1 in model:
        amod = mod1+";"
        mod.append(amod)
    model = mod
    er = []
    for m in model:
        sql_query = m
        table_list, relationships = extract_relationships(sql_query)
        dot_output = generate_dot(table_list, relationships)
        er.append(dot_output)
    return model, looker_measure.to_dict(orient='records'), er, explore

@app.route('/process_file', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if file:
        file_content = file.read()
        model, looker_measure, er,explore = process_lookml(file_content)
        return jsonify({'explore_name':explore,'model': model, 'looker_measure': looker_measure, 'er': er})


@app.route('/file_process', methods=['POST'])
def file_process():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if file:
        file_content = file.read().decode('utf-8')
        result = process_powerbi(file_content)
        return ({'result': result})


@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>LookML Processor</title>
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Sen:wght@400..800&display=swap');
            body,
            select,
            button,
            input {
                font-family: "Sen", sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f2f2f2;
            }

            .container {
                max-width: 800px;
                margin: 50px auto;
                background-color: #fff;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                padding: 20px;
            }

            h1 {
                text-align: center;
            }

            .form-group {
                margin-bottom: 10px;
            }

            label {
                font-weight: bold;
            }

            input[type="file"] {
                display: none;
            }

            .custom-file-upload {
                border: 1px solid #ccc;
                display: inline-block;
                padding: 6px 12px;
                cursor: pointer;
                background-color: #f9f9f9;
                border-radius: 4px;
            }

            .custom-file-upload:hover {
                background-color: #e9e9e9;
            }

            #output {
                margin-top: 20px;
                padding: 10px;
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: #f9f9f9;
            }

            #output pre {
                white-space: pre-wrap;
                word-wrap: break-word;
            }
            .power_bi{
                width: 100%;
                display: flex;
                flex-direction: column;
                margin-bottom: 1.5rem;  
            }
            .power_bi label{
                margin-bottom: .8rem;
            }
            .power_bi select{
                padding: .8rem;
                border-radius: 7px;
                background: #fff;
                border: 1px solid #dbdbdb;
                font-size: 1rem;
            }
            .btn{
                padding: .8rem;
                border: 1px solid #dbdbdb;
                border-radius: 7px;
                background: #003049;
                color: #fff;
            }

           #looker_hint,
           #power_bi_hint{
            margin-top: 1.5rem;
           }
           #looker_hint > div,
           #power_bi_hint > div{
            margin-top: 1rem;
           }
           #looker_hint > div:nth-child(1),
           #power_bi_hint > div:nth-child(1){
             font-weight: 700;
             font-size: 1.2rem;
           }
           #looker_hint > div > span,
           #power_bi_hint > div > span{
            font-weight: 700;
           }

        </style>
    </head> 
    <body>
        <div class="container">
            <h1>Domo Migrator</h1>

            <div class='power_bi'>
                <label>BI Tools</label>
                <select id='bi_tools'>
                    <option value='looker'>Looker</option>
                    <option value='power_bi'>Power BI</option>
                </select>
            </div>

            <form id="uploadForm" enctype="multipart/form-data">
                <div class="form-group">
                    <label for="file">Upload LookML File:</label>
                    <label class="custom-file-upload">
                        <input type="file" id="file" name="file" accept=".model.lkml">
                        Choose File
                    </label>
                    <span id="filename"></span>
                </div>
                <button type="submit" class='btn'>Process File</button>
            </form>

            <form id="uploadForm1" enctype="multipart/form-data">
                <div class="form-group">
                    <label for="file">Upload Power BI File:</label>
                    <label class="custom-file-upload">
                        <input type="file" id="file1" name="file" accept=".json">
                        Choose File
                    </label>
                    <span id="filename1"></span>
                </div>
                <button type="submit" class='btn'>Process File</button>
            </form>

            <div id='looker_hint'>
                <div>Looker Instruction</div>
                <div><span>1.</span> Get the Lookml from repective Model file</div>
                <div><span>2.</span> Paste the code in the new text document</div>
                <div><span>3.</span> Make use to clean the other parameters apart from explores</div>
                <div><span>4.</span> Save the file with extension of yourfilename.model.lkml</div>
            </div>

            <div id='power_bi_hint'>
                <div>Power BI Instruction</div>
                <div><span>1.</span> Export the <a href = 'https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-templates'><b>template file</b></a> from yourfilename.pbix</div>
                <div><span>2.</span> Zip the template file (yourfilename.pbit) using <a href='https://www.win-rar.com/download.html?&L=0'><b>WinRAR</b></a></div>
                <div><span>3.</span> Open datamodelschema file to copy the Json in the zipped template file</div>
                <div><span>4.</span> Validate the Json using any online json <a href = 'https://jsonformatter.org/'><b>validator</b></a></div>
                <div><span>5.</span> After Validation save the Json file like yourfilename.json</div>
            </div>

            <div id="output" style="display: none;">
                <h2>Processed Output:</h2>
                <pre id="outputContent"></pre>
            </div>
        </div>

         <script>
document.getElementById('uploadForm').style.display = "block";
document.getElementById('uploadForm1').style.display = "none";
document.getElementById('looker_hint').style.display = "block";
document.getElementById('power_bi_hint').style.display = "none";

document.getElementById('bi_tools').addEventListener('change', (e) => {
    if (e.target.value === 'looker') {
        document.getElementById('uploadForm').style.display = "block";
        document.getElementById('uploadForm1').style.display = "none";
        document.getElementById('looker_hint').style.display = "block";
        document.getElementById('power_bi_hint').style.display = "none";
    } else if (e.target.value === 'power_bi') {
        document.getElementById('uploadForm').style.display = "none";
        document.getElementById('uploadForm1').style.display = "block";
        document.getElementById('looker_hint').style.display = "none";
        document.getElementById('power_bi_hint').style.display = "block";
    } else {
        document.getElementById('uploadForm').style.display = "none";
        document.getElementById('uploadForm1').style.display = "none";
        document.getElementById('looker_hint').style.display = "none";
        document.getElementById('power_bi_hint').style.display = "none";
    }
});

document.getElementById('file').addEventListener('change', function () {
    document.getElementById('filename').innerText = this.files[0].name;
});

document.getElementById('file1').addEventListener('change', function () {
    document.getElementById('filename1').innerText = this.files[0].name;
});

document.getElementById('uploadForm').addEventListener('submit', function (e) {
    e.preventDefault();
    let formData = new FormData(this);
    fetch('/process_file', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.model) {
            document.getElementById('output').style.display = "block";
            document.getElementById('outputContent').innerText = JSON.stringify(data, null, 2);
        }
    })
    .catch(error => console.error('Error:', error));
});

document.getElementById('uploadForm1').addEventListener('submit', function (e) {
    e.preventDefault();
    let formData = new FormData(this);
    fetch('/file_process', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.result) {
            document.getElementById('output').style.display = "block";
            document.getElementById('outputContent').innerText = JSON.stringify(data, null, 2);
        }
    })
    .catch(error => console.error('Error:', error));
});
</script>

    </body>
    </html>
    '''


def handler(event, context):
    return awsgi.response(app, event, context)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
