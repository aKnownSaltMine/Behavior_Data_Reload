# import dependencies
import os
from datetime import datetime
from pathlib import Path
from shutil import move, rmtree
from zipfile import ZipFile
from Dependencies.setup import setup

try:
    import pandas as pd
    import sqlalchemy
    import tableauhyperapi as tab
    from sqlalchemy import event
except ImportError:
    setup()
    import pandas as pd
    import sqlalchemy
    import tableauhyperapi as tab
    from sqlalchemy import event

tableau_url = '' # Network URL

# declaring the folder paths
cwd = os.path.dirname(__file__)

data_folder = os.path.join(cwd, 'Data')
dashboard_file = 'Behavior Analyzer Agent Performance IV VR.twbx'
dasboard_path = os.path.join(data_folder, dashboard_file)
download_folder = os.path.join(Path.home(), 'Downloads')
download_path = os.path.join(download_folder, dashboard_file)
hyper_folder = os.path.join(data_folder, 'hyper')

# creating flag for if the dashboard has already been downloaded on the day it has been run
# this way if the script errors out on upload of data, it doesnt have to redownload
downloaded_today = False
if os.path.exists(dasboard_path):
    file_time = datetime.fromtimestamp(os.path.getmtime(dasboard_path)).date()
    today = datetime.today().date()
    print(file_time)
    print(today)
    if file_time == today:
        print('Using dashboard that has already been downloaded today.')
        downloaded_today = True


if downloaded_today == False:
    while os.path.exists(download_path) == False:
        
        print(f'Dashboard has not been detected.')
        print(f'Please download the report from: {tableau_url}')
        print('-'*25)
        input('Press Enter when completed')
    else:
        print('Dashboard detected. Continuing with the script.')
        move(download_path, dasboard_path)

# removing old hyper database if exists
if os.path.exists(hyper_folder) == False:
    os.makedirs(hyper_folder)
else:
    rmtree(hyper_folder)
    os.makedirs(hyper_folder)

# unzipping the tableau file in order to get the database out of the twbx file
with ZipFile(dasboard_path, 'r') as zip_ref:
    zip_ref.extractall(hyper_folder)
    print("Tableau File Extracted")

# creating path to the hyper file
database_folder = os.path.join(hyper_folder, 'Data', 'Extracts')
database_folder_contents = os.listdir(database_folder)
hyper_files = [
    value for value in database_folder_contents if value.endswith('.hyper')]

if len(hyper_files) > 1:
    print('there is more than one hyper file')

database_path = os.path.join(database_folder, hyper_files[0])
print('Database Located')

# opening the hyper file to query it and return it as a list of lists
with tab.HyperProcess(telemetry=tab.Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with tab.Connection(hyper.endpoint, database=database_path) as connection:
        table_name = tab.TableName("Extract", "Extract")
        table_definition = connection.catalog.get_table_definition(
            name=table_name)
        column_names = [str(value.name)[1:-1]
                        for value in table_definition.columns]
        results = connection.execute_list_query(
            query=f"SELECT agent_id, behavior, caldt, deno_mediafilecount, num_mediafilecount FROM {table_name} WHERE lob = 'Video Repair'")

# creating dataframe from the lists and correcting the datatypes
column_names = ['Agent Id', 'Behavior', 'Caldt',
                'Denominator Count', 'Numerator Count']
df = pd.DataFrame(data=results, columns=column_names)
df = df.dropna(how='all', subset=[
               'Denominator Count', 'Numerator Count']).reset_index(drop=True)
df['Caldt'] = df['Caldt'].astype(str)
df['Caldt'] = pd.to_datetime(df['Caldt']).dt.date
row_count = df.shape[0]

print(f'Total rows extracted: {row_count:,}')

# grabbing the earliest date from the data in order to delete the data from the server
earliest_date = df['Caldt'].min()
earliest_date_str = earliest_date.strftime("%m/%d/%Y")

clean_table_query = f'''
DELETE FROM GVPOperations.VID.OAI_VR_Export where Caldt >= '{earliest_date.strftime("%m/%d/%Y")}'
'''

print(f'Earliest date from Tableau: {earliest_date.strftime("%m/%d/%Y")}')

server_name = '' # Network Server
db_name = 'GVPOperations'

engine = sqlalchemy.create_engine(
    f'mssql+pyodbc://@{server_name}/{db_name}?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server', fast_executemany=True)
conn = engine.connect()
print('Database connected')

# updating the receive_before_cursor method to use fast_executemany in order to insert faster rather than one row at a time
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(
    conn, cursor, statement, params, context, executemany
):
    if executemany:
        cursor.fast_executemany = True

# cleaning out overlapping data
conn.execute(sqlalchemy.sql.text(clean_table_query))
conn.commit()

print('Old Rows Deleted')

# writing data to table from dataframe before committing the changes and closing the connection to server.
print(f'Writing {row_count:,} to database. (This can take up to 10 min)')
df.to_sql(name='OAI_VR_Export', con=engine,
          schema='VID', if_exists='append', index=False)

conn.close()
engine.dispose()

print('Writing completed.')
