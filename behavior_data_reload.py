# import dependencies
import os
from datetime import datetime
from pathlib import Path
from requests import post, get
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

"""
SharePoint 365 Custom Authenticator
Description: class will return the auth
authentication header need to make
REST SP API calls. The intention of
this class is to establish and maintain
the authentication header needed to make
authenticated SP REST API calls
"""



class sp_api:
    #Sharepoint EndPoints
    ep_sites = 'sites'
    ep_api = '_api'
    ep_dlep = '/Web/GetFilesByServerRelativePath'
    #Custom EndPoints
    #Custome EndPoint to a Sharepoint List
    aat_instance = ''
    #Sharepoint Site Info
    sp_url = 'chartercom.sharepoint.com/sites/'
    sp_siteName = 'OAI'
    #OAuth 2.0 TokenSites
    oAuth_token_url = 'https://accounts.accesscontrol.windows.net/'
    oAuth_OAuth2_ep = '/tokens/OAuth/2'
    #OAuth 2.0 Values
    #Update with Tenant, Client ID's and Client Secret
    tenant_id = ''
    client_id = ''
    client_secret = ''
    #Defaults
    resource_id = '00000003-0000-0ff1-ce00-000000000000'
    verf_token = True
    authObj = None
    #OAuth 2.0 Responce Values
    token_type = None
    expires_in = None
    not_before = None
    expires_on = None
    #Set Default Resource
    resource = None
    access_token = None

    dlPath_api = sp_url + sp_siteName + "/" + ep_api + ep_dlep

    #Grab the Compiled ResourceID
    def get_resource(self):
        return self.resource_id +'/' + self.sp_url + '@' + self.tenant_id
    #Grab the Sharepoint Site URL
    def get_site_url(self):
        return 'https://'+self.sp_url + '/' + self.ep_sites + '/' + self.sp_siteName + '/' + self.aat_instance
    #Grab the Sharepoint Site API
    def get_site_api_url(self):
        u = self.get_site_url()
        u = u + '/' + self.ep_api
        return u
    #Grab the oAuth2.0 Token URL
    def get_token_api_url(self):
        return self.oAuth_token_url + '/'+self.tenant_id + '/' + self.oAuth_OAuth2_ep
    #Grab the ClickId Token for oAuth
    def get_clientId_token(self):
        return self.client_id + '@' + self.tenant_id
    #Grab the Auth Responce
    def get_auth_response(self):
        #Create a Post Request and set it to the auth Obj in the class
        req = post(self.get_token_api_url(),
            headers={'content-type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'client_credentials',
                'client_id': self.get_clientId_token(),
                'client_secret': self.client_secret,
                'resource': self.get_resource(),
            },verify=self.verf_token)
        self.authObj = req

    def makeAPICall(self,apiuri):
        api_return = None
        api_return_call = get(
            apiuri,
            headers=self.site_auth_header
        )
        if api_return_call.status_code == 200:
            #api_return = api_return_call.json()
            return api_return
    
    #During Initilization, get the Request Request Responce
    def __init__(self, spAData) -> None:
        self.tenant_id = spAData['tenant_id']
        self.client_id = spAData['client_id']
        self.client_secret = spAData['client_secret']
        self.sp_siteName = spAData['sp_siteName']
        self.get_auth_response()
        #If the Responce was 'OK' then Set Variables
        if self.authObj.status_code == 200:
            authObjson = self.authObj.json()
            self.token_type = authObjson['token_type']
            self.expires_in = authObjson['expires_in']
            self.not_before = authObjson['not_before']
            self.expires_on = authObjson['expires_on']
            self.resource = authObjson['resource']
            self.access_token = authObjson['access_token']



def main():
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

    # Using the SP_API call to produce the downloaded file #
    fullDLPath = sp_api.dlPath_api + "(decodedurl='')" # Network URL
    sp_api.makeAPICall(sp_api,fullDLPath)


    if os.path.exists(dasboard_path):
        file_time = datetime.fromtimestamp(os.path.getmtime(dasboard_path)).date()
        today = datetime.today().date()
        if file_time == today:
            downloaded_today = True


    # removing old hyper database if exists
    if os.path.exists(hyper_folder) == False:
        os.makedirs(hyper_folder)
    else:
        rmtree(hyper_folder)
        os.makedirs(hyper_folder)

    # unzipping the tableau file in order to get the database out of the twbx file
    with ZipFile(dasboard_path, 'r') as zip_ref:
        zip_ref.extractall(hyper_folder)

    # creating path to the hyper file
    database_folder = os.path.join(hyper_folder, 'Data', 'Extracts')
    database_folder_contents = os.listdir(database_folder)
    hyper_files = [
        value for value in database_folder_contents if value.endswith('.hyper')]



    database_path = os.path.join(database_folder, hyper_files[0])


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



    # grabbing the earliest date from the data in order to delete the data from the server
    earliest_date = df['Caldt'].min()

    clean_table_query = f'''
    DELETE FROM GVPOperations.VID.OAI_VR_Export where Caldt >= '{earliest_date.strftime("%m/%d/%Y")}'
    '''



    server_name = '' # Network Server
    db_name = 'GVPOperations'

    engine = sqlalchemy.create_engine(
        f'mssql+pyodbc://@{server_name}/{db_name}?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server', fast_executemany=True)
    conn = engine.connect()


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


    # writing data to table from dataframe before committing the changes and closing the connection to server.
    df.to_sql(name='OAI_VR_Export', con=engine,
            schema='VID', if_exists='append', index=False)

    conn.close()
    engine.dispose()

if __name__ == '__main__':
    main()