
import pandas as pd
import logging
import requests
import json

from io import StringIO
from sqlalchemy import create_engine

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
    
class DataConn:
    def __init__(self, config: dict,schema: str):
        self.config = config
        self.schema = schema
        self.db_engine = None


    def get_conn(self):
        username = self.config.get('REDSHIFT_USERNAME')
        password = self.config.get('REDSHIFT_PASSWORD')
        host = self.config.get('REDSHIFT_HOST')
        port = self.config.get('REDSHIFT_PORT', '5439')
        dbname = self.config.get('REDSHIFT_DBNAME')

        # Construct the connection URL
        connection_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
        self.db_engine = create_engine(connection_url)

        try:
            with self.db_engine.connect() as connection:
                result = connection.execute('SELECT 1;')
            if result:
                logging.info("Connection created")
                return
        except Exception as e:
            logging.error(f"Failed to create connection: {e}")
            raise

    def upload_data(self, data: pd.DataFrame, table: str):
        try:
            data.to_sql(
                table,
                con=self.db_engine,
                schema=self.schema,
                if_exists='append',
                index=False
            )

            logging.info(f"Data from the DataFrame has been uploaded to the {self.schema}.{table} table in Redshift.")
        except Exception as e:
            logging.error(f"Failed to upload data to {self.schema}.{table}: {e}")
            raise

    def close_conn(self):
        if self.db_engine:
            self.db_engine.dispose()
            logging.info("Connection to Redshift closed.")
        else:
            logging.warning("No active connection to close.")




class DataManager:

    def __init__(self):
        self.data = None

    def get_data(self):
        try:
            logging.info("Creacion de data")
            data_list={'Name':[],'Gender':[],'Birth_year':[],'Eye_color':[],'Skin_color':[],'Hair_color':[],'Mass':[],'Height':[],'Homeworld':[]}
            endpoint=1
            while endpoint < 82:
                if endpoint == 17:
                    endpoint += 1
                    continue

                url="https://www.swapi.tech/api/people/{}".format(endpoint)
                response=requests.get(url)
                data_json=response.json()['result']
                homeworld_url=data_json['properties']['homeworld']

   
                name=data_json['properties']['name']
                gender=data_json['properties']['gender']
                birth=data_json['properties']['birth_year']
                eyes=data_json['properties']['eye_color']
                skin=data_json['properties']['skin_color']
                hair=data_json['properties']['hair_color']
                mass=data_json['properties']['mass']
                height=data_json['properties']['height']
                homeworld=requests.get(homeworld_url).json()['result']['properties']['name']

                hair=hair.replace(", ","-")
                eyes=eyes.replace(", ","-")
                skin=skin.replace(", ","-")

                data_list['Name'].append(name)
                data_list['Gender'].append(gender)
                data_list['Birth_year'].append(birth)
                data_list['Eye_color'].append(eyes)
                data_list['Skin_color'].append(skin)
                data_list['Hair_color'].append(hair)
                data_list['Mass'].append(mass)
                data_list['Height'].append(height)
                data_list['Homeworld'].append(homeworld)

                endpoint += 1
            
            return data_list
            
        except Exception as e:
            logging.error(e)
        finally:
            logging.warn("Check the data format")

    def data_transform(self):
        try:
            logging.info('Data to Pandas Dataframe')
            data = self.get_data()
            df=pd.DataFrame(data)
           
            
            return df
        
        except Exception as e:
            logging.error(e)