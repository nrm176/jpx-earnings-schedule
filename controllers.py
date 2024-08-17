import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy.dialects import postgresql
import psycopg2
from consts import JPX_URL, BASE_FILE_PATH, PATTERN_MAPPING
from model import EarningsSchedule, Base
from db import session, engine
from transform import JpxEarningDataTransformer

class JpxExcelDataController:
    def get_hrefs(self):
        response = requests.get(JPX_URL)
        soup = BeautifulSoup(response.text, 'lxml')
        xlses = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.xlsx')]
        for xls in xlses:
            logging.info(f'Found the URL for excel file: {xls}')
        return xlses

    def download(self, xlses):
        file_paths = [self.download_xls(xls.split('/')[-1], f'https://www.jpx.co.jp{xls}') for xls in xlses]
        for idx, path in enumerate(file_paths):
            logging.info(f'Downloaded {idx + 1} files')
        return file_paths

    def download_xls(self, file_name, url):
        res = requests.get(url)
        save_to = f'{BASE_FILE_PATH}{file_name}'
        if res.status_code == 200:
            with open(save_to, 'wb') as f:
                f.write(res.content)
            logging.info('Done')
            return save_to
        else:
            logging.error(f'Failed to download {url}')
            return None

class EarningsDataController:
    def __init__(self):
        self.data_manager = JpxEarningDataTransformer()

    def generate_dataframe(self, file_paths):
        dfs = []
        for file_path in file_paths:
            df = self.data_manager.add_file_path(file_path).to_dataframe().get_dataframe()
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce')
            df['date'] = df['date'].astype(object).where(df['date'].notnull(), None)
            df['code'] = df['code'].astype(str)
            df['pattern'] = df['pattern'].map(PATTERN_MAPPING)
            try:
                df['id'] = df['code'] + '-' + df['pattern'] + '_' + pd.DatetimeIndex(df['date']).strftime('%Y')
            except AttributeError as ae:
                logging.info(f'AttributeError: {ae}')
                df['id'] = df['code'] + '-' + df['pattern'] + '_undecided'
            dfs.append(df)
        return dfs

    def cleanup(self, dfs):
        combined_df = pd.concat(dfs).drop_duplicates(subset='id', keep="last")
        return combined_df

    def create_table_if_not_exists(self):
        Base.metadata.create_all(engine)

    def upsert_to_postgres(self, df):
        values = df.to_dict('records')
        table = EarningsSchedule.__table__
        stmt = postgresql.insert(table).values(values)
        update_cols = [c.name for c in table.c if c not in list(table.primary_key.columns) and c.name not in ['']]
        logging.info(update_cols)
        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns,
            set_={k: getattr(stmt.excluded, k) for k in update_cols},
        )
        logging.info('upserting...')
        try:
            session.execute(on_conflict_stmt)
            session.commit()
            logging.info('executed!')
        except psycopg2.ProgrammingError as e:
            logging.error(e)