import pandas as pd
from bs4 import BeautifulSoup
import requests
from sqlalchemy.dialects import postgresql
from model import EarningsSchedule, Base
from db import session, ENGINE
import os
from dotenv import load_dotenv
from os.path import join, dirname

ON_HEROKU = os.environ.get("ON_HEROKU", False)
if not ON_HEROKU:
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

JPX_URL = os.environ.get('JPX_URL')
DATABASE_URL = os.environ.get('DATABASE_URL')
COLUMN_MAPPING = {'発表予定日': 'date', 'コード': 'code', '会社名': 'name', '決算期末': 'term', '業種名': 'segment', '種別': 'pattern',
                  '市場区分': 'market'}
PATTERN_MAPPING = {
    '第３四半期': '3Q', '第２四半期': '2Q', '第１四半期': '1Q', '本決算': '4Q', '-': ''
}

BASE_FILE_PATH = '/tmp/' if ON_HEROKU else './'

class EarningsDataController():
    def __init__(self):
        pass

    def download_xls(self, file_name, url):
        res = requests.get(url)
        save_to = '{}{}'.format(BASE_FILE_PATH, file_name)
        if res.status_code == 200:
            open(save_to, 'wb').write(res.content)
            print('Done')
            return save_to

    def get_hrefs(self):
        response = requests.get(JPX_URL)
        soup = BeautifulSoup(response.text, 'lxml')

        xlses = []
        for a in soup.find_all('a', href=True):
            if a['href'].endswith('.xls'):
                xlses.append(a['href'])
        return xlses

    def clean_dataframe(self, df):
        df = df.dropna()
        df = df.rename(columns=COLUMN_MAPPING)
        df['pattern'] = df['pattern'].map(PATTERN_MAPPING)
        df['code'] = df['code'].astype(int)
        df['code'] = df['code'].astype(str)
        return df

    def download(self, xlses):
        file_paths = []
        for idx, xls in enumerate(xlses):
            file_name = xls.split('/')[-1]
            path = self.download_xls(file_name, '{}{}'.format('https://www.jpx.co.jp', xls))
            file_paths.append(path)
        return file_paths

    def generate_dataframe(self, file_paths):
        dfs = []
        for file_path in file_paths:
            idx_key = file_path.split('/')[-1].replace('.xls', '')
            df = pd.read_excel(file_path, skiprows=2)
            df = self.clean_dataframe(df)
            df['id'] = idx_key + '-' + df['code']
            dfs.append(df)
        return dfs

    def cleanup(self, dfs):
        combined_df = pd.concat(dfs)
        combined_df['date'] = combined_df['date'].replace('未定', '')
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        combined_df['date'] = combined_df.date.astype(object).where(combined_df.date.notnull(), None)
        return combined_df

    def run(self):
        hrefs = self.get_hrefs()
        paths = self.download(hrefs)
        dfs = self.generate_dataframe(paths)
        df = self.cleanup(dfs)
        self.create_table_if_not_exists()
        self.upsert_to_postgres(df)

    def create_table_if_not_exists(self):
        Base.metadata.create_all(ENGINE)

    def upsert_to_postgres(self, df):
        values = df.to_dict('records')
        table = EarningsSchedule.__table__

        stmt = postgresql.insert(table).values(values)

        update_cols = [c.name for c in table.c
                       if c not in list(table.primary_key.columns)
                       and c.name not in []]

        on_conflict_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns,
            set_={k: getattr(stmt.excluded, k) for k in update_cols},
        )

        print('upserting...')
        session.execute(on_conflict_stmt)
        session.commit()
        print('executed!')


if __name__ == '__main__':
    controller = EarningsDataController()
    controller.run()
