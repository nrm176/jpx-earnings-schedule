import pandas as pd

class JpxEarningDataTransformer:
    def __init__(self):
        self.data = []
        self.file_path = None

    def add_file_path(self, file_path):
        self.file_path = file_path
        return self

    def to_dataframe(self) -> pd.DataFrame:
        # Load the specific sheet, skipping the first 4 rows
        df = pd.read_excel(self.file_path, sheet_name='List', skiprows=4)

        # Rename the relevant columns to match the required JSON keys
        df = df.rename(columns={
            '決算発表予定日\nScheduled Dates for Earnings Announcements': 'date',
            'コード\nCode': 'code',
            '種別': 'pattern',
            '会社名': 'name',
            '決算期末\nFiscal Year-end': 'term',
            '業種名': 'segment',
            '市場区分': 'market'
        })

        df = df[df['date'] != '未定_Undecided']
        df = df.dropna(subset=['date', 'code', 'pattern'])
        df['code'] = df['code'].astype(str)

        # select certain columns
        # ['date', 'code', 'pattern', 'name', 'term', 'segment', 'market']
        df = df[['date', 'code', 'pattern', 'name', 'term', 'segment', 'market']]

        self.data = df
        return self

    def get_dataframe(self) -> pd.DataFrame:
        return self.data

if __name__ == '__main__':
    manager = JpxEarningDataTransformer()
    df = manager.add_file_path('./kessan06_0802.xlsx').to_dataframe().get_dataframe()
    print(df)