import os
from dotenv import load_dotenv
from os.path import join, dirname

ON_HEROKU = os.environ.get("ON_HEROKU", False)
if not ON_HEROKU:
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

PATTERN_MAPPING = {
    '第３四半期': '3Q', '第２四半期': '2Q', '第１四半期': '1Q', '本決算': '4Q', '-': ''
}


JPX_URL = os.environ.get('JPX_URL')
COLUMN_MAPPING = {'発表予定日': 'date', 'コード': 'code', '会社名': 'name', '決算期末': 'term', '業種名': 'segment',
                  '種別': 'pattern',
                  '市場区分': 'market'}



BASE_FILE_PATH = '/tmp/' if ON_HEROKU else './excels/'