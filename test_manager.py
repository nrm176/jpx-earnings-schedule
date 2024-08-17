import pytest
import pandas as pd
from transform import JpxEarningDataTransformer

class TestJpxEarningDataManager:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.manager = JpxEarningDataTransformer()
        self.sample_data = {
            '決算発表予定日\nScheduled Dates for Earnings Announcements': ['2023-01-01', '2023-02-01'],
            'コード\nCode': [1234, 5678],
            '種別': ['Type1', 'Type2'],
            '会社名': ['Company1', 'Company2'],
            '決算期末\nFiscal Year-end': ['2023-12-31', '2023-12-31'],
            '業種名': ['Industry1', 'Industry2'],
            '市場区分': ['Market1', 'Market2']
        }
        self.sample_df = pd.DataFrame(self.sample_data)

    @pytest.fixture
    def mocker(self, mocker):
        mocker.patch('pandas.read_excel', return_value=self.sample_df)
        return mocker

    def test_add_file_path(self):
        result = self.manager.add_file_path('sample.xlsx')
        assert result.file_path == 'sample.xlsx'
        assert result is self.manager

    def test_to_dataframe(self, mocker):
        self.manager.add_file_path('sample.xlsx').to_dataframe()
        expected_data = {
            'date': ['2023-01-01', '2023-02-01'],
            'code': ['1234', '5678'],
            'pattern': ['Type1', 'Type2'],
            'name': ['Company1', 'Company2'],
            'term': ['2023-12-31', '2023-12-31'],
            'segment': ['Industry1', 'Industry2'],
            'market': ['Market1', 'Market2']
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(self.manager.get_dataframe(), expected_df)

    def test_get_dataframe(self, mocker):
        self.manager.add_file_path('sample.xlsx').to_dataframe()
        result_df = self.manager.get_dataframe()
        assert not result_df.empty
        assert list(result_df.columns) == ['date', 'code', 'pattern', 'name', 'term', 'segment', 'market']