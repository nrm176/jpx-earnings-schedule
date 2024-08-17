import pytest
import requests
from controllers import JpxExcelDataController

class TestJpxExcelDataController:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.controller = JpxExcelDataController()

    @pytest.fixture
    def mock_requests_get(self, mocker):
        mock_response = mocker.Mock()
        mock_response.status_code = 200
        mock_response.text = '''
        <html>
            <body>
                <a href="/files/sample1.xlsx">sample1.xlsx</a>
                <a href="/files/sample2.xlsx">sample2.xlsx</a>
            </body>
        </html>
        '''
        mocker.patch('requests.get', return_value=mock_response)
        return mocker

    def test_get_hrefs(self, mock_requests_get):
        hrefs = self.controller.get_hrefs()
        assert hrefs == ['/files/sample1.xlsx', '/files/sample2.xlsx']

    def test_download_xls(self, mocker):
        mock_response = mocker.Mock()
        mock_response.status_code = 200
        mock_response.content = b'file content'
        mocker.patch('requests.get', return_value=mock_response)
        mock_open = mocker.patch('builtins.open', mocker.mock_open())

        file_name = 'sample1.xlsx'
        url = 'https://www.jpx.co.jp/files/sample1.xlsx'
        save_to = './excels/sample1.xlsx'
        result = self.controller.download_xls(file_name, url)

        mock_open.assert_called_once_with(save_to, 'wb')
        mock_open().write.assert_called_once_with(b'file content')
        assert result == save_to

    def test_download(self, mock_requests_get, mocker):
        mocker.patch.object(self.controller, 'download_xls', return_value='./excels/sample1.xlsx')
        hrefs = ['/files/sample1.xlsx']
        file_paths = self.controller.download(hrefs)
        assert file_paths == ['./excels/sample1.xlsx']