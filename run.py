import logging
from controllers import EarningsDataController, JpxExcelDataController
logging.basicConfig(level=logging.INFO)


def run(dry_run=False):
    jpx_excel_data_controller = JpxExcelDataController()
    earnings_data_controller = EarningsDataController()

    logging.info('downloading files and generating dataframes')
    hrefs = jpx_excel_data_controller.get_hrefs()
    paths = jpx_excel_data_controller.download(hrefs)

    dfs = earnings_data_controller.generate_dataframe(paths)
    df = earnings_data_controller.cleanup(dfs)
    earnings_data_controller.create_table_if_not_exists()

    if dry_run:
        logging.info('dry run. not upserting to postgres')
        return
    logging.info('upserting to postgres')
    earnings_data_controller.upsert_to_postgres(df)


if __name__ == '__main__':
    logging.info('start')
    run(dry_run=True)
    logging.info('done')
