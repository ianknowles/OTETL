import logging
from pathlib import Path

import pandas

from OTETL.download import download
from OTETL.score import transform_data

module_path = Path(__file__).parent.parent
log_path = module_path.joinpath('log')

if __name__ == '__main__':
	import datetime
	now_string: str = datetime.datetime.now().isoformat().replace(':', '.')

	log_path.mkdir(parents=True, exist_ok=True)
	console = logging.StreamHandler()
	logfile = logging.FileHandler(filename=log_path.joinpath(f'{now_string}_score.log'))
	logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8', level=logging.DEBUG, handlers=[console,logfile])
	logger = logging.getLogger(__name__)

	pandas.set_option('display.max_columns', None)

	download()
	transform_data()
