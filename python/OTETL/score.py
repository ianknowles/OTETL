import logging
from pathlib import Path

import pandas

module_path = Path(__file__).parent.parent
log_path = module_path.joinpath('log')
data_in_path = module_path.joinpath('data/in')
disease_path = data_in_path.joinpath('diseases')
target_path = data_in_path.joinpath('targets')
evidence_path = data_in_path.joinpath('evidence')
eva_path = evidence_path.joinpath('sourceId=eva')


def load_data():
	for child in sorted(eva_path.glob('**/*.json')):
		logger.info(f'Processing file {child}')
		dataframe = pandas.read_json(child, lines=True)
		logger.debug(f'\n{dataframe}')


if __name__ == '__main__':
	log_path.mkdir(parents=True, exist_ok=True)
	console = logging.StreamHandler()
	logfile = logging.FileHandler(filename=log_path.joinpath('score.log'))
	logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8', level=logging.DEBUG, handlers=[console,logfile])
	logger = logging.getLogger(__name__)

	load_data()
