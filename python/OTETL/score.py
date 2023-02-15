import json
import logging
from heapq import nlargest
from pathlib import Path

import pandas
from numpy import median

module_path = Path(__file__).parent.parent
log_path = module_path.joinpath('log')
data_in_path = module_path.joinpath('data/in')
disease_path = data_in_path.joinpath('diseases')
target_path = data_in_path.joinpath('targets')
evidence_path = data_in_path.joinpath('evidence')
eva_path = evidence_path.joinpath('sourceId=eva')
data_out_path = module_path.joinpath('data/out')


def load_data_folder(path, filter_cols):
	files = sorted(path.glob('**/*.json'))
	data = pandas.read_json(files.pop(), lines=True).filter(items=filter_cols)
	for child in files:
		logger.info(f'Processing file {child}')
		dataframe = pandas.read_json(child, lines=True).filter(items=filter_cols)
		data = pandas.concat([data, dataframe], ignore_index=True)
	return data


def transform_data():
	disease_filter_cols = ['id', 'name']
	diseases = load_data_folder(disease_path, disease_filter_cols)
	logger.debug(f'\n{diseases}')

	target_filter_cols = ['id', 'approvedSymbol']
	targets = load_data_folder(target_path, target_filter_cols)
	logger.debug(f'\n{targets}')

	eva_filter_cols = ['diseaseId', 'targetId', 'score']
	eva = load_data_folder(eva_path, eva_filter_cols)
	logger.debug(f'\n{eva}')

	grouped = eva.groupby(['diseaseId', 'targetId'], group_keys=True)

	score_list = grouped['score'].apply(list).to_frame('scorelist').reset_index(names=['diseaseId', 'targetId'])
	score_list['median'] = score_list['scorelist'].apply(median)
	score_list['top3'] = score_list['scorelist'].apply(lambda x: nlargest(3, x))

	disease_join = score_list.join(diseases.set_index('id'), on='diseaseId')
	joined = disease_join.join(targets.set_index('id'), on='targetId')

	logger.debug(f"\n{joined}")

	data_out_path.mkdir(parents=True, exist_ok=True)
	out_filepath = data_out_path.joinpath('data.json')
	parsed = json.loads(joined.drop(columns=['scorelist']).sort_values(by=['median']).to_json(orient="index"))
	with open(out_filepath, 'w', encoding='utf-8') as f:
		json.dump(parsed, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
	import datetime
	now_string = datetime.datetime.now().isoformat().replace(':', '.')

	log_path.mkdir(parents=True, exist_ok=True)
	console = logging.StreamHandler()
	logfile = logging.FileHandler(filename=log_path.joinpath(f'{now_string}_score.log'))
	logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8', level=logging.DEBUG, handlers=[console,logfile])
	logger = logging.getLogger(__name__)

	pandas.set_option('display.max_columns', None)

	transform_data()
