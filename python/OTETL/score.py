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


def load_data_folder(path: Path, filter_cols: list[str]) -> pandas.DataFrame:
	"""
	Load all json files in a given path into a dataframe, filtering to select only filter_cols

	:param path: The path to search for json files to load
	:param filter_cols: The columns to select from the loaded json data
	:return: pandas.DataFrame containing all the loaded data
	"""
	files: list = sorted(path.glob('**/*.json'))
	data: pandas.DataFrame = pandas.read_json(files.pop(), lines=True).filter(items=filter_cols)
	for child in files:
		logger.info(f'Processing file {child}')
		dataframe: pandas.DataFrame = pandas.read_json(child, lines=True).filter(items=filter_cols)
		data = pandas.concat([data, dataframe], ignore_index=True)
	return data


def transform_data():
	"""
	Perform the following on Open Targets Data

	1. Load the Diseases dataset into a pandas.DataFrame selecting id and name columns
	2. Load the Targets dataset into a pandas.DataFrame selecting id and approvedSymbol columns
	3. Load the EVA Evidence dataset into a pandas.DataFrame selecting diseaseId, targetId, and score columns
	4. Group the EVA Evidence by diseaseId, targetId pairs and create a new column containing a list of the grouped scores
	5. Create a new column containing the median of the scores
	6. Create a new column containing the top 3 scores
	7. Join the disease and targets datasets to add disease name and target symbol columns
	8. Sort by the median column
	9. Save the dataframe to a json file in the output directory
	"""
	disease_filter_cols: list[str] = ['id', 'name']
	diseases: pandas.DataFrame = load_data_folder(disease_path, disease_filter_cols)
	logger.debug(f'\n{diseases}')

	target_filter_cols: list[str] = ['id', 'approvedSymbol']
	targets: pandas.DataFrame = load_data_folder(target_path, target_filter_cols)
	logger.debug(f'\n{targets}')

	eva_filter_cols: list[str] = ['diseaseId', 'targetId', 'score']
	eva: pandas.DataFrame = load_data_folder(eva_path, eva_filter_cols)
	logger.debug(f'\n{eva}')

	grouped = eva.groupby(['diseaseId', 'targetId'], group_keys=True)

	score_list: pandas.DataFrame = grouped['score'].apply(list).to_frame('scorelist').reset_index(names=['diseaseId', 'targetId'])
	score_list['median'] = score_list['scorelist'].apply(median)
	score_list['top3'] = score_list['scorelist'].apply(lambda x: nlargest(3, x))

	disease_join: pandas.DataFrame = score_list.join(diseases.set_index('id'), on='diseaseId')
	joined: pandas.DataFrame = disease_join.join(targets.set_index('id'), on='targetId')

	logger.debug(f"\n{joined}")

	data_out_path.mkdir(parents=True, exist_ok=True)
	out_filepath: Path = data_out_path.joinpath('data.json')
	parsed = json.loads(joined.drop(columns=['scorelist']).sort_values(by=['median']).to_json(orient="index"))
	with open(out_filepath, 'w', encoding='utf-8') as f:
		json.dump(parsed, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
	import datetime
	now_string: str = datetime.datetime.now().isoformat().replace(':', '.')

	log_path.mkdir(parents=True, exist_ok=True)
	console = logging.StreamHandler()
	logfile = logging.FileHandler(filename=log_path.joinpath(f'{now_string}_score.log'))
	logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8', level=logging.DEBUG, handlers=[console,logfile])
	logger = logging.getLogger(__name__)

	pandas.set_option('display.max_columns', None)

	transform_data()
