import ftplib
from ftplib import FTP
from pathlib import Path
import logging


module_path = Path(__file__).parent.parent
log_path = module_path.joinpath('log')
data_in_path = module_path.joinpath('data/in')
disease_path = data_in_path.joinpath('diseases')
target_path = data_in_path.joinpath('targets')
evidence_path = data_in_path.joinpath('evidence')

log_path.mkdir(parents=True, exist_ok=True)
console = logging.StreamHandler()
logfile = logging.FileHandler(filename=log_path.joinpath('download.log'))
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf-8', level=logging.DEBUG, handlers=[console,logfile])
logger = logging.getLogger(__name__)

ftp_url: str = 'ftp.ebi.ac.uk'
release_path: str = '/pub/databases/opentargets/platform/21.11/output/etl/json'


def download():
	"""Download the EVA Evidence, Disease and Target datasets"""
	with FTP(ftp_url) as ftp:
		ftp.login()
		logger.info(f'Logged into {ftp_url}')

		download_folder(ftp, disease_path, f'{release_path}/diseases')
		download_folder(ftp, target_path, f'{release_path}/targets')
		download_folder(ftp, evidence_path.joinpath('sourceId=eva'), f'{release_path}/evidence/sourceId=eva/')


def download_full_evidence():
	"""Download all the available evidence datasets"""
	with FTP(ftp_url) as ftp:
		ftp.login()
		logger.info(f'Logged into {ftp_url}')

		download_subfolders(ftp, evidence_path, f'{release_path}/evidence')


def download_subfolders(ftp: FTP, local_path: Path, remote_path: str):
	"""Download the contents of FTP subfolders, found at remote_path, to local_path"""
	try:
		ftp.cwd(remote_path)
		folders: list[str] = ftp.nlst()
		folders.remove('_SUCCESS')
		logger.debug(folders)
		for folder in folders:
			path: Path = local_path.joinpath(folder)

			download_folder(ftp, path, f'{remote_path}/{folder}')
	except ftplib.all_errors:
		logger.exception('An FTP exception has occurred, check connection, remote and local files and paths')


def download_folder(ftp: FTP, local_path: Path, remote_path: str):
	"""Download the contents of an FTP folder, found at remote_path, to local_path"""
	local_path.mkdir(parents=True, exist_ok=True)
	try:
		ftp.cwd(remote_path)
		filenames: list[str] = ftp.nlst()
		filenames.remove('_SUCCESS')
		logger.debug(filenames)
		for filename in filenames:
			path: Path = local_path.joinpath(filename)

			with path.open('wb') as file:
				ftp.retrbinary(f'RETR {filename}', file.write)
				logger.info(f'{remote_path}/{filename} saved to {path}')
	except ftplib.all_errors:
		logger.exception('An FTP exception has occurred, check connection, remote and local files and paths')


if __name__ == '__main__':
	download()
