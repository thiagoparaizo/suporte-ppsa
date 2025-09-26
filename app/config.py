import os

MONGO_URI = "mongodb://localhost:27017/" #os.getenv('MONGO_URI', '-')
MONGO_URI_PRD = os.getenv('MONGO_URI_PRD', '-')
CA_CERTIFICATE_PATH_DEFAULT = os.getenv('CA_CERTIFICATE_PATH_DEFAULT', '')
MONGO_URI_PRD = MONGO_URI_PRD.replace('tlsCAFile=PATH_CERT', f'tlsCAFile={CA_CERTIFICATE_PATH_DEFAULT}')
CACHE_DIR_PATH = os.getenv('CACHE_DIR_PATH', os.path.join(os.getcwd(), 'cache'))

PESQUISA_AMBINTE_PRODUCAO = os.getenv('PESQUISA_AMBINTE_PRODUCAO', False)