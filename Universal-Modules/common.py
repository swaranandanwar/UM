import logging
import sys
path = r"C:\Users\adamszeq\Desktop\Clones\Universal-Modules"
sys.path.append(path)
import yaml


with open("Config\config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.Loader)

# Create logger, set level, and add stream handler
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
fhandler = logging.FileHandler('./log/log_model.log')
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
