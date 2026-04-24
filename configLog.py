import os, logging
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

LOG_PATH = "./.logs/"

def buscarPath() -> str:
    """ Busca o caminho do log atual ou cria um novo caso não exista """

    path_atual:str = ''
    mes_atual:str = datetime.now().strftime('%Y%m')
    try:
        if not os.path.exists(LOG_PATH+f"{mes_atual}.log"):
            os.makedirs(LOG_PATH, exist_ok=True)
            with open(LOG_PATH+f"{mes_atual}.log", "w") as f:
                pass
        path_atual = LOG_PATH+f"{mes_atual}.log"
    except Exception as e:
        print(f"Erro ao criar log: {e}")
    finally:
        pass
    return path_atual

def configLog(name:str) -> logging:
    """
    Configura o logger.
        :param name: nome da função que está sendo executada
    """

    logger = logging.getLogger(name)
    logging.basicConfig(filename=buscarPath(),
                        encoding='utf-8',
                        format=os.getenv('LOGGER_FORMAT'),
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG)
    # silencia libs verbosas
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)                        
    return logger