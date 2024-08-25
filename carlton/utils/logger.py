import logging

# Configurar o logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('app.log'), logging.StreamHandler()],
)

# Cria um logger com o nome do módulo atual
logger = logging.getLogger(__name__)


def log_info(message: str):
    """
    Loga uma mensagem de informação.

    Args:
        message (str): A mensagem a ser logada.
    """
    logger.info(message)


def log_warning(message: str):
    """
    Loga uma mensagem de aviso.

    Args:
        message (str): A mensagem a ser logada.
    """
    logger.warning(message)


def log_error(message: str):
    """
    Loga uma mensagem de erro.

    Args:
        message (str): A mensagem a ser logada.
    """
    logger.error(message)


def log_debug(message: str):
    """
    Loga uma mensagem de depuração.

    Args:
        message (str): A mensagem a ser logada.
    """
    logger.debug(message)
