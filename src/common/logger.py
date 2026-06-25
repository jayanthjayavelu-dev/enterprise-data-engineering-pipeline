import logging
import os


def setup_logging():
    if not os.getenv("AIRFLOW_HOME"):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        )
