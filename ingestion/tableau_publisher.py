"""
ingestion/tableau_publisher.py
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import tableauserverclient as TSC

load_dotenv()

TABLEAU_USERNAME   = os.getenv("TABLEAU_USERNAME")
TABLEAU_PASSWORD   = os.getenv("TABLEAU_PASSWORD")
TABLEAU_WORKBOOK   = os.getenv("TABLEAU_WORKBOOK_NAME", "Gas Price Analytics")
TABLEAU_PUBLIC_URL = "https://api.tableau.com"
CSV_PATH           = Path("./data/gas_prices_tableau.csv")


def publish_to_tableau():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    logger.info(f"Connecting to Tableau Public as {TABLEAU_USERNAME}...")

    tableau_auth = TSC.TableauAuth(
        username=TABLEAU_USERNAME,
        password=TABLEAU_PASSWORD,
        site_id=""
    )

    server = TSC.Server("https://public.tableau.com")
    server.version = "3.17"
    server.add_http_options({"verify": True})

    with server.auth.sign_in(tableau_auth):
        logger.info("Connected to Tableau Public!")

        projects, _ = server.projects.get()
        project = next((p for p in projects if p.name == "default"), projects[0])

        workbook_item = TSC.WorkbookItem(
            project_id=project.id,
            name=TABLEAU_WORKBOOK,
            show_tabs=False,
        )

        logger.info(f"Publishing {CSV_PATH}...")
        workbook = server.workbooks.publish(
            workbook_item,
            str(CSV_PATH),
            TSC.Server.PublishMode.Overwrite,
        )

        logger.success(f"Published! URL: {TABLEAU_PUBLIC_URL}/views/{workbook.content_url}")
        return workbook.content_url


if __name__ == "__main__":
    logger.add("logs/tableau_{time:YYYY-MM-DD}.log", rotation="1 day")
    url = publish_to_tableau()
    print(f"Dashboard: {TABLEAU_PUBLIC_URL}/views/{url}")
