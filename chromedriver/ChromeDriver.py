from pathlib import Path
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium import webdriver

import os


class ChromeDriver:
    def __init__(self, download_path: Path = Path.cwd()) -> None:
        user = os.getenv("USERNAME")
        self.options = Options()

        # Garante que a pasta de download exista
        download_path.mkdir(parents=True, exist_ok=True)

        # Define as preferências do Chrome, incluindo o local de download
        prefs = {
            "plugins.always_open_pdf_externally": True,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "download.default_directory": str(
                download_path
            ),  # Converte o Path para string
        }

        self.options.add_experimental_option("prefs", prefs)
        self.options.add_experimental_option("excludeSwitches", ["enable-logging"])
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--enable-automation")
        self.options.add_argument("--disable-infobars")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--ignore-certificate-errors")
        self.options.add_argument(
            f"user-data-dir=C:\\Users\\{user}\\AppData\\Local\\Google\\Chrome\\User Data\\Profile 2"
        )
        
        # --- AIRFLOW ---
        #self.options.add_argument("--headless") # Essencial para rodar no Airflow
        #self.options.add_argument("--window-size=1920,1080") # Define um tamanho de janela


        self.service = ChromeService(executable_path=ChromeDriverManager().install())

    @property
    def path_folder_temp(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while not current_dir.endswith("src"):
            current_dir = os.path.dirname(current_dir)

        self.path_temp = os.path.join(current_dir, "temp")
        if not os.path.exists(self.path_temp):
            os.makedirs(self.path_temp)
        return self.path_temp

    def start_driver(self) -> tuple[WebDriver, WebDriverWait[WebDriver]]:
        driver = webdriver.Chrome(service=self.service, options=self.options)
        driver.maximize_window()
        wait = WebDriverWait(driver, 15)
        return driver, wait