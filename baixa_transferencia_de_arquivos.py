import os
from dotenv import load_dotenv; load_dotenv()


from ChromeDriver import ChromeDriver


def login(driver: WebDriver, wait: WebDriverWait):
    try:
        templates_carregados = carregar_templates_digitos_dinamico()
    except FileNotFoundError as e:
        print(e)
        return
    
    driver.get(URL_LOGIN)
    
    insere_codigo_usuario(driver, wait)
    
    mapeamento_numeros_para_id = mapear_numeros_para_id(driver, wait, templates_carregados)


    if len(mapeamento_numeros_para_id) < 10:
        print("\nAVISO CRÍTICO: Não foi possível mapear todos os dígitos (0-9).")
        print("Verifique os templates e tente novamente.")
        login(driver, wait)
    else:
        print(f"\nIniciando clique da sequência: {SENHA_ALVO}")
        sequencia_de_cliques(wait, mapeamento_numeros_para_id)

def mains():
    chrome_driver = ChromeDriver(download_path="")
    driver, wait = chrome_driver.start_driver()
    

if __name__ == "__main__":
    main()