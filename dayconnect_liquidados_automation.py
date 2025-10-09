import base64
import datetime
from io import BytesIO
import logging
import os
from pathlib import Path
import re
from time import sleep
import time
from typing import Dict, List
# Módulos essenciais para Template Matching
import cv2
import numpy as np

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from chromedriver.ChromeDriver import ChromeDriver

from data_controller import process_and_upload

# --- CONFIGURAÇÕES GLOBAIS ---
URL_LOGIN = "https://ecode.daycoval.com.br/Login.aspx"
URL_PREVIA_DE_TITULOS = "https://ecode.daycoval.com.br/ng/Cobranca/#/titulos-agrupados"
SENHA_ALVO = "01082018"
# endregion

# region Caminhos
CWD = Path(os.path.dirname(os.path.abspath(__file__)))
PATH_DOWNLOAD = CWD / "downloads"
PATH_DIGIT_TEMPLATES = CWD / "digit_variations" # A pasta onde suas variações estão
# endregion Caminhos


CROP_REGIONS = [
    [27, 53, 50, 70],    # 1. Dígito da Esquerda
    [27, 53, 88, 106],   # 2. Dígito do Centro
    [27, 53, 160, 179],  # 3. Dígito da Direita
]

# Função atualizada para carregar todos os templates da pasta de variações.
def carregar_templates_digitos_dinamico():
    """Carrega TODOS os templates de dígitos encontrados na pasta de variações, lendo o nome do arquivo."""
    
    # templates_agrupados: {'0': [template_0_var1, template_0_var2], '1': [...], ...}
    templates_agrupados: Dict[str, List[np.ndarray]] = {str(i): [] for i in range(10)}
    
    print(f"Carregando templates de dígitos da pasta: {PATH_DIGIT_TEMPLATES.resolve()}")
    
    total_carregado = 0
    
    # 1. Agrupa todos os arquivos por dígito
    for template_path in PATH_DIGIT_TEMPLATES.glob("digit_*.png"):
        try:
            nome_base = template_path.stem 
            # 2. Divide por '_' (['digit', '9', 'pos3'])
            partes = nome_base.split('_') 
            # 3. Pega o segundo elemento, que é o dígito (Ex: '9')
            digito = partes[1] 

            if digito in templates_agrupados:
                template = cv2.imread(str(template_path), cv2.IMREAD_COLOR)
                if template is not None:
                    template_cinza = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
                    templates_agrupados[digito].append(template_cinza)
                    total_carregado += 1
            else:
                print(f"AVISO: Arquivo com nome inesperado ou dígito inválido encontrado: {template_path.name}")

        except Exception as e:
            print(f"ERRO de processamento no arquivo {template_path.name}: {e}")
            continue
    
    if total_carregado == 0:
        raise FileNotFoundError(f"Nenhum template de dígito foi carregado da pasta {PATH_DIGIT_TEMPLATES}. Verifique o caminho e os nomes dos arquivos.")

    print(f"Templates de dígitos carregados com sucesso: {total_carregado} variações.")
    return templates_agrupados


def identificar_digito_por_template(digit_img_cinza: np.ndarray, templates_carregados: Dict[str, List[np.ndarray]]) -> tuple[str, float]:
    """Usa Template Matching para identificar um único dígito recortado, comparando com TODAS as variações."""
    MELHOR_SCORE = 0.85 
    
    maior_similaridade = 0.0
    digito_identificado = ""
    
    # templates_carregados agora é um dicionário que mapeia o dígito para uma LISTA de variações de templates.
    for digito, lista_templates in templates_carregados.items():
        for template in lista_templates: # Itera sobre todas as variações daquele dígito
            
            if digit_img_cinza.shape == template.shape:
                resultado = cv2.matchTemplate(digit_img_cinza, template, cv2.TM_CCOEFF_NORMED)
                _, max_val, _, _ = cv2.minMaxLoc(resultado)
                
                if max_val > maior_similaridade:
                    maior_similaridade = max_val
                    if max_val >= MELHOR_SCORE:
                        digito_identificado = digito
                        # Se encontrarmos um match forte com qualquer variação, retornamos imediatamente.
                        return digito_identificado, maior_similaridade 
    
    return digito_identificado, maior_similaridade


def mapear_numeros_para_id(driver: WebDriver, wait: WebDriverWait, templates_carregados: Dict[str, List[np.ndarray]]):
    mapeamento_numeros_para_id = {}
    
    wait.until(
        EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "input.btnTecla[type='image']")
        )
    )
    botoes_teclado = driver.find_elements(
        By.CSS_SELECTOR, "input.btnTecla[type='image']"
    )
    
    if len(botoes_teclado) == 0:
        print("ERRO: Não foram encontrados botões do teclado virtual.")
        return {}


#region Image Handler
    for botao in botoes_teclado:
        id_do_botao = botao.get_attribute("id")
        img_src = botao.get_attribute("src")
        
        if "base64," not in img_src:
            continue
            
        base64_string = img_src.split(",")[1]
        img_data = base64.b64decode(base64_string)
        
        image_np = np.frombuffer(img_data, np.uint8)
        img_opencv = cv2.imdecode(image_np, cv2.IMREAD_COLOR)
        
        digitos_do_botao: List[str] = []

        # Itera sobre as 3 regiões de corte (Dígito 1, 2 e 3)
        for j, (y_start, y_end, x_start, x_end) in enumerate(CROP_REGIONS):
            try:
                cropped_img = img_opencv[y_start:y_end, x_start:x_end]
                cropped_img_cinza = cv2.cvtColor(cropped_img, cv2.COLOR_BGR2GRAY)
            except Exception as e:
                print(f"AVISO: Falha ao cortar a imagem do botão {id_do_botao}. Erro: {e}")
                break
                
            digito_identificado, score = identificar_digito_por_template(cropped_img_cinza, templates_carregados)
            
            if digito_identificado:
                digitos_do_botao.append(digito_identificado)
            else:
#IF DEBUG == True:
                # --- DEBUG PARA DÍGITO NÃO ENCONTRADO ---
                #print(f"  - DEBUG: Dígito {j+1} falhou no Botão {id_do_botao}. Score Máx: {score:.2f}.")
                
                # Salva o corte para inspeção visual
                #cv2.imwrite(f"debug_falha_{id_do_botao}_corte_{j+1}_score_{score:.2f}.png", #cropped_img)
                # --- FIM DEBUG ---
#ENDIF
                break 
#endregion Image handler

        if len(digitos_do_botao) == 3:
            digitos_reconstruidos = "".join(digitos_do_botao)
            print(
                f"  - Botão ID '{id_do_botao}' processado. Dígitos reconstruídos: {digitos_reconstruidos}"
            )
            
            for digito in digitos_do_botao:
                if digito not in mapeamento_numeros_para_id:
                    mapeamento_numeros_para_id[digito] = id_do_botao

        else:
            print(f"  - AVISO: Botão ID '{id_do_botao}' incompleto ({len(digitos_do_botao)}/3). Não mapeado.")

    print("\n--- Mapeamento Concluído ---")
    print(f"Mapa de Dígitos para IDs: {mapeamento_numeros_para_id}")
    return mapeamento_numeros_para_id


def sequencia_de_cliques(wait: WebDriverWait, mapeamento_numeros_para_id: Dict[str, str]):
    for digito_para_clicar in SENHA_ALVO:
        if digito_para_clicar in mapeamento_numeros_para_id:
            id_do_botao_alvo = mapeamento_numeros_para_id[digito_para_clicar]

            try:
                botao_alvo = wait.until(
                    EC.element_to_be_clickable((By.ID, id_do_botao_alvo))
                )
                botao_alvo.click()
                print(f"  - Clicado no botão para o dígito: '{digito_para_clicar}' (ID: {id_do_botao_alvo})")
                sleep(0.5)
            except Exception as e:
                print(
                    f"Erro ao tentar clicar no botão para o dígito '{digito_para_clicar}': {e}"
                )
                break
        else:
            print(
                f"ERRO: O dígito '{digito_para_clicar}' não foi encontrado no teclado."
            )
            break

    # Botão Acessar
    btn_acessar = wait.until(
        EC.element_to_be_clickable((By.ID, "ctl00_cphLogin_btnAcessoConta"))
    )
    sleep(2)
    btn_acessar.click()



def extrair_data_hora_da_pagina(wait: WebDriverWait) -> str:
    """
    Extrai a data e a hora do relatório da página para usar no nome do arquivo.
    Ex: '02/10/2025 às 02 horas' -> '02102025_0200'
    """
    try:
        # Localizador do parágrafo que contém a informação de atualização (baseado na imagem)
        xpath_info = "//app-aviso-titulos/div/p"
        
        print("Buscando informação de data/hora do relatório...")
        
        # 1. Espera o elemento de texto ficar visível
        elemento_data = wait.until(
            EC.visibility_of_element_located((By.XPATH, xpath_info))
        )
        
        texto_completo = elemento_data.text
        print(f"Texto encontrado: '{texto_completo.split('.')[0]}'")
        
        
        match = re.search(r'(\d{2}/\d{2}/\d{4}) [àá]s (\d{2}) horas', texto_completo)
        
        if match:
            data_str = match.group(1) # Ex: 02/10/2025
            hora_str = match.group(2) # Ex: 02
            
            hora_24h = convert_to_24h(hora_str, None)
            
            data_formatada = data_str.replace('/', '-') # Remove as barras
            nome_arquivo_base = f"{data_formatada}_{hora_24h}00" 
            print(f"Nome de arquivo base gerado: {nome_arquivo_base}")
            return nome_arquivo_base
        else:
            print("Padrão de data e hora não encontrado no texto.")
            return datetime.now().strftime("%d%m%Y_%H%M") # Nome de arquivo de fallback
            
    except Exception as e:
        print(f"Erro ao extrair data/hora: {e}")
        return datetime.now().strftime("%d%m%Y_%H%M") # Nome de arquivo de fallback


def convert_to_24h(hora_str: str, periodo_str: str | None) -> str:
    """
    Converte uma hora no formato 12h para 24h. Se o período (AM/PM)
    for ambíguo, infere o mais recente que já ocorreu no dia.
    """
    from datetime import datetime
    import re

    hora_int = int(hora_str)

    # Prioridade 1: Período explícito (AM/PM) no texto
    if periodo_str:
        is_pm = re.search(r'PM|da tarde|da noite', periodo_str, re.IGNORECASE)
        is_am = re.search(r'AM|da manhã', periodo_str, re.IGNORECASE)

        if is_pm:
            return f"{hora_int + 12:02d}" if hora_int != 12 else "12"
        elif is_am:
            return "00" if hora_int == 12 else f"{hora_int:02d}"

    # Prioridade 2: Inferência baseada na hora do sistema (quando não há AM/PM)
    hora_atual_sistema = datetime.now().hour

    # Caso especial para "12 horas"
    if hora_int == 12:
        # Se a hora atual for muito cedo (0h ou 1h), "12 horas" provavelmente é a meia-noite que passou.
        if hora_atual_sistema <= 1:
            print("Alerta: Hora '12h' ambígua. Inferindo como 00h (meia-noite).")
            return "00"
        # Em todos os outros casos, é mais provável que seja meio-dia.
        else:
            print("Alerta: Hora '12h' ambígua. Inferindo como 12h (meio-dia).")
            return "12"

    # Lógica principal para horas de 1 a 11 (seu caso)
    hora_pm_potencial = hora_int + 12

    # Se a versão PM da hora do relatório (ex: 23h para "11h") for maior que a hora atual (ex: 12h),
    # significa que a hora PM ainda não aconteceu hoje. Portanto, a hora correta só pode ser a AM.
    if hora_pm_potencial > hora_atual_sistema:
        # Seu cenário: 11 (hora_int) -> 23 (hora_pm_potencial). 12 (hora_atual_sistema).
        # 23 > 12 é VERDADEIRO, então a função retorna "11" (correto).
        return f"{hora_int:02d}"
    else:
        # Cenário alternativo: hora do sistema é 15h, hora do relatório é 2h.
        # 2 (hora_int) -> 14 (hora_pm_potencial). 15 (hora_atual_sistema).
        # 14 > 15 é FALSO, então a função retorna "14" (2h da tarde, correto).
        return f"{hora_pm_potencial:02d}"


def baixar_excel(driver: WebDriver, wait: WebDriverWait, nome_arquivo_base: str) -> str | Path:
    print("Iniciando processo de download do Excel...")
    
    # Define o nome que o arquivo final deverá ter
    novo_nome_arquivo = f"TitulosRecebidos_{nome_arquivo_base}.xlsx"
    caminho_arquivo_alvo = PATH_DOWNLOAD / novo_nome_arquivo
    
    # 1. Limpa downloads anteriores com o mesmo nome, se existirem
    if caminho_arquivo_alvo.exists():
        os.remove(caminho_arquivo_alvo)
        print(f"Arquivo anterior '{novo_nome_arquivo}' removido.")

    try:
        xpath_do_botao = "//d-button[@id='btnExportarExcel']/button"

        # 2. Aguarda e clica no botão de exportação
        print("Aguardando o botão de exportação ficar disponível...")
        button_element = wait.until(
            EC.presence_of_element_located((By.XPATH, xpath_do_botao))
        )
        print("Clicando para exportar Excel...")
        driver.execute_script("arguments[0].click();", button_element)

        # 3. Lógica para esperar o download (mantida)
        timeout = time.time() + 60 # Aumentei o timeout para 60 segundos por segurança
        download_completo = False
        print("Aguardando o arquivo ser baixado...")
        
        # O nome padrão do arquivo pode variar, mas assumiremos que será um XLSX
        arquivo_baixado = None
        
        while time.time() < timeout:
            arquivos_xlsx = list(PATH_DOWNLOAD.glob("*.xlsx"))
            arquivos_temp = list(PATH_DOWNLOAD.glob("*.crdownload"))

            # Verifica se algum arquivo .xlsx foi baixado e se não há downloads em andamento
            if arquivos_xlsx and not arquivos_temp:
                # Vamos assumir o arquivo mais recente ou o único .xlsx
                arquivo_baixado = max(arquivos_xlsx, key=os.path.getctime)
                print(f"Download concluído! Arquivo temporário: '{arquivo_baixado.name}'")
                download_completo = True
                break
            sleep(1)
        
        # 4. Renomear o arquivo
        if download_completo and arquivo_baixado:
            # Renomeia o arquivo baixado para o nome desejado
            os.rename(arquivo_baixado, caminho_arquivo_alvo)
            print(f"Arquivo renomeado para: '{novo_nome_arquivo}'")

        if not download_completo:
            print("O tempo para download esgotou e o arquivo não foi encontrado.")
            
        return caminho_arquivo_alvo

    except Exception as e:
        print(f"Erro ao tentar baixar o arquivo: {e}")

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


def insere_codigo_usuario(driver: WebDriver, wait: WebDriverWait):
    # Aba Para sa Empresa
    wait.until(EC.element_to_be_clickable((By.ID, "tabSuaEmpresa"))).click()
    
    # Código de Usuário
    wait.until(
        EC.visibility_of_element_located((By.ID, "ctl00_cphLogin_txtCodigoUsuario"))
    ).send_keys("00014736203")
    
    wait.until(
        EC.element_to_be_clickable(
            (By.ID, "ctl00_cphLogin_btnPesquisaCodigoUsuarioPessoaJuridica")
        )
    ).click()


def run_dayconnect_automation():
    PATH_DOWNLOAD.mkdir(exist_ok=True)
    
    from chromedriver.ChromeDriver import ChromeDriver
    
    chrome_driver = ChromeDriver(download_path=PATH_DOWNLOAD)
    driver, wait = chrome_driver.start_driver()
    
    try:
        login(driver, wait)

        sleep(3)

        driver.get(URL_PREVIA_DE_TITULOS)
        sleep(5)
        nome_excel_base = extrair_data_hora_da_pagina(wait)

        caminho_arquivo = baixar_excel(driver, wait, nome_excel_base)

        if caminho_arquivo and os.path.exists(caminho_arquivo):
            logging.info(f"Arquivo baixado com sucesso: {caminho_arquivo}")
            logging.info(f"Iniciando fase de UPLOAD")
            
            process_and_upload(caminho_arquivo, nome_excel_base)
        else:
            logging.error("Não foi possível processar o arquivo. Download falhou ou caminho inválido.")
            
    except Exception as e:
        print(f"Ocorreu um erro durante a automação: {e}")
        # Tira um screenshot em caso de erro para facilitar o debug
        driver.save_screenshot(str(CWD / "error_screenshot.png"))
        raise

        
    finally:
        sleep(15)
        driver.quit()


if __name__ == "__main__":
    run_dayconnect_automation()