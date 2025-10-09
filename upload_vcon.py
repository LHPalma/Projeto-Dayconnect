from typing import Any, Dict, List

from ServiceRoutineClimbLowOccurence import ServiceRoutineClimbLowOccurence

def fazer_upload_vcon(arquivos_para_upload: List[Dict[str, Any]]):
    """
    Recebe uma lista de dicionários e realiza o upload de cada arquivo.
    """
    if not arquivos_para_upload:
        print("\nNenhum arquivo para fazer upload.")
        return

    print("\n--- INICIANDO ROTINA DE UPLOAD ---")
    try:
        rotina_upload = ServiceRoutineClimbLowOccurence()
        rotina_upload.login_vcom()

        # Corrigido o loop para iterar sobre uma lista de dicionários
        for item in arquivos_para_upload:
            caminho_arquivo = item['arquivo']
            credor = item['credor']
            rotina_upload.import_carga_vcom(credor, caminho_arquivo)
        
        print("\n--- ROTINA DE UPLOAD CONCLUÍDA ---")

    except NameError:
        print("ERRO: A classe 'ServiceRoutineClimbLowOccurence' não está disponível.")
    except Exception as e:
        print(f"ERRO inesperado durante o processo de upload: {e}")

