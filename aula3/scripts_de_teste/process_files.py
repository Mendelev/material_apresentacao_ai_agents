import os
import shutil
import pandas as pd
import logging
from datetime import datetime
import tempfile

# --- Configurações ---
SOURCE_DIR = "/home/yuri/fs-agente-vendas/scripts_de_teste"
DEST_DIR = "/home/yuri/fs-agente-vendas/scripts_de_teste"
TARGET_FILENAME = "precofixo-de-para.csv"
LOG_DIR = "/home/yuri/fs-agente-vendas/scripts_de_teste"
LOG_FILE = os.path.join(LOG_DIR, "processor.log")
TARGET_SHEET_NAME = "E-mails Clientes" # Nome exato da aba que você quer processar
# --- Fim das Configurações ---

def setup_logging():
    """Configura o logging para o script."""
    os.makedirs(LOG_DIR, exist_ok=True)
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def process_single_file(filename, source_file_path, destination_file_path):
    file_name_lower = filename.lower()
    processed_successfully = False
    operation_stage = "iniciando"

    try:
        if file_name_lower.endswith((".xlsx", ".xls")):
            logging.info(f"Processando arquivo Excel: {filename}")
            logging.info(f"CWD: {os.getcwd()}, Gravável? {os.access(os.getcwd(), os.W_OK)}")
            logging.info(f"Dir Temp: {tempfile.gettempdir()}, Gravável? {os.access(tempfile.gettempdir(), os.W_OK)}")

            operation_stage = f"lendo aba '{TARGET_SHEET_NAME}' do arquivo Excel '{filename}' como string"
            logging.info(f"Tentando ler a aba '{TARGET_SHEET_NAME}' do arquivo Excel: {filename}")

            try:
                excel_df = pd.read_excel(source_file_path,
                                         sheet_name=TARGET_SHEET_NAME, # Especifica a aba a ser lida
                                         engine='openpyxl',
                                         dtype=str, # Lê todas as colunas como string
                                         keep_default_na=False) # Células vazias viram strings vazias ""
            except ValueError as ve:
                if "Worksheet named" in str(ve) and f"'{TARGET_SHEET_NAME}'" in str(ve): # Verifica se o erro é de aba não encontrada
                    logging.error(f"ERRO CRÍTICO: A aba '{TARGET_SHEET_NAME}' não foi encontrada no arquivo '{filename}'. Verifique o nome exato da aba no arquivo Excel.")
                    logging.warning(f"Pulando o processamento do arquivo '{filename}' devido à aba não encontrada.")
                    return False # Retorna False para indicar que o processamento deste arquivo falhou
                else:
                    logging.error(f"Erro de valor ao ler o Excel '{filename}' (aba '{TARGET_SHEET_NAME}'): {ve}", exc_info=True)
                    raise # Re-lança outras ValueErrors para serem pegas pelo bloco except geral
            except FileNotFoundError:
                logging.error(f"ERRO CRÍTICO: Arquivo Excel '{source_file_path}' não encontrado durante a leitura.")
                return False


            logging.info(f"Aba '{TARGET_SHEET_NAME}' do Excel '{filename}' lida. Shape (linhas, colunas): {excel_df.shape}")
            logging.info(f"Número de linhas lidas (DataFrame): {len(excel_df)}") # Pandas não inclui o header na contagem de len() se header é identificado.

            # Se o DataFrame estiver vazio após ler a aba correta, pode não haver dados ou apenas cabeçalho.
            if excel_df.empty and excel_df.columns.empty:
                 logging.warning(f"A aba '{TARGET_SHEET_NAME}' no arquivo '{filename}' parece estar vazia ou não tem um cabeçalho reconhecível e nenhum dado.")
            elif excel_df.empty:
                 logging.warning(f"A aba '{TARGET_SHEET_NAME}' no arquivo '{filename}' foi lida, mas não contém linhas de dados (apenas cabeçalho?).")


            # Bloco opcional para converter 'Quantidade Testemunhas' e 'Quantidade Clientes' para Int64
            # Se o CSV final puder ter esses valores como strings (ex: "1"), você pode comentar/remover este bloco.
            cols_para_inteiro = ['Quantidade Testemunhas', 'Quantidade Clientes']
            for col in cols_para_inteiro:
                if col in excel_df.columns:
                    try:
                        excel_df[col] = pd.to_numeric(excel_df[col], errors='coerce').astype('Int64')
                        logging.info(f"Coluna '{col}' em '{filename}' (aba '{TARGET_SHEET_NAME}') convertida para Int64.")
                    except Exception as e_conv_int:
                        logging.warning(f"Não foi possível converter coluna '{col}' para Int64: {e_conv_int}. Mantendo como string.")


            operation_stage = f"convertendo '{filename}' (aba '{TARGET_SHEET_NAME}') para CSV e salvando em '{destination_file_path}'"
            excel_df.to_csv(destination_file_path, index=False, encoding='utf-8-sig')
            logging.info(f"Convertido '{filename}' (aba '{TARGET_SHEET_NAME}') para CSV e salvo como {destination_file_path}")

            operation_stage = f"removendo arquivo original Excel '{source_file_path}'"
            os.remove(source_file_path)
            logging.info(f"Removido arquivo Excel original: {source_file_path}")
            processed_successfully = True

        elif file_name_lower.endswith(".csv"):
            # Se o arquivo original já for CSV, ele simplesmente o move.
            # Isso não se aplica se o usuário sempre envia Excel, mas mantém a flexibilidade.
            operation_stage = f"movendo arquivo CSV '{filename}' para '{destination_file_path}'"
            logging.info(f"Processando arquivo CSV (movendo diretamente): {filename}")
            shutil.move(source_file_path, destination_file_path)
            logging.info(f"Movido e renomeado {filename} para {destination_file_path}")
            processed_successfully = True
        else:
            logging.info(f"Ignorando tipo de arquivo não suportado: {filename}")

    except Exception as e:
        logging.error(f"ERRO INESPERADO durante a operação '{operation_stage}' para o arquivo {filename}: {e}", exc_info=True)

    return processed_successfully

def main():
    """Função principal para encontrar e processar os arquivos."""
    setup_logging()
    logging.info("==== Script de processamento de arquivos SFTP iniciado ====")

    if not os.path.exists(SOURCE_DIR):
        logging.warning(f"Diretório de origem {SOURCE_DIR} não existe. Saindo.")
        return

    if not os.path.exists(DEST_DIR):
        logging.error(f"Diretório de destino {DEST_DIR} não existe. Verifique o caminho ou crie o diretório. Saindo.")
        return

    destination_file_path = os.path.join(DEST_DIR, TARGET_FILENAME)
    found_files_to_process = []

    for filename in os.listdir(SOURCE_DIR):
        source_file_path = os.path.join(SOURCE_DIR, filename)
        if os.path.isfile(source_file_path):
            # Processa apenas arquivos Excel, já que o objetivo é converter de Excel para CSV da aba específica
            if filename.lower().endswith((".xlsx", ".xls")):
                found_files_to_process.append((filename, source_file_path))
            elif filename.lower().endswith(".csv"):
                # Se um CSV com o nome do Excel for encontrado, talvez seja um upload errado.
                # Ou você pode querer processá-lo de forma diferente. Por ora, vamos focar no Excel.
                logging.info(f"Arquivo CSV encontrado '{filename}', será movido diretamente se não houver Excel prioritário (lógica atual processa um por um).")
                # Para simplificar, vamos priorizar o fluxo Excel->CSV. Se um CSV puro precisar ser manuseado
                # com o mesmo nome alvo, a lógica precisaria de mais refinamento.
                # A lógica atual de loop processará este CSV se for o único ou após um Excel.
                found_files_to_process.append((filename, source_file_path)) # Adiciona CSV para ser movido
            else:
                logging.info(f"Arquivo ignorado (tipo não suportado na varredura inicial): {filename}")

    if not found_files_to_process:
        logging.info("Nenhum arquivo Excel ou CSV encontrado para processar.")
    else:
        logging.info(f"Arquivos encontrados para potencial processamento: {[f[0] for f in found_files_to_process]}")
        # A lógica atual processa todos os arquivos encontrados, um por um.
        # O último arquivo processado com sucesso se tornará o "precofixo-de-para.csv".
        for filename, source_file_path in found_files_to_process:
            logging.info(f"Tentando processar: {filename}")
            process_single_file(filename, source_file_path, destination_file_path)

    logging.info("==== Script de processamento de arquivos SFTP finalizado ====")

if __name__ == "__main__":
    main()