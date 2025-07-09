import os

# Lista dos arquivos a serem incluídos, com caminhos relativos à raiz do projeto
files_to_consolidate = [
    "src/agents/extraction_agent.py",
    "src/agents/mapping_agent.py",
    "src/agents/orchestration_agent.py",
    "src/prompts/extraction_prompt.txt",
    "src/utils/formatting.py",
    "src/utils/normalization.py",
    "src/app.py",
    "src/config.py",
    "src/topdesk_client.py",
    "src/whatsapp_integration.py",
    #"src/utils/transcription.py",
    # "src/test_regression_audio.py",
    # "src/test_regression.py"
]

# Nome do arquivo de saída
output_filename = "consolidated_project_code.txt"

print(f"Iniciando a consolidação do código para o arquivo: {output_filename}")

# Abre o arquivo de saída em modo de escrita ('w'), sobrescrevendo se já existir
try:
    with open(output_filename, 'w', encoding='utf-8') as outfile:
        # Itera sobre cada arquivo na lista
        for filepath in files_to_consolidate:
            print(f"Processando: {filepath}")
            # Escreve o cabeçalho indicando o arquivo
            outfile.write(f"--- Conteúdo de {filepath} ---\n\n")

            try:
                # Tenta abrir e ler o arquivo de origem
                with open(filepath, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    # Escreve o conteúdo lido no arquivo de saída
                    outfile.write(content)
                    # Adiciona duas linhas em branco para separar do próximo arquivo
                    outfile.write("\n\n")
            except FileNotFoundError:
                # Se o arquivo não for encontrado, registra um erro no arquivo de saída
                error_message = f"*** ERRO: Arquivo não encontrado: {filepath} ***\n\n"
                print(f"   AVISO: Arquivo não encontrado: {filepath}")
                outfile.write(error_message)
            except Exception as e:
                # Captura outros possíveis erros de leitura
                error_message = f"*** ERRO: Falha ao ler o arquivo {filepath}: {e} ***\n\n"
                print(f"   ERRO: Falha ao ler {filepath}: {e}")
                outfile.write(error_message)

    print(f"\nConsolidação concluída com sucesso! Arquivo salvo como: {output_filename}")

except Exception as e:
    # Captura erros ao tentar criar/escrever no arquivo de saída
    print(f"\nERRO FATAL: Não foi possível criar ou escrever no arquivo de saída '{output_filename}': {e}")