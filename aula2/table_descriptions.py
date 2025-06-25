# table_descriptions.py

TABLE_DESCRIPTIONS = {
    "us_precipitation": """
Esta tabela contém dados sobre a precipitação em várias cidades e estações meteorológicas nos Estados Unidos.
Cada linha representa uma estação de medição.

Descrição das colunas principais:
------------------------------------------
id: Identificador único da estação meteorológica no formato dos EUA.
city: A cidade onde a estação está localizada.
station: O nome oficial da estação meteorológica.
average: A precipitação média anual histórica, provavelmente em polegadas (inches). Use esta coluna para perguntas sobre média.
latitude: A coordenada de latitude da estação.
longitude: A coordenada de longitude da estação.
state: A sigla do estado dos EUA (ex: HI para Havaí, WA para Washington, CA para Califórnia).
total19: O total de precipitação registrado especificamente no ano de 2019, em polegadas.
"""
}