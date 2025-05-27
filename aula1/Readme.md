
# Executar com Gemini na CLI
python retention_app.py clientes.json --provider gemini

# Executar com Azure na CLI
python retention_app.py clientes.json --provider azure   # padrão

# Executar com  interface e Gemini
streamlit run retention_app.py -- --ui --provider gemini
