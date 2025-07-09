# Integração com whatsapp


Para executar a integração com o whatsapp você deve executar a aplicação pelo entrypoint whatsapp_integration, da seguinte maneira

```Python
cd src
gunicorn -w 1 whatsapp_integration:app
```

para executar DURANTE DESENVOLVIMENTO usar
```
cd src
python -m whatsapp_integration 
```

É necessário um token de acesso o GRAPH_API_TOKEN, atenção o token padrão tem tempo curto de vida, para produção é necessário gerar token permanente

Também é necessário fornecer o WEBHOOK_VERIFY_TOKEN que atua como senha secreta entre a Meta e o app




# Testes de regressão

Entrar na pasta src

Executar 
```
python -m unittest test_regression.py
```

Na pasta tests faça
```
python -m unittest test_topdesk_client
python -m unittest test_whatsapp_integration
```