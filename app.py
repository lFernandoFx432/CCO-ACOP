from flask import Flask, request, render_template, send_file
import requests
import json
import datetime
import pandas as pd
import os

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
app.config['CELERY_RESULT_BACKEND'] = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
def comparar_penalizacoes(viagem, lista_mensagens_validas, lista_horarios_nao_batem, data_gestão_de_falhas, horarios_nao_batem_condicao, falha, data_atual):
    mensagens_viagem = [obs["mensagem"] for obs in viagem["mensagemObs"]] if viagem["mensagemObs"] else []
    analistas = [obs["usuarioCriacao"]["nome"] for obs in viagem["mensagemObs"]] if viagem["mensagemObs"] else []
    mensagem_valida = any(msg in lista_mensagens_validas for msg in mensagens_viagem)
    data_atual_convertida = datetime.datetime.strptime(data_atual, "%Y-%m-%d").strftime("%d-%m-%Y")
    if not mensagem_valida or not viagem["mensagemObs"]:
        lista_horarios_nao_batem = {
            "Data": data_atual_convertida,
            "numeroLinha": viagem["trajeto"]["numeroLinha"],
            "partidaPlan": viagem["partidaPlan"],
            "Ocorrencia no gestão de falhas": falha["tipoFalha"]["descricaoFalha"],
            "Observação de viagens": mensagens_viagem,
            "Analsita": analistas,
        }
        #print(f"Adicionado: {lista_horarios_nao_batem}") 
        horarios_nao_batem_condicao.append(lista_horarios_nao_batem)
@celery.task
def processar_viagens(data_atual, token_autorizacao):
    data_inicial = f"{data_atual}"
    data_final = f"{data_atual}"

    url_get = 'https://zn4.sinopticoplus.com/service-api/linhasTrajetos/1335'
    url_put = 'https://zn4-viagem-planejamento-api.sinopticoplus.com/viagem-planejamento-api/v1/planejamentoViagem/1335'
    url_get_gestao_de_falha = f'https://zn4.sinopticoplus.com/api/gestaoFalhas/ocorrenciaFalha/1335/null/2499,2500,2497,2501,2495,2496,2502,2498/null/{data_inicial}%2000:00/{data_final}%2023:59?gmtCliente=%22America/Manaus%22'
    
    headers = {
        'Authorization': f'Bearer {token_autorizacao}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url_get, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        print(f"Erro na requisição GET de trajetos: {e}")
        data = []

    try:
        response_gestao_de_falhas = requests.get(url_get_gestao_de_falha, headers=headers)
        response_gestao_de_falhas.raise_for_status()
        data_gestão_de_falhas = response_gestao_de_falhas.json()
    except requests.RequestException as e:
        print(f"Erro na requisição GET de gestão de falhas: {e}")
        data_gestão_de_falhas = []

    linha_ids = list(set(entrada["linhaId"] for entrada in data_gestão_de_falhas))
    linha_idhorario = list(set(entrada["horarioId"] for entrada in data_gestão_de_falhas))

    mensagens_gestao_de_falhas = [
        "ATRASO OPERADOR", "ATRASO VEÍCULO", "BAIXADA POR FALTA DE VEÍCULO/OPERADOR", "CAD. DO VEÍCULO DIVERGENTE",
        "CATEGORIA DIFERENTE DO PROGRAMADO", "ERRO DE SELEÇAO DE LINHA", "FORA DE ROTA", "GPS INTERMITENTE",
        "PROBLEMA EM PERCURSO IDA OU VOLTA", "QUEIMA DE PARADA", "RETORNO TÉCNICO", "T.U NÃO RECOLHEU",
        "VEÍCULO OCIOSO/FECHADO", "VEÍCULO SEM/FALHA TRANSMISSÃO", "VIAGEM ADIANTADA", "VIAGEM FEITA SEM COBRADOR",
        "VNR - MANUTENÇÃO", "VNR - OPERAÇÃO"
    ]

    mensagens_validas = {
        "CATEGORIA DIFERENTE DO PROGRAMADO": [f"0,{i:02d} - CATEGORIA DE VEÍCULO DIFERENTE DO PROGRAMADO" for i in range(1, 51)] + ["1,00 - CATEGORIA DE VEÍCULO DIFERENTE DO PROGRAMADO"],
        "ATRASO OPERADOR": [
            "0,20 - ATRASO POR FALTA DE OPERADOR", "0,20 - ATRASO POR TER ULTRAPASSADO OS 10 MIN DE INTERVALO",
            "0,20 - SAIU COM ATRASO NA VIAGEM ANTERIOR POR FALTA DE OPERADOR", "0,40 - ATRASO POR FALTA DE OPERADOR",
            "0,40 - ATRASO POR TER ULTRAPASSADO OS 10 MIN DE INTERVALO", "0,40 - SAIU COM ATRASO NA VIAGEM ANTERIOR POR FALTA DE OPERADOR",
            "1,00 - ATRASO POR FALTA DE OPERADOR"
        ],
        "ATRASO VEÍCULO": [
            "0,20 - ATRASO POR FALTA DE VEÍCULO", "0,20 - SAIU COM ATRASO NA VIAGEM ANTERIOR POR FALTA DE VEÍCULO",
            "0,40 - ATRASO POR FALTA DE VEÍCULO", "0,40 - SAIU COM ATRASO NA VIAGEM ANTERIOR POR FALTA DE VEÍCULO",
            "1,00 - ATRASO POR FALTA DE VEÍCULO"
        ],
        "BAIXADA POR FALTA DE VEÍCULO/OPERADOR": [
            "0,50 - BAIXADA POR FALTA DE OPERADOR", "0,50 - BAIXADA POR FALTA DE VEÍCULO"
        ],
        "CAD. DO VEÍCULO DIVERGENTE": [
            "1,00 - CAD. DO VEÍCULO DIVERGENTE", "1,00 - ID DIVERGENTE", "1,00 - VEÍCULO INATIVO", "1,00 - VEÍCULO SEM CADASTRO"
        ],
        "ERRO DE SELEÇAO DE LINHA": ["1,00 - ERRO DE SELEÇÃO DE LINHA"],
        "FORA DE ROTA": [
            "0,50 - FORA DE ROTA - IDA", "0,50 - FORA DE ROTA - VOLTA", "1,00 - FORA DE ROTA - IDA E VOLTA"
        ],
        "GPS INTERMITENTE": ["1,00 - GPS INTERMITENTE",""],
        "PROBLEMA EM PERCURSO IDA OU VOLTA": [
            "0,50 - PROBLEMA NA IDA C/ SUBST. NA VOLTA", "0,50 - VEÍCULO QUEBROU EM PERCURSO POR PROBLEMA MECÂNICO - VOLTA",
            "0,50 - VEÍCULO RECOLHEU PARA GARAGEM"
        ],
        "QUEIMA DE PARADA": [
            "0,50 - QUEIMA DE PARADA - IDA", "0,50 - QUEIMA DE PARADA - VOLTA", "1,00 - QUEIMA DE PARADA - IDA E VOLTA"
        ],
        "RETORNO TÉCNICO": [
            "0,50 - RETORNO TÉCNICO POR FALTA DE OPERADOR", "0,50 - RETORNO TÉCNICO POR FALTA DE VEÍCULO"
        ],
        "T.U NÃO RECOLHEU": ["1,00 - VEÍCULO DO TU NÃO FOI RECOLHIDO"],
        "VEÍCULO OCIOSO/FECHADO": [
            "0,20 - VEÍCULO OCIOSO/FECHADO - IDA", "0,50 - VEÍCULO OCIOSO/FECHADO - IDA",
            "0,50 - VEÍCULO OCIOSO/FECHADO - VOLTA", "1,00 - VEÍCULO OCIOSO/FECHADO - IDA E VOLTA"
        ],
        "VEÍCULO SEM/FALHA TRANSMISSÃO": ["1,00 - FALHA DE TRANSMISSÃO",""],
        "VIAGEM ADIANTADA": ["0,50 - ADIANTAMENTO MAIOR QUE 5 MIN"],
        "VIAGEM FEITA SEM COBRADOR": [f"0,{i:02d} - VIAGEM FEITA SEM COBRADOR" for i in range(1, 51)] + ["1,00 - VIAGEM FEITA SEM COBRADOR"],
        "VNR - MANUTENÇÃO": [
            "0,50 - ACIDENTE / AGUARDANDO O ENVIO DO B.O/IMAGENS", "0,50 - ASSALTO / AGUARDANDO O ENVIO DO B.O/IMAGENS",
            "1,00 - ACIDENTE / AGUARDANDO O ENVIO DO B.O/IMAGENS", "1,00 - ASSALTO / AGUARDANDO O ENVIO DO B.O/IMAGENS",
            "1,00 - BAIXADA ADIANTADA OU ATRASADA POR FALTA DE VEÍCULO", "1,00 - BAIXADA FORA DE HORÁRIO",
            "1,00 - NÃO SAIU POR FALTA DE VEÍCULO", "1,00 - RETORNO TÉCNICO ADIANTADO OU ATRASado POR FALTA DE VEÍCULO",
            "1,00 - VEÍCULO QUEBROU EM PERCURSO POR PROBLEMA MECÂNICO - IDA"
        ],
        "VNR - OPERAÇÃO": [
            "1,00 - BAIXADA ADIANTADA OU ATRASADA POR FALTA DE OPERADOR", "1,00 - BAIXADA NÃO CUMPRIU O PERCURSO",
            "1,00 - NÃO SAIU POR FALTA DE OPERADOR", "1,00 - RETORNO TÉCNICO ADIANTADO OU ATRASADO POR FALTA DE OPERADOR",
            "1,00 - RETORNO TÉCNICO FORA DE HORÁRIO"
        ]
    }

    horarios_nao_batem_condicao = []
    for linha_id in linha_ids:
        trajeto_filtrado = next((trajeto for trajeto in data if trajeto.get('_id') == linha_id), None)
        if not trajeto_filtrado:
            continue

        novo_trajetos = {"trajetos": trajeto_filtrado['trajetos']}
        payload = {
            "agrupamentos": [],
            "dataFim": data_atual,
            "dataInicio": data_atual,
            "empresas": [2499, 2500, 2497, 2501, 2495, 2496, 2502, 2498],
            "horaFim": "23:59:59",
            "horaInicio": "00:00:00",
            "idCliente": 1335,
            "ordenacao": "horario",
            "timezone": "America/Manaus",
            "trajetos": novo_trajetos['trajetos'],
        }

        try:
            response_put = requests.put(url_put, headers=headers, data=json.dumps(payload))
            response_put.raise_for_status()
            dashboard = response_put.json()
        except requests.RequestException as e:
            print(f"Erro na requisição PUT de planejamento de viagem: {e}")
            continue
        except json.JSONDecodeError:
            print("Erro ao decodificar o JSON da resposta PUT. Resposta recebida:")
            print(f"Status Code: {response_put.status_code}")
            print(response_put.text)
            continue

        viagens = dashboard["viagens"]
        for viagem in viagens:
            if viagem["idHorario"] in linha_idhorario:
                for falha in data_gestão_de_falhas:
                    if falha["horarioId"] == viagem["idHorario"]:
                        descricao_falha = falha["tipoFalha"]["descricaoFalha"]
                        if descricao_falha in mensagens_validas:
                            comparar_penalizacoes(
                                viagem, mensagens_validas[descricao_falha], horarios_nao_batem_condicao, data_gestão_de_falhas, horarios_nao_batem_condicao, falha, data_atual
                            )

    return horarios_nao_batem_condicao

def gerar_intervalo_datas(data_inicio, data_fim):
    data_inicio = datetime.datetime.strptime(data_inicio, '%d/%m/%Y')
    data_fim = datetime.datetime.strptime(data_fim, '%d/%m/%Y')
    intervalo = []
    while data_inicio <= data_fim:
        intervalo.append(data_inicio.strftime('%Y-%m-%d'))
        data_inicio += datetime.timedelta(days=1)
    return intervalo

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/processar', methods=['POST'])
def processar():
    token_autorizacao = request.form['token']
    data_inicio = request.form['data_inicio']
    data_fim = request.form['data_fim']
    
    
    intervalo_datas = gerar_intervalo_datas(data_inicio, data_fim)
    resultados_acumulados = []

    for data_atual in intervalo_datas:
        resultados_dia = processar_viagens(data_atual, token_autorizacao)
        resultados_acumulados.extend(resultados_dia)

    df_horarios_nao_batem_condicao = pd.DataFrame(resultados_acumulados)

    # Clean DataFrame from unwanted spaces and newlines
    df_horarios_nao_batem_condicao.replace(r'\n', '', regex=True, inplace=True)
    df_horarios_nao_batem_condicao.replace(r'\r', '', regex=True, inplace=True)
    df_horarios_nao_batem_condicao.replace(r'\s+', ' ', regex=True, inplace=True)

    file_path = os.path.join(os.getcwd(), "horarios_nao_batem_condicao.xlsx")
    df_horarios_nao_batem_condicao.to_excel(file_path, index=False, engine='openpyxl')

    # Convert DataFrame to HTML
    table_html = df_horarios_nao_batem_condicao.to_html(classes='data', index=False)

    # Render the complete template with the DataFrame HTML
    return render_template('complete.html', tables=table_html, download_link='/download_excel')

@app.route('/complete')
def complete():
    return render_template('complete.html')

@app.route('/download_excel')
def download_excel():
    file_path = os.path.join(os.getcwd(), "horarios_nao_batem_condicao.xlsx")
    return send_file(file_path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
