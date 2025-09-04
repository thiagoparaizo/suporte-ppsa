"""
Rotas para análise de remessas x CCOs
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
import json
import logging
import uuid
from datetime import datetime

from app.config import MONGO_URI
from app.services.remessa_service import RemessaAnaliseService
from app.utils.converters import formatar_data_brasileira
from app.utils.json_encoder import json_response
from app.utils.cache_utils import CacheManager

analise_bp = Blueprint('analise_ui', __name__)

logger = logging.getLogger(__name__)

# Instância do serviço
remessa_service = RemessaAnaliseService(MONGO_URI)

@analise_bp.route('/api/remessa-detalhada-analise/<remessa_id>')
def api_remessa_detalhada_analise(remessa_id):
    """API para obter detalhes completos de uma remessa para análise"""
    try:
        # Buscar remessa completa no banco
        remessa_completa = remessa_service.remessa_repo.buscar_remessa_completa(remessa_id)
        
        if not remessa_completa:
            return jsonify({'error': 'Remessa não encontrada'}), 404
        
        # Processar gastos para estatísticas detalhadas
        gastos = remessa_completa.get('gastos', [])
        
        # Calcular estatísticas dos gastos
        stats_gastos = calcular_estatisticas_gastos(gastos)
        
        # Identificar fases com reconhecimento
        fases_reconhecimento = remessa_service._identificar_fases_reconhecimento_simples(gastos)
        
        detalhes = {
            'informacoes_basicas': {
                'id': remessa_completa['_id'],
                'contratoCPP': remessa_completa.get('contratoCPP', ''),
                'campo': remessa_completa.get('campo', ''),
                'remessa': remessa_completa.get('remessa', 0),
                'remessaExposicao': remessa_completa.get('remessaExposicao', 0),
                'exercicio': remessa_completa.get('exercicio', 0),
                'periodo': remessa_completa.get('periodo', 0),
                'mesAnoReferencia': remessa_completa.get('mesAnoReferencia', ''),
                'faseRemessa': remessa_completa.get('faseRemessa', ''),
                'etapa': remessa_completa.get('etapa', ''),
                'origemDoGasto': remessa_completa.get('origemDoGasto', ''),
                'gastosCompartilhados': remessa_completa.get('gastosCompartilhados', False),
                'usuarioResponsavel': remessa_completa.get('usuarioResponsavel', ''),
                'dataLancamento': formatar_data_brasileira(remessa_completa.get('dataLancamento')),
                'version': remessa_completa.get('version', 0),
                'fatorAlocacao': remessa_completa.get('fatorAlocacao'),
                'uep': remessa_completa.get('uep', ''),
                'processoAdministrativo': remessa_completa.get('processoAdministrativo', '')
            },
            'estatisticas_gastos': stats_gastos,
            'fases_reconhecimento': fases_reconhecimento,
            'top_classificacoes': obter_top_classificacoes(gastos),
            'top_responsaveis': obter_top_responsaveis(gastos),
            'distribuicao_status': obter_distribuicao_status(gastos),
            'moedas_utilizadas': obter_moedas_utilizadas(gastos)
        }
        
        return json_response(detalhes)
        
    except Exception as e:
        logger.error(f"Erro ao obter detalhes da remessa {remessa_id}: {e}")
        return json_response({'error': str(e)}, 500)
    
@analise_bp.route('/api/remessa-original-json/<remessa_id>', methods=['GET'])
def api_remessa_original_json(remessa_id: str):
    """API para obter estatísticas gerais de remessas"""
    try:
        # Buscar remessa completa no banco
        remessa_completa = remessa_service.remessa_repo.buscar_remessa_completa(remessa_id)
        
        return json_response({
            'success': True,
            'remessa': remessa_completa
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas de remessas: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@analise_bp.route('/api/estatisticas-remessas')
def api_estatisticas_remessas():
    """API para obter estatísticas gerais de remessas"""
    try:
        stats = remessa_service.remessa_repo.buscar_estatisticas_basicas()
        
        return json_response({
            'success': True,
            'estatisticas': stats
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas de remessas: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@analise_bp.route('/api/ultimas-remessas')
def api_ultimas_remessas():
    """API para obter últimas remessas cadastradas"""
    try:
        limite = request.args.get('limite', 10, type=int)
        remessas = remessa_service.remessa_repo.buscar_ultimas_remessas(limite)
        
        # Processar dados para exibição
        resultados = []
        for remessa in remessas:
            resultados.append({
                'id': remessa['_id'],
                'contratoCPP': remessa.get('contratoCPP', ''),
                'campo': remessa.get('campo', ''),
                'remessa': remessa.get('remessa', 0),
                'exercicio': remessa.get('exercicio', 0),
                'periodo': remessa.get('periodo', 0),
                'faseRemessa': remessa.get('faseRemessa', ''),
                'dataLancamento': formatar_data_brasileira(remessa.get('dataLancamento')),
                'version': remessa.get('version', 0)
            })
        
        return json_response({
            'success': True,
            'remessas': resultados
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter últimas remessas: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@analise_bp.route('/api/dashboard-personalizado', methods=['POST'])
def api_dashboard_personalizado():
    """API para gerar dashboard personalizado com dados da análise"""
    try:
        dados = request.get_json()
        
        if not dados or 'resultadoAnalise' not in dados:
            return jsonify({'success': False, 'error': 'Dados de análise não fornecidos'}), 400
        
        resultado_analise = dados['resultadoAnalise']
        
        # Processar dados para dashboard
        dashboard_data = processar_dados_dashboard(resultado_analise)
        
        return jsonify({
            'success': True,
            'dashboard': dashboard_data
        })
        
    except Exception as e:
        logger.error(f"Erro ao gerar dashboard personalizado: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@analise_bp.route('/api/exportar-analise-csv', methods=['POST'])
def api_exportar_analise_csv():
    """API para exportar resultado da análise em CSV"""
    try:
        dados = request.get_json()
        
        if not dados or 'resultadoAnalise' not in dados:
            return jsonify({'success': False, 'error': 'Dados de análise não fornecidos'}), 400
        
        resultado_analise = dados['resultadoAnalise']
        formato = dados.get('formato', 'detalhado')  # 'detalhado' ou 'resumido'
        
        # Gerar CSV
        csv_content = gerar_csv_analise(resultado_analise, formato)
        
        return jsonify({
            'success': True,
            'csv_content': csv_content,
            'filename': f"analise_remessas_{formato}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        })
        
    except Exception as e:
        logger.error(f"Erro ao exportar CSV: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@analise_bp.route('/api/validar-filtros-analise', methods=['POST'])
def api_validar_filtros_analise():
    """API para validar filtros antes de executar análise"""
    try:
        dados = request.get_json()
        
        if not dados:
            return jsonify({'success': False, 'error': 'Filtros não fornecidos'}), 400
        
        # Validar filtros obrigatórios
        if not dados.get('contratoCPP'):
            return jsonify({
                'success': False, 
                'error': 'Contrato CPP é obrigatório',
                'campo_erro': 'contratoCPP'
            }), 400
        
        # Construir e testar filtro
        filtro_mongo = remessa_service._construir_filtro_mongo(dados)
        
        # Contar registros que seriam processados
        count = remessa_service.remessa_repo.contar_por_filtros(filtro_mongo)
        
        # Verificar se há gastos com reconhecimento
        filtro_com_reconhecimento = filtro_mongo.copy()
        filtro_com_reconhecimento["gastos.reconhecido"] = "SIM"
        count_com_reconhecimento = remessa_service.remessa_repo.contar_por_filtros(filtro_com_reconhecimento)
        
        # Estimar tempo de processamento (aprox. 1 segundo por 100 remessas)
        tempo_estimado = max(1, count_com_reconhecimento // 100)
        
        validacao = {
            'success': True,
            'filtros_validos': True,
            'total_remessas': count,
            'remessas_com_reconhecimento': count_com_reconhecimento,
            'tempo_estimado_segundos': tempo_estimado,
            'recomendacao': gerar_recomendacao_analise(count, count_com_reconhecimento)
        }
        
        return jsonify(validacao)
        
    except Exception as e:
        logger.error(f"Erro ao validar filtros: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@analise_bp.route('/api/salvar-analise-sessao', methods=['POST'])
def api_salvar_analise_sessao():
    """API para salvar resultado da análise em cache para o dashboard"""
    try:
        dados = request.get_json()
        
        if not dados or 'resultadoAnalise' not in dados:
            return json_response({'success': False, 'error': 'Dados não fornecidos'}, 400)
        
        # Gerar chave única para o cache
        cache_key = str(uuid.uuid4())
        
        # Salvar no cache (TTL de 1 hora)
        cache_manager = CacheManager(scope='user')
        cache_manager.store_data(
            key=f"analise_temp_{cache_key}", 
            value=dados['resultadoAnalise'], 
            timeout=3600
        )
        
        return json_response({
            'success': True, 
            'cache_key': cache_key
        })
        
    except Exception as e:
        logger.error(f"Erro ao salvar análise em cache: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)
    

# Funções auxiliares
def calcular_estatisticas_gastos(gastos):
    """Calcula estatísticas detalhadas dos gastos"""
    from app.utils.converters import converter_decimal128_para_float
    
    total_gastos = len(gastos)
    gastos_reconhecidos = len([g for g in gastos if g.get('reconhecido') == 'SIM'])
    
    valor_total = sum(converter_decimal128_para_float(g.get('valorMoedaOBJReal', 0)) for g in gastos)
    valor_reconhecido = sum(converter_decimal128_para_float(g.get('valorReconhecido', 0)) 
                           for g in gastos if g.get('reconhecido') == 'SIM')
    valor_nao_reconhecido = sum(converter_decimal128_para_float(g.get('valorNaoReconhecido', 0)) for g in gastos)
    
    # Estatísticas por tipo de reconhecimento
    reconhecimento_tipos = {}
    status_tipos = {}
    
    for gasto in gastos:
        rec_tipo = gasto.get('reconhecimentoTipo', 'INDEFINIDO')
        reconhecimento_tipos[rec_tipo] = reconhecimento_tipos.get(rec_tipo, 0) + 1
        
        status = gasto.get('statusGastoTipo', 'INDEFINIDO')
        status_tipos[status] = status_tipos.get(status, 0) + 1
    
    return {
        'totalGastos': total_gastos,
        'gastosReconhecidos': gastos_reconhecidos,
        'gastosPendentes': total_gastos - gastos_reconhecidos,
        'taxaReconhecimento': (gastos_reconhecidos / total_gastos * 100) if total_gastos > 0 else 0,
        'valorTotal': valor_total,
        'valorReconhecido': valor_reconhecido,
        'valorNaoReconhecido': valor_nao_reconhecido,
        'percentualValorReconhecido': (valor_reconhecido / valor_total * 100) if valor_total > 0 else 0,
        'reconhecimentoTipos': reconhecimento_tipos,
        'statusTipos': status_tipos
    }

def obter_top_classificacoes(gastos, top=5):
    """Obtém top classificações dos gastos"""
    from collections import Counter
    
    classificacoes = [g.get('classificacaoGastoTipo', 'INDEFINIDO') for g in gastos if g.get('classificacaoGastoTipo')]
    counter = Counter(classificacoes)
    
    return [{'classificacao': k, 'quantidade': v} for k, v in counter.most_common(top)]

def obter_top_responsaveis(gastos, top=5):
    """Obtém top responsáveis pelos gastos"""
    from collections import Counter
    
    responsaveis = [g.get('responsavel', 'INDEFINIDO') for g in gastos if g.get('responsavel')]
    counter = Counter(responsaveis)
    
    return [{'responsavel': k, 'quantidade': v} for k, v in counter.most_common(top)]

def obter_distribuicao_status(gastos):
    """Obtém distribuição dos gastos por status"""
    from collections import Counter
    
    status = [g.get('statusGastoTipo', 'INDEFINIDO') for g in gastos]
    counter = Counter(status)
    
    return [{'status': k, 'quantidade': v, 'percentual': v/len(gastos)*100} for k, v in counter.items()]

def obter_moedas_utilizadas(gastos):
    """Obtém lista de moedas utilizadas"""
    from collections import Counter
    
    moedas = [g.get('moedaTransacao', 'INDEFINIDO') for g in gastos if g.get('moedaTransacao')]
    counter = Counter(moedas)
    
    return [{'moeda': k, 'quantidade': v} for k, v in counter.items()]

def processar_dados_dashboard(resultado_analise):
    """Processa dados para dashboard personalizado"""
    return {
        'resumo': {
            'totalRemessas': resultado_analise['estatisticas']['totalRemessas'],
            'totalFases': resultado_analise['estatisticas']['totalFasesEncontradas'],
            'ccosEncontradas': resultado_analise['estatisticas']['totalCCOsEncontradas'],
            'taxaEncontro': (resultado_analise['estatisticas']['totalCCOsEncontradas'] / 
                           resultado_analise['estatisticas']['totalFasesEncontradas'] * 100) if resultado_analise['estatisticas']['totalFasesEncontradas'] > 0 else 0
        },
        'distribuicaoFases': resultado_analise['estatisticas']['fasesPorTipo'],
        'timeline': extrair_timeline_dados(resultado_analise['remessasAnalisadas']),
        'consolidacao': resultado_analise['estatisticas'].get('consolidacaoGeral', {})
    }

def extrair_timeline_dados(remessas_analisadas):
    """Extrai dados para timeline do dashboard"""
    timeline_data = []
    
    for remessa in remessas_analisadas:
        for fase in remessa['fasesComReconhecimento']:
            if fase.get('dataReconhecimento'):
                timeline_data.append({
                    'data': fase['dataReconhecimento'],
                    'remessa': remessa['remessa'],
                    'fase': fase['fase'],
                    'contrato': remessa['contratoCPP'],
                    'campo': remessa['campo'],
                    'cco_status': fase.get('cco', {}).get('statusCCO', 'SEM_DADOS')
                })
    
    # Ordenar por data
    timeline_data.sort(key=lambda x: x['data'])
    
    return timeline_data

def gerar_csv_analise(resultado_analise, formato='detalhado'):
    """Gera conteúdo CSV da análise"""
    import csv
    import io
    
    output = io.StringIO()
    
    if formato == 'resumido':
        # CSV resumido - apenas estatísticas principais
        writer = csv.writer(output, delimiter=';')
        writer.writerow(['Métrica', 'Valor'])
        writer.writerow(['Total de Remessas', resultado_analise['estatisticas']['totalRemessas']])
        writer.writerow(['Total de Fases', resultado_analise['estatisticas']['totalFasesEncontradas']])
        writer.writerow(['CCOs Encontradas', resultado_analise['estatisticas']['totalCCOsEncontradas']])
        writer.writerow(['CCOs Não Encontradas', resultado_analise['estatisticas']['totalCCOsNaoEncontradas']])
        writer.writerow(['CCOs Duplicadas', resultado_analise['estatisticas']['totalCCOsDuplicadas']])
        
        # Distribuição por fase
        writer.writerow([])
        writer.writerow(['Fase', 'Quantidade'])
        for fase, count in resultado_analise['estatisticas']['fasesPorTipo'].items():
            writer.writerow([fase, count])
            
    else:
        # CSV detalhado - linha por fase
        writer = csv.writer(output, delimiter=';')
        headers = [
            'Remessa ID', 'Contrato', 'Campo', 'Remessa', 'Exercicio', 'Periodo',
            'Mes Ano Ref', 'Fase', 'Data Reconhecimento', 'CCO Status', 'CCO ID',
            'Valor Reconhecido Fase', 'Valor CCO', 'Overhead Total', 'Observacao'
        ]
        writer.writerow(headers)
        
        for remessa in resultado_analise['remessasAnalisadas']:
            for fase in remessa['fasesComReconhecimento']:
                cco = fase.get('cco', {})
                row = [
                    remessa['id'],
                    remessa['contratoCPP'],
                    remessa['campo'],
                    remessa['remessa'],
                    remessa['exercicio'],
                    remessa['periodo'],
                    remessa['mesAnoReferencia'],
                    fase['fase'],
                    fase.get('dataReconhecimento', ''),
                    cco.get('statusCCO', 'SEM_DADOS'),
                    cco.get('id', ''),
                    fase.get('valorReconhecido', 0),
                    cco.get('valorReconhecidoComOH', 0),
                    cco.get('overHeadTotal', 0),
                    cco.get('observacao', '')
                ]
                writer.writerow(row)
    
    return output.getvalue()

def gerar_recomendacao_analise(total_remessas, remessas_com_reconhecimento):
    """Gera recomendação baseada no volume de dados"""
    if remessas_com_reconhecimento == 0:
        return "AVISO: Nenhuma remessa com reconhecimento encontrada. Verifique os filtros aplicados."
    elif remessas_com_reconhecimento > 1000:
        return "ATENÇÃO: Volume alto de dados. Considere refinar os filtros para melhor performance."
    elif remessas_com_reconhecimento > 500:
        return "Volume moderado de dados. A análise pode levar alguns minutos."
    else:
        return "Volume adequado para análise rápida."

@analise_bp.route('/analise-remessas')
def analise_remessas():
    """Página principal de análise de remessas x CCOs"""
    return render_template('analise_remessas.html', 
                         titulo="Análise de Remessas x CCOs")

@analise_bp.route('/api/contratos-remessas')
def api_contratos_remessas():
    """API para listar contratos disponíveis em remessas"""
    try:
        contratos = remessa_service.obter_contratos_disponiveis()
        return json_response({
            'success': True,
            'contratos': contratos
        })
    except Exception as e:
        logger.error(f"Erro ao obter contratos de remessas: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@analise_bp.route('/api/campos-remessa/<contrato>')
def api_campos_remessa(contrato):
    """API para listar campos por contrato em remessas"""
    try:
        campos = remessa_service.obter_campos_por_contrato(contrato)
        return json_response({'success': True, 'campos': campos})
    except Exception as e:
        logger.error(f"Erro ao obter campos da remessa: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@analise_bp.route('/api/etapas-remessas')
def api_etapas_remessas():
    """API para listar etapas disponíveis"""
    try:
        etapas = remessa_service.obter_etapas_disponiveis()
        return json_response({'success': True, 'etapas': etapas})
    except Exception as e:
        logger.error(f"Erro ao obter etapas: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@analise_bp.route('/api/pesquisar-remessas', methods=['POST'])
def api_pesquisar_remessas():
    """API para pesquisar remessas por filtros"""
    try:
        dados = request.get_json()
        
        if not dados:
            return jsonify({'success': False, 'error': 'Dados não fornecidos'}), 400
        
        # Pesquisa por ID tem prioridade
        if dados.get('id'):
            resultado = remessa_service.pesquisar_remessa_por_id(dados['id'])
            if resultado['success']:
                return jsonify({
                    'success': True,
                    'resultados': [resultado['resultado']],
                    'total': 1,
                    'tipoConsulta': 'ID'
                })
            else:
                return jsonify(resultado), 404
        
        # Pesquisa por filtros
        if not dados.get('contratoCPP'):
            return jsonify({
                'success': False, 
                'error': 'Contrato CPP é obrigatório para pesquisa por filtros'
            }), 400
        
        resultado = remessa_service.pesquisar_remessas_por_filtros(dados)
        
        if resultado['success']:
            resultado['tipoConsulta'] = 'FILTROS'
            return jsonify(resultado)
        else:
            return jsonify(resultado), 500
            
    except Exception as e:
        logger.error(f"Erro ao pesquisar remessas: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@analise_bp.route('/api/analisar-remessas-ccos', methods=['POST'])
def api_analisar_remessas_ccos():
    """API para executar análise completa de remessas vs CCOs"""
    try:
        dados = request.get_json()
        
        if not dados:
            return json_response({'success': False, 'error': 'Dados não fornecidos'}, 400)
        
        # Extrair parâmetros
        filtros = dados.get('filtros', {})
        analise_detalhada = dados.get('analiseDetalhada', False)
        
        # Validar filtros obrigatórios
        if not filtros.get('contratoCPP'):
            return json_response({
                'success': False, 
                'error': 'Contrato CPP é obrigatório para análise'
            }, 400)
        
        logger.info(f"Iniciando análise com filtros: {filtros}")
        logger.info(f"Análise detalhada: {analise_detalhada}")
        
        # Executar análise
        resultado = remessa_service.analisar_remessas_vs_ccos(filtros, analise_detalhada)
        
        # Adicionar timestamp
        resultado['timestampAnalise'] = datetime.now().isoformat()
        resultado['success'] = True
        
        return json_response(resultado)
        
    except Exception as e:
        logger.error(f"Erro ao executar análise: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

