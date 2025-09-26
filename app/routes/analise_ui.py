"""
Rotas para análise de remessas x CCOs
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for
import json
import logging
import uuid
from datetime import datetime

from app.config import MONGO_URI, MONGO_URI_PRD
from app.services.remessa_service import RemessaAnaliseService
from app.utils.converters import formatar_data_brasileira
from app.utils.json_encoder import json_response
from app.utils.cache_utils import CacheManager
from app.services.analise_helpers import (
    calcular_estatisticas_gastos,
    obter_top_classificacoes,
    obter_top_responsaveis,
    obter_distribuicao_status,
    obter_moedas_utilizadas,
    processar_dados_dashboard,
    gerar_csv_analise,
    gerar_recomendacao_analise,
)

analise_bp = Blueprint('analise_ui', __name__)

logger = logging.getLogger(__name__)

# Instância do serviço
remessa_service = RemessaAnaliseService(MONGO_URI, MONGO_URI_PRD)

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
    

# Funções auxiliares foram extraídas para app/services/analise_helpers.py

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

