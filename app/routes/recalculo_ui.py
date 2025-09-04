"""
Rotas da interface de recálculo de CCOs
"""

import logging
import json
import uuid
import csv
import io
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for

from app.config import MONGO_URI
from app.services.recalculo_service import RecalculoService, TipoRecalculo, ModoRecalculo
from app.utils.cache_utils import CacheManager
from app.utils.converters import converter_decimal128_para_float

recalculo_bp = Blueprint('recalculo_ui', __name__, url_prefix='/recalculo')

logger = logging.getLogger(__name__)

def json_response(data, status=200):
    """Helper para respostas JSON padronizadas"""
    response = jsonify(data)
    response.status_code = status
    return response

@recalculo_bp.route('/')
def index_recalculo():
    """Página inicial do módulo de recálculo"""
    return render_template('recalculo/index.html', titulo="Recálculo de CCOs")

@recalculo_bp.route('/pesquisar-ccos')
def pesquisar_ccos():
    """Página de pesquisa de CCOs para recálculo"""
    return render_template('recalculo/pesquisar_ccos.html', titulo="Pesquisar CCOs para Recálculo")

@recalculo_bp.route('/api/pesquisar-ccos', methods=['POST'])
def api_pesquisar_ccos():
    """API para pesquisar CCOs disponíveis para recálculo"""
    try:
        dados = request.get_json()
        
        if not dados:
            return json_response({'success': False, 'error': 'Dados não fornecidos'}, 400)
        
        recalculo_service = RecalculoService(MONGO_URI)
        resultado = recalculo_service.pesquisar_ccos_para_recalculo(dados)
        
        return json_response(resultado)
        
    except Exception as e:
        logger.error(f"Erro ao pesquisar CCOs: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@recalculo_bp.route('/executar/<cco_id>')
def executar_recalculo(cco_id):
    """Página para executar recálculo de uma CCO específica"""
    return render_template('recalculo/executar_recalculo.html', 
                         titulo="Executar Recálculo", 
                         cco_id=cco_id)

@recalculo_bp.route('/api/executar-recalculo', methods=['POST'])
def api_executar_recalculo():
    """API para executar recálculo de CCO"""
    try:
        dados = request.get_json()
        
        # Validar dados obrigatórios
        campos_obrigatorios = ['cco_id', 'tp_original', 'tp_correcao', 'tipo_recalculo', 'modo_recalculo']
        for campo in campos_obrigatorios:
            if campo not in dados:
                return json_response({'success': False, 'error': f'Campo obrigatório: {campo}'}, 400)
        
        recalculo_service = RecalculoService(MONGO_URI)
        
        # Executar recálculo baseado no tipo
        if dados['tipo_recalculo'] == TipoRecalculo.TRACK_PARTICIPATION:
            resultado = recalculo_service.executar_recalculo_tp(
                cco_id=dados['cco_id'],
                tp_original=float(dados['tp_original']),
                tp_correcao=float(dados['tp_correcao']),
                modo=dados['modo_recalculo'],
                observacoes=dados.get('observacoes', '')
            )
        else:
            return json_response({'success': False, 'error': 'Tipo de recálculo não implementado'}, 400)
        
        if resultado['success']:
            # Salvar resultado em cache para exibição
            cache_key = str(uuid.uuid4())
            cache_manager = CacheManager(scope='user')
            cache_manager.store_data(
                key=f"recalculo_resultado_{cache_key}",
                value=resultado['resultado'],
                timeout=3600  # 1 hora
            )
            
            return json_response({
                'success': True,
                'cache_key': cache_key,
                'message': 'Recálculo executado com sucesso'
            })
        else:
            return json_response(resultado, 400)
        
    except Exception as e:
        logger.error(f"Erro ao executar recálculo: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@recalculo_bp.route('/resultado/<cache_key>')
def resultado_recalculo(cache_key):
    """Página de resultado do recálculo"""
    try:
        cache_manager = CacheManager(scope='user')
        resultado = cache_manager.get_data(f"recalculo_resultado_{cache_key}")
        
        if not resultado:
            return redirect(url_for('recalculo_ui.index_recalculo'))
        
        return render_template('recalculo/resultado_recalculo.html',
                             titulo="Resultado do Recálculo",
                             resultado=resultado,
                             cache_key=cache_key)
        
    except Exception as e:
        logger.error(f"Erro ao exibir resultado: {e}")
        return redirect(url_for('recalculo_ui.index_recalculo'))

@recalculo_bp.route('/api/salvar-temporario', methods=['POST'])
def api_salvar_temporario():
    """API para salvar resultado em coleção temporária"""
    try:
        dados = request.get_json()
        cache_key = dados.get('cache_key')
        
        if not cache_key:
            return json_response({'success': False, 'error': 'Cache key não fornecida'}, 400)
        
        # Recuperar resultado do cache
        cache_manager = CacheManager(scope='user')
        resultado = cache_manager.get_data(f"recalculo_resultado_{cache_key}")
        
        if not resultado:
            return json_response({'success': False, 'error': 'Resultado não encontrado no cache'}, 400)
        
        # Salvar em coleção temporária
        recalculo_service = RecalculoService(MONGO_URI)
        resultado_salvar = recalculo_service.salvar_resultado_temporario(resultado)
        
        return json_response(resultado_salvar)
        
    except Exception as e:
        logger.error(f"Erro ao salvar temporário: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@recalculo_bp.route('/api/aplicar-definitivo', methods=['POST'])
def api_aplicar_definitivo():
    """API para aplicar recálculo definitivamente"""
    try:
        dados = request.get_json()
        id_temporario = dados.get('id_temporario')
        
        if not id_temporario:
            return json_response({'success': False, 'error': 'ID temporário não fornecido'}, 400)
        
        recalculo_service = RecalculoService(MONGO_URI)
        resultado = recalculo_service.aplicar_recalculo_definitivo(id_temporario)
        
        return json_response(resultado)
        
    except Exception as e:
        logger.error(f"Erro ao aplicar definitivo: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@recalculo_bp.route('/api/exportar-csv/<cache_key>')
def api_exportar_csv(cache_key):
    """API para exportar resultado comparativo em CSV"""
    try:
        cache_manager = CacheManager(scope='user')
        resultado = cache_manager.get_data(f"recalculo_resultado_{cache_key}")
        
        if not resultado:
            return json_response({'success': False, 'error': 'Resultado não encontrado'}, 400)
        
        # Gerar CSV
        csv_content = gerar_csv_comparativo(resultado)
        
        return json_response({
            'success': True,
            'csv_content': csv_content,
            'filename': f"recalculo_cco_{resultado['cco_original']['_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        })
        
    except Exception as e:
        logger.error(f"Erro ao exportar CSV: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@recalculo_bp.route('/temporarios')
def listar_temporarios():
    """Página para listar recálculos temporários"""
    return render_template('recalculo/temporarios.html', titulo="Recálculos Temporários")

@recalculo_bp.route('/api/listar-temporarios')
def api_listar_temporarios():
    """API para listar recálculos temporários"""
    try:
        recalculo_service = RecalculoService(MONGO_URI)
        resultados = recalculo_service.listar_recalculos_temporarios()
        
        return json_response({
            'success': True,
            'resultados': resultados
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar temporários: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@recalculo_bp.route('/api/modal-recalculo', methods=['POST'])
def api_modal_recalculo():
    """API para preparar dados do modal de recálculo (usado na timeline)"""
    try:
        dados = request.get_json()
        cco_id = dados.get('cco_id')
        
        if not cco_id:
            return json_response({'success': False, 'error': 'CCO ID não fornecido'}, 400)
        
        # Buscar dados básicos da CCO para o modal
        recalculo_service = RecalculoService(MONGO_URI)
        resultado_pesquisa = recalculo_service.pesquisar_ccos_para_recalculo({'id': cco_id})
        
        if not resultado_pesquisa['success'] or not resultado_pesquisa['resultados']:
            return json_response({'success': False, 'error': 'CCO não encontrada'}, 400)
        
        cco_dados = resultado_pesquisa['resultados'][0]
        
        # Preparar dados para o modal
        modal_data = {
            'cco_id': cco_id,
            'contrato': cco_dados['contratoCpp'],
            'campo': cco_dados['campo'],
            'remessa': cco_dados['remessa'],
            'fase': cco_dados['faseRemessa'],
            'valor_atual': cco_dados['valorReconhecidoComOH'],
            'tipos_recalculo': [
                {'value': TipoRecalculo.TRACK_PARTICIPATION, 'label': 'Track Participation (TP)'},
                {'value': TipoRecalculo.AJUSTE_IPCA, 'label': 'Ajuste IPCA (Em desenvolvimento)'},
                {'value': TipoRecalculo.AJUSTE_IGPM, 'label': 'Ajuste IGPM (Em desenvolvimento)'},
                {'value': TipoRecalculo.AJUSTE_MANUAL, 'label': 'Ajuste Manual (Em desenvolvimento)'}
            ],
            'modos_recalculo': [
                {'value': ModoRecalculo.COMPLETO, 'label': 'Recálculo Completo'},
                {'value': ModoRecalculo.CORRECAO_MONETARIA, 'label': 'Correção Monetária'}
            ]
        }
        
        return json_response({
            'success': True,
            'modal_data': modal_data
        })
        
    except Exception as e:
        logger.error(f"Erro ao preparar modal: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

# Funções auxiliares

def gerar_csv_comparativo(resultado):
    """Gera conteúdo CSV comparativo entre CCO original e recalculada"""
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    
    # Cabeçalho do arquivo
    writer.writerow(['RELATÓRIO COMPARATIVO - RECÁLCULO DE CCO'])
    writer.writerow([])
    
    # Informações gerais
    cco_original = resultado['cco_original']
    metadata = resultado['metadata_recalculo']
    
    writer.writerow(['INFORMAÇÕES GERAIS'])
    writer.writerow(['CCO ID', cco_original['_id']])
    writer.writerow(['Contrato', cco_original.get('contratoCpp', '')])
    writer.writerow(['Campo', cco_original.get('campo', '')])
    writer.writerow(['Remessa', cco_original.get('remessa', '')])
    writer.writerow(['Fase', cco_original.get('faseRemessa', '')])
    writer.writerow(['Tipo Recálculo', metadata['tipo_recalculo']])
    writer.writerow(['Modo Recálculo', metadata['modo_recalculo']])
    writer.writerow(['Data Recálculo', metadata['data_recalculo'].strftime('%d/%m/%Y %H:%M:%S')])
    writer.writerow(['Observações', metadata.get('observacoes', '')])
    writer.writerow([])
    
    # Parâmetros do recálculo
    if metadata['tipo_recalculo'] == TipoRecalculo.TRACK_PARTICIPATION:
        writer.writerow(['PARÂMETROS TRACK PARTICIPATION'])
        writer.writerow(['TP Original', metadata['tp_original']/100])
        writer.writerow(['TP Correção', metadata['tp_correcao']/100])
        writer.writerow(['Fator de Correção', metadata['fator_correcao']])
        writer.writerow([])
    
    # Comparativo de valores
    writer.writerow(['COMPARATIVO DE VALORES'])
    writer.writerow(['Campo', 'Valor Original (R$)', 'Valor Recalculado (R$)', 'Diferença (R$)', 'Variação (%)'])
    
    comparativo = resultado['comparativo']
    for campo, dados in comparativo.items():
        if campo in ['correcao_diferencaValor']: continue # Campo desnecessário
        if campo in ['correcao_flgRecuperado']: 
            writer.writerow([
                campo.replace('correcao_', '').replace('cco_', ''),
                f"{'SIM' if dados['valor_original'] else 'NÃO'}",
                f"{'SIM' if dados['valor_recalculado'] else 'NÃO'}",
                f"-",
                f"-"
            ])
        else:        
            writer.writerow([
                campo.replace('correcao_', '').replace('cco_', ''),
                f"{dados['valor_original']:,.16f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                f"{dados['valor_recalculado']:,.15f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                f"{dados['diferenca']:,.15f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                f"{dados['percentual_variacao']:.15f}%"
            ])
    
    writer.writerow([])
    
    # Resumo
    resumo = resultado['resumo']
    writer.writerow(['RESUMO'])
    writer.writerow(['Total de Campos Alterados', resumo['total_campos_alterados']])
    writer.writerow(['Maior Variação Percentual', f"{resumo['maior_variacao_percentual']:.15f}%"])
    
    return output.getvalue()