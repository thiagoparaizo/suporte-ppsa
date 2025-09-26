"""
Rotas para funcionalidade de promoção de correções IPCA/IGPM
"""

import logging
import json
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify
from pymongo import MongoClient

from app.services.ipca_promocao_service import IPCAPromocaoService
from app.services.portal_service import PortalService
from app.config import MONGO_URI, MONGO_URI_PRD

logger = logging.getLogger(__name__)

# Criar blueprint
ipca_promocao_bp = Blueprint('ipca_promocao', __name__, url_prefix='/ipca-promocao')

def get_services():
    """Inicializar serviços"""
    client = MongoClient(MONGO_URI)
    db = client.sgppServices
    client_pdb = MongoClient(MONGO_URI_PRD)
    db_pdb = client_pdb.sgppServices
    
    promocao_service = IPCAPromocaoService(db, db_pdb)
    portal_service = PortalService(MONGO_URI, MONGO_URI_PRD)
    
    return promocao_service, portal_service

def json_response(data, status=200):
    """Helper para respostas JSON padronizadas"""
    response = jsonify(data)
    response.status_code = status
    return response

@ipca_promocao_bp.route('/')
def index():
    """Página principal de promoção de correções IPCA"""
    return render_template('ipca_promocao/pesquisar_correcoes.html', 
                         titulo="Promoção de Correções IPCA/IGPM")

@ipca_promocao_bp.route('/api/pesquisar', methods=['POST'])
def api_pesquisar_correcoes():
    """API para pesquisar correções pendentes"""
    try:
        dados = request.get_json()
        
        promocao_service, _ = get_services()
        resultado = promocao_service.pesquisar_correcoes_pendentes(dados)
        
        return json_response(resultado)
        
    except Exception as e:
        logger.error(f"Erro ao pesquisar correções: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/api/contratos')
def api_contratos():
    """API para listar contratos disponíveis na coleção de correções"""
    try:
        promocao_service, _ = get_services()
        db = promocao_service.db
        
        contratos = db.conta_custo_oleo_corrigida_entity.distinct('contratoCpp')
        contratos_ordenados = sorted([c for c in contratos if c])
        
        return json_response({
            'success': True,
            'contratos': contratos_ordenados
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar contratos: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/api/campos-por-contrato/<contrato>')
def api_campos_por_contrato(contrato):
    """API para listar campos por contrato na coleção de correções"""
    try:
        promocao_service, _ = get_services()
        
        campos = promocao_service.db.conta_custo_oleo_corrigida_entity.distinct('campo', {'contratoCpp': contrato})
        campos_ordenados = sorted([c for c in campos if c])
        
        return json_response({
            'success': True,
            'campos': campos_ordenados
        })
        
    except Exception as e:
        logger.error(f"Erro ao listar campos: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/detalhar/<cco_id>')
def detalhar_correcao(cco_id):
    """Página de detalhamento de uma correção"""
    try:
        promocao_service, _ = get_services()
        resultado = promocao_service.detalhar_correcao(cco_id)
        
        if not resultado['success']:
            return render_template('erro.html', 
                                 erro="Erro ao carregar correção", 
                                 mensagem=resultado.get('error', 'Erro desconhecido'))
        
        return render_template('ipca_promocao/detalhar_correcao.html',
                             cco_corrigida=resultado['cco_corrigida'],
                             cco_original=resultado['cco_original'],
                             sessao_correcao=resultado['sessao_correcao'],
                             pode_promover=resultado['pode_promover'],
                             titulo=f"Detalhes da Correção - {cco_id}")
        
    except Exception as e:
        logger.error(f"Erro ao detalhar correção {cco_id}: {e}")
        return render_template('erro.html', 
                             erro="Erro interno", 
                             mensagem="Erro ao carregar detalhes da correção.")

@ipca_promocao_bp.route('/timeline/<cco_id>')
def timeline_correcao(cco_id):
    """Timeline da CCO corrigida"""
    try:
        promocao_service, portal_service = get_services()
        db = promocao_service.db
        
        # Buscar CCO corrigida
        cco_corrigida = db.conta_custo_oleo_corrigida_entity.find_one({"_id": cco_id})
        
        if not cco_corrigida:
            return render_template('erro.html', 
                                 erro="CCO corrigida não encontrada", 
                                 mensagem=f"CCO corrigida com ID {cco_id} não foi encontrada.")
        
        # Processar timeline usando serviços existentes
        timeline_data = portal_service.processar_timeline_cco(cco_corrigida)
        valores_atuais = portal_service.extrair_valores_atuais_cco(cco_corrigida)
        
        return render_template('ipca_promocao/timeline_correcao.html', 
                              cco=cco_corrigida,
                              timeline=timeline_data,
                              valores_atuais=valores_atuais,
                              cco_json=json.dumps(cco_corrigida, indent=2, default=str),
                              titulo=f"Timeline CCO Corrigida - {cco_id}")
        
    except Exception as e:
        logger.error(f"Erro ao carregar timeline da CCO corrigida {cco_id}: {e}")
        return render_template('erro.html', 
                             erro="Erro interno", 
                             mensagem="Erro ao carregar timeline da CCO corrigida.")

@ipca_promocao_bp.route('/memoria-calculo/<session_id>')
def memoria_calculo(session_id):
    """Página com a memória de cálculo de uma sessão"""
    try:
        promocao_service, _ = get_services()
        resultado = promocao_service.obter_memoria_calculo(session_id)
        
        if not resultado['success']:
            return render_template('erro.html', 
                                 erro="Memória de cálculo não encontrada", 
                                 mensagem=resultado.get('error', 'Sessão não encontrada'))
        
        memoria_convertida = converter_tipos_mongodb(resultado['memoria_calculo'])
        
        return render_template('ipca_promocao/memoria_calculo.html',
                             memoria_calculo=memoria_convertida,
                             titulo=f"Memória de Cálculo - {session_id}")
        
    except Exception as e:
        logger.error(f"Erro ao carregar memória de cálculo {session_id}: {e}")
        return render_template('erro.html', 
                             erro="Erro interno", 
                             mensagem="Erro ao carregar memória de cálculo.")

@ipca_promocao_bp.route('/api/promover', methods=['POST'])
def api_promover_correcao():
    """API para promover correção para produção"""
    try:
        dados = request.get_json()
        
        cco_id = dados.get('cco_id')
        user_id = 'thiago.paraizo@scalait.com' # TODO ajustar --> dados.get('user_id', 'thiago.paraizo@scalait.com')
        observacoes = dados.get('observacoes', '')
        
        if not cco_id:
            return json_response({'success': False, 'error': 'CCO ID é obrigatório'}, 400)
        
        promocao_service, _ = get_services()
        resultado = promocao_service.promover_correcao(cco_id, user_id, observacoes)
        
        return json_response(resultado)
        
    except Exception as e:
        logger.error(f"Erro ao promover correção: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/api/validar-promocao/<cco_id>')
def api_validar_promocao(cco_id):
    """API para validar se uma correção pode ser promovida"""
    try:
        promocao_service, _ = get_services()
        resultado = promocao_service.detalhar_correcao(cco_id)
        
        if not resultado['success']:
            return json_response({'success': False, 'error': resultado.get('error', 'Erro na validação')}, 400)
        
        return json_response({
            'success': True,
            'validacao': resultado['pode_promover']
        })
        
    except Exception as e:
        logger.error(f"Erro ao validar promoção {cco_id}: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/api/estatisticas')
def api_estatisticas():
    """API para obter estatísticas das promoções"""
    try:
        promocao_service, _ = get_services()
        resultado = promocao_service.obter_estatisticas_promocao()
        
        return json_response(resultado)
        
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/api/timeline-comparativo/<cco_id>')
def api_timeline_comparativo(cco_id):
    """API para obter dados comparativos entre CCO original e corrigida"""
    try:
        promocao_service, portal_service = get_services()
        
        # Obter detalhes da correção
        resultado = promocao_service.detalhar_correcao(cco_id)
        if not resultado['success']:
            return json_response({'success': False, 'error': resultado.get('error', 'CCO não encontrada')}, 400)
        
        cco_corrigida = resultado['cco_corrigida']
        cco_original = resultado['cco_original']
        
        dados_comparativo = {
            'cco_corrigida': {
                'timeline': portal_service.processar_timeline_cco(cco_corrigida) if cco_corrigida else None,
                'valores_atuais': portal_service.extrair_valores_atuais_cco(cco_corrigida) if cco_corrigida else None
            },
            'cco_original': {
                'timeline': portal_service.processar_timeline_cco(cco_original) if cco_original else None,
                'valores_atuais': portal_service.extrair_valores_atuais_cco(cco_original) if cco_original else None
            }
        }
        
        return json_response({
            'success': True,
            'comparativo': dados_comparativo
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter timeline comparativo {cco_id}: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)

@ipca_promocao_bp.route('/api/historico-promocoes')
def api_historico_promocoes():
    """API para obter histórico de promoções realizadas"""
    try:
        promocao_service, _ = get_services()
        db = promocao_service.db
        
        # Buscar correções já promovidas
        pipeline = [
            {'$match': {'status_promocao': 'PROMOVIDA'}},
            {'$sort': {'data_promocao': -1}},
            {'$limit': 50},
            {'$project': {
                '_id': 1,
                'contratoCpp': 1,
                'campo': 1,
                'remessa': 1,
                'session_id': 1,
                'data_promocao': 1,
                'usuario_promocao': 1,
                'observacoes_promocao': 1,
                'versao_promovida': 1
            }}
        ]
        
        historico = list(db.conta_custo_oleo_corrigida_entity.aggregate(pipeline))
        
        return json_response({
            'success': True,
            'historico': historico,
            'total': len(historico)
        })
        
    except Exception as e:
        logger.error(f"Erro ao obter histórico de promoções: {e}")
        return json_response({'success': False, 'error': str(e)}, 500)
    
from bson import Decimal128
import json

def converter_tipos_mongodb(obj):
    """Converte tipos do MongoDB para tipos serializáveis"""
    if isinstance(obj, dict):
        return {k: converter_tipos_mongodb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [converter_tipos_mongodb(item) for item in obj]
    elif isinstance(obj, Decimal128):
        return float(obj.to_decimal())
    elif hasattr(obj, 'isoformat'):  # datetime
        return obj.isoformat() if obj else None
    else:
        return obj