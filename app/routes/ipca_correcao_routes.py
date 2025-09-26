from flask import Blueprint, request, jsonify, render_template
from app.config import MONGO_URI, MONGO_URI_PRD
from app.services.ipca_correcao_orquestrador import IPCACorrectionOrchestrator
from app.services.ipca_correcao_engine import IPCACorrectionEngine
from app.services.ipca_gap_analyzer import IPCAGapAnalyzer
from pymongo import MongoClient
import logging

logger = logging.getLogger(__name__)

# Criar blueprint
ipca_correcao_bp = Blueprint('ipca_correcao', __name__, url_prefix='/ipca-correcao')

# Inicializar serviços (ajustar conforme sua configuração de DB)
def get_services():
    # Substitua pela sua configuração de DB
    # TODO verificar
    client = MongoClient(MONGO_URI)
    db = client.sgppServices
    client_prd = MongoClient(MONGO_URI_PRD)
    db_prd = client_prd.sgppServices
    
    gap_analyzer = IPCAGapAnalyzer(db, db_prd)
    correction_engine = IPCACorrectionEngine(db, db_prd, gap_analyzer)
    orchestrator = IPCACorrectionOrchestrator(db, db_prd, gap_analyzer, correction_engine)
    
    return orchestrator

@ipca_correcao_bp.route('/')
def index():
    """Página principal de correção IPCA"""
    return render_template('recalculo/ipca_analise_e_recalculo.html')

@ipca_correcao_bp.route('/api/iniciar-analise', methods=['POST'])
def iniciar_analise():
    """API para iniciar análise de CCO"""
    try:
        data = request.get_json()
        cco_id = data.get('cco_id')
        user_id = data.get('user_id', 'unknown')
        
        if not cco_id:
            return jsonify({'success': False, 'error': 'CCO ID é obrigatório'}), 400
        
        orchestrator = get_services()
        resultado = orchestrator.iniciar_analise_cco(cco_id, user_id)
        
        return jsonify(resultado)
        
    except Exception as e:
        logger.error(f"Erro na API iniciar-analise: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@ipca_correcao_bp.route('/api/gerar-propostas', methods=['POST'])
def gerar_propostas():
    """API para gerar propostas de correção"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({'success': False, 'error': 'Session ID é obrigatório'}), 400
        
        orchestrator = get_services()
        resultado = orchestrator.gerar_propostas_correcao(session_id)
        
        return jsonify(resultado)
        
    except Exception as e:
        logger.error(f"Erro na API gerar-propostas: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@ipca_correcao_bp.route('/api/aprovar-correcoes', methods=['POST'])
def aprovar_correcoes():
    """API para aprovar correções selecionadas"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        corrections_approved = data.get('corrections_approved', [])
        
        if not session_id:
            return jsonify({'success': False, 'error': 'Session ID é obrigatório'}), 400
        
        orchestrator = get_services()
        resultado = orchestrator.aprovar_correcoes(session_id, corrections_approved)
        
        return jsonify(resultado)
        
    except Exception as e:
        logger.error(f"Erro na API aprovar-correcoes: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@ipca_correcao_bp.route('/api/aplicar-correcoes', methods=['POST'])
def aplicar_correcoes():
    """API para aplicar correções aprovadas"""
    try:
        data = request.get_json()
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({'success': False, 'error': 'Session ID é obrigatório'}), 400
        
        orchestrator = get_services()
        resultado = orchestrator.aplicar_correcoes(session_id)
        
        return jsonify(resultado)
        
    except Exception as e:
        logger.error(f"Erro na API aplicar-correcoes: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@ipca_correcao_bp.route('/api/status-sessao/<session_id>')
def status_sessao(session_id):
    """API para consultar status de uma sessão"""
    try:
        orchestrator = get_services()
        resultado = orchestrator.get_session_status(session_id)
        
        return jsonify(resultado)
        
    except Exception as e:
        logger.error(f"Erro na API status-sessao: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    
@ipca_correcao_bp.route('/api/avaliar-ipca-vigente', methods=['POST'])
def avaliar_ipca_vigente():
    """API para avaliar aplicação de IPCA do ano vigente"""
    try:
        data = request.get_json()
        cco_id = data.get('cco_id')
        user_id = data.get('user_id', 'unknown')
        
        orchestrator = get_services()
        resultado = orchestrator.avaliar_ipca_ano_vigente(cco_id, user_id)
        
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500