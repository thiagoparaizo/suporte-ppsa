"""
IPCACorrectionOrchestrator - Coordenador Principal do Sistema de Correção IPCA/IGPM
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, asdict
import json

logger = logging.getLogger(__name__)

class CorrectionStatus(Enum):
    """Status da sessão de correção"""
    ANALYZING = "ANALYZING"
    PREVIEW = "PREVIEW" 
    APPROVED = "APPROVED"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    ERROR = "ERROR"

class CorrectionType(Enum):
    """Tipos de correção possíveis"""
    IPCA_ADDITION = "IPCA_ADDITION"          # Adicionar IPCA faltante
    IPCA_UPDATE = "IPCA_UPDATE"              # Atualizar IPCA existente
    COMPENSATION = "COMPENSATION"             # Compensação por recuperação
    REACTIVATION = "REACTIVATION"            # Reativar CCO recuperada
    DUPLICATA_REMOVAL = "DUPLICATA_REMOVAL"  # Remoção de duplicatas
    DUPLICATA_ADJUSTMENT = "DUPLICATA_ADJUSTMENT"  # Ajuste de duplicatas
    CORRECTION_DATE_CHANGE = "CORRECTION_DATE_CHANGE"  # Alteração de data de correção

@dataclass
class CorrectionProposal:
    """Proposta de correção individual"""
    correction_id: str
    type: CorrectionType
    scenario: str
    target_date: datetime
    target_period: str  # "MM/YYYY"
    current_value: float
    proposed_value: float
    impact: float
    taxa_aplicada: float
    taxa_referencia: str  # "MM/YYYY"
    description: str
    dependencies: List[str]  # IDs de outras correções dependentes
    business_rules_applied: List[str]
    indice_remover: Optional[int] = None
    taxas_recalculadas: Optional[List[Dict[str, Any]]] = None
    
    def to_dict(self):
        result = asdict(self)
        result['target_date'] = self.target_date.isoformat()
        result['type'] = self.type.value
        return result

@dataclass
class CorrectionSession:
    """Sessão de correção de uma CCO"""
    session_id: str
    cco_id: str
    user_id: str
    status: CorrectionStatus
    gaps_identified: List[Dict[str, Any]]
    corrections_fora_periodo: List[Dict[str, Any]]
    ccos_com_duplicatas: List[Dict[str, Any]]
    corrections_proposed: List[CorrectionProposal]
    corrections_approved: List[str]  # IDs das correções aprovadas
    financial_impact: Dict[str, float]
    scenario_detected: str
    created_at: datetime
    updated_at: datetime
    applied_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    def to_dict(self):
        result = asdict(self)
        result['status'] = self.status.value
        result['created_at'] = self.created_at.isoformat()
        result['updated_at'] = self.updated_at.isoformat()
        result['applied_at'] = self.applied_at.isoformat() if self.applied_at else None
        result['corrections_proposed'] = [cp.to_dict() for cp in self.corrections_proposed]
        return result

class IPCACorrectionOrchestrator:
    """
    Coordenador principal do sistema de correção IPCA/IGPM
    """
    
    def __init__(self, db_connection, db_prd_connection, gap_analyzer, correction_engine=None):
        """
        Inicializa o orchestrator
        
        Args:
            db_connection: Conexão com MongoDB
            gap_analyzer: Instância do IPCAGapAnalyzer
            correction_engine: Instância do IPCACorrectionEngine (será implementado)
        """
        self.db = db_connection
        self.db_prd = db_prd_connection
        self.gap_analyzer = gap_analyzer
        self.correction_engine = correction_engine
        
        # Usar MongoDB para persistir sessões
        self.sessions_collection = self.db.ipca_correction_sessions
        
        logger.info("IPCACorrectionOrchestrator inicializado")
    
    def _save_session(self, session: CorrectionSession):
        """Salva sessão no MongoDB"""
        try:
            session_dict = session.to_dict()
            self.sessions_collection.replace_one(
                {'session_id': session.session_id},
                session_dict,
                upsert=True
            )
            logger.info(f"Sessão {session.session_id} salva no MongoDB")
        except Exception as e:
            logger.error(f"Erro ao salvar sessão: {e}")

    def _load_session(self, session_id: str) -> Optional[CorrectionSession]:
        """Carrega sessão do MongoDB"""
        try:
            session_doc = self.sessions_collection.find_one({'session_id': session_id})
            if not session_doc:
                return None
            
            # Converter de volta para CorrectionSession
            session = self._dict_to_session(session_doc)
            logger.info(f"Sessão {session_id} carregada do MongoDB")
            return session
            
        except Exception as e:
            logger.error(f"Erro ao carregar sessão {session_id}: {e}")
            return None

    def _dict_to_session(self, session_dict: Dict) -> CorrectionSession:
        """Converte dicionário do MongoDB para CorrectionSession"""
        # Remover campos do MongoDB que não fazem parte da classe
        session_data = session_dict.copy()
        session_data.pop('_id', None)  # Remover _id do MongoDB
        
        # Converter strings de volta para datetime
        session_data['created_at'] = datetime.fromisoformat(session_data['created_at'])
        session_data['updated_at'] = datetime.fromisoformat(session_data['updated_at'])
        if session_data.get('applied_at'):
            session_data['applied_at'] = datetime.fromisoformat(session_data['applied_at'])
        
        # Converter status de volta para enum
        session_data['status'] = CorrectionStatus(session_data['status'])
        
        # Converter propostas de volta para objetos
        proposals = []
        for p_dict in session_data.get('corrections_proposed', []):
            # Criar cópia para não modificar original
            proposal_data = p_dict.copy()
            proposal_data['target_date'] = datetime.fromisoformat(proposal_data['target_date'])
            proposal_data['type'] = CorrectionType(proposal_data['type'])
            proposals.append(CorrectionProposal(**proposal_data))
        session_data['corrections_proposed'] = proposals
        
        return CorrectionSession(**session_data)
    
    def iniciar_analise_cco(self, cco_id: str, user_id: str) -> Dict[str, Any]:
        """
        Inicia análise de uma CCO específica para identificar problemas e gerar correções
        
        Args:
            cco_id: ID da CCO a ser analisada
            user_id: ID do usuário que solicitou a análise
            
        Returns:
            Resultado da análise inicial
        """
        try:
            logger.info(f"Iniciando análise da CCO {cco_id} pelo usuário {user_id}")
            
            # Criar nova sessão
            session_id = str(uuid.uuid4())
            
            # Executar análise de gaps
            filtros = {'_id': cco_id}
            resultado_gaps = self.gap_analyzer.analisar_gaps_sistema(filtros)
            
            if 'error' in resultado_gaps:
                return {
                    'success': False,
                    'error': f"Erro na análise de gaps: {resultado_gaps['error']}"
                }
            
            # Extrair informações da CCO analisada
            gaps_identificados = resultado_gaps.get('ccos_com_gaps', [])
            correcoes_fora = resultado_gaps.get('ccos_com_correcoes_fora_periodo', [])
            duplicatas = resultado_gaps.get('ccos_com_duplicatas', {})
            
            # Determinar cenário
            cenario = self._determinar_cenario(gaps_identificados, correcoes_fora, duplicatas, cco_id)
            
            # Criar sessão de correção
            session = CorrectionSession(
                session_id=session_id,
                cco_id=cco_id,
                user_id=user_id,
                status=CorrectionStatus.ANALYZING,
                gaps_identified=gaps_identificados,
                corrections_fora_periodo=correcoes_fora,
                ccos_com_duplicatas=duplicatas,
                corrections_proposed=[],
                corrections_approved=[],
                financial_impact={},
                scenario_detected=cenario,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc)
            )
            
            # Armazenar sessão
            self._save_session(session)
            
            # Retornar resultado inicial
            return {
                'success': True,
                'session_id': session_id,
                'cco_id': cco_id,
                'scenario_detected': cenario,
                'gaps_count': len(gaps_identificados),
                'corrections_fora_count': len(correcoes_fora),
                'duplicates_count': len(duplicatas),
                'analysis_summary': self._gerar_resumo_analise(session),
                'next_step': 'gerar_propostas_correcao'
            }
            
        except Exception as e:
            logger.error(f"Erro ao iniciar análise da CCO {cco_id}: {e}")
            return {
                'success': False,
                'error': f"Erro interno: {str(e)}"
            }
    
    def gerar_propostas_correcao(self, session_id: str) -> Dict[str, Any]:
        """
        Gera propostas de correção baseadas na análise realizada
        
        Args:
            session_id: ID da sessão de correção
            
        Returns:
            Propostas de correção geradas
        """
        try:
            session = self._load_session(session_id)
            if not session:
                return {'success': False, 'error': 'Sessão não encontrada'}
            
            logger.info(f"Gerando propostas para sessão {session_id}, cenário {session.scenario_detected}")
            
            # Atualizar status
            session.status = CorrectionStatus.PREVIEW
            session.updated_at = datetime.now(timezone.utc)
            self._save_session(session)
            
            # Gerar propostas baseadas no cenário
            if session.scenario_detected == "CENARIO_0":
                propostas = self._gerar_propostas_cenario_0(session)
            elif session.scenario_detected == "CENARIO_1":
                propostas = self._gerar_propostas_cenario_1(session)
            elif session.scenario_detected == "CENARIO_2":
                propostas = self._gerar_propostas_cenario_2(session)
            elif session.scenario_detected == "CENARIO_DUPLICATAS":
                propostas = self._gerar_propostas_cenario_duplicatas(session)
            else:
                propostas = []
                logger.warning(f"Cenário não implementado: {session.scenario_detected}")
            
            # Adicionar propostas à sessão
            session.corrections_proposed = propostas
            
            # Calcular impacto financeiro total
            session.financial_impact = self._calcular_impacto_financeiro(propostas)
            
            self._save_session(session)
            
            return {
                'success': True,
                'session_id': session_id,
                'scenario': session.scenario_detected,
                'proposals_count': len(propostas),
                'proposals': [p.to_dict() for p in propostas],
                'financial_impact': session.financial_impact,
                'preview_data': self._gerar_preview_data(session),
                'next_step': 'aprovar_correcoes'
            }
            
        except Exception as e:
            logger.error(f"Erro ao gerar propostas para sessão {session_id}: {e}")
            session.status = CorrectionStatus.ERROR
            session.error_message = str(e)
            return {'success': False, 'error': f"Erro interno: {str(e)}"}
    
    def aprovar_correcoes(self, session_id: str, corrections_approved: List[str]) -> Dict[str, Any]:
        """
        Aprova correções selecionadas pelo usuário
        
        Args:
            session_id: ID da sessão
            corrections_approved: Lista de IDs das correções aprovadas
            
        Returns:
            Resultado da aprovação
        """
        try:
            session = self._load_session(session_id)
            if not session:
                return {'success': False, 'error': 'Sessão não encontrada'}
            
            logger.info(f"Aprovando {len(corrections_approved)} correções para sessão {session_id}")
            
            # Validar IDs das correções
            propostas_ids = [p.correction_id for p in session.corrections_proposed]
            corrections_validas = [c for c in corrections_approved if c in propostas_ids]
            
            if len(corrections_validas) != len(corrections_approved):
                return {'success': False, 'error': 'Algumas correções selecionadas são inválidas'}
            
            # Atualizar sessão
            session.corrections_approved = corrections_validas
            session.status = CorrectionStatus.APPROVED
            session.updated_at = datetime.now(timezone.utc)
            self._save_session(session)
            
            # Gerar preview final das correções aprovadas
            preview_final = self._gerar_preview_final(session)
            
            return {
                'success': True,
                'session_id': session_id,
                'corrections_approved_count': len(corrections_validas),
                'final_preview': preview_final,
                'ready_to_apply': True,
                'next_step': 'aplicar_correcoes'
            }
            
        except Exception as e:
            logger.error(f"Erro ao aprovar correções para sessão {session_id}: {e}")
            return {'success': False, 'error': f"Erro interno: {str(e)}"}
    
    def aplicar_correcoes(self, session_id: str) -> Dict[str, Any]:
        """
        Aplica as correções aprovadas na CCO
        
        Args:
            session_id: ID da sessão
            
        Returns:
            Resultado da aplicação
        """
        try:
            session = self._load_session(session_id)
            if not session or session.status != CorrectionStatus.APPROVED:
                return {'success': False, 'error': 'Sessão inválida ou não aprovada'}
            
            logger.info(f"Aplicando correções para sessão {session_id}")
            
            # Filtrar correções aprovadas
            correcoes_para_aplicar = [
                p for p in session.corrections_proposed 
                if p.correction_id in session.corrections_approved
            ]
            
            # Aplicar baseado no cenário
            if session.scenario_detected == "CENARIO_0":
                resultado = self.correction_engine.aplicar_correcoes_cenario_0(
                    session.session_id,
                    session.cco_id, 
                    [asdict(c) for c in correcoes_para_aplicar]
                )
            elif session.scenario_detected == "CENARIO_1":
                resultado = self.correction_engine.aplicar_correcoes_cenario_1(
                    session.session_id,
                    session.cco_id, 
                    [asdict(c) for c in correcoes_para_aplicar]
                )
            elif session.scenario_detected == "CENARIO_2":
                resultado = self.correction_engine.aplicar_correcoes_cenario_2(
                    session.session_id,
                    session.cco_id, 
                    [asdict(c) for c in correcoes_para_aplicar]
                )
            elif session.scenario_detected == "CENARIO_DUPLICATAS":
                resultado = self.correction_engine.aplicar_correcoes_cenario_duplicatas(
                    session.session_id,
                    session.cco_id, 
                    [asdict(c) for c in correcoes_para_aplicar]
                )
            elif session.scenario_detected == "CENARIO_IPCA_VIGENTE":
                resultado = self.correction_engine.aplicar_correcoes_cenario_ipca_vigente(
                    session.session_id,
                    session.cco_id, 
                    [asdict(c) for c in correcoes_para_aplicar]
                )
                
            else:
                return {'success': False, 'error': f'Cenário {session.scenario_detected} não implementado'}
            
            if not resultado['success']:
                session.status = CorrectionStatus.ERROR
                session.error_message = resultado.get('error', 'Erro desconhecido')
                return resultado
            
            # Atualizar sessão
            session.status = CorrectionStatus.APPLIED
            session.applied_at = datetime.now(timezone.utc)
            session.updated_at = datetime.now(timezone.utc)
            self._save_session(session)
            
            return {
                'success': True,
                'session_id': session_id,
                'applied_at': session.applied_at.isoformat(),
                'corrections_applied_count': len(session.corrections_approved),
                'final_status': 'COMPLETED'
            }
            
        except Exception as e:
            logger.error(f"Erro ao aplicar correções para sessão {session_id}: {e}")
            session.status = CorrectionStatus.ERROR
            session.error_message = str(e)
            return {'success': False, 'error': f"Erro interno: {str(e)}"}
    
    def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """
        Obtém status atual de uma sessão
        """
        session = self._load_session(session_id)
        if not session:
            return {'success': False, 'error': 'Sessão não encontrada'}
        
        return {
            'success': True,
            'session': session.to_dict()
        }
    
    def _determinar_cenario(self, gaps: List[Dict], correcoes_fora: List[Dict], duplicatas: List[Dict], cco_id: str) -> str:
        """
        Determina qual cenário se aplica à CCO baseado nos gaps e correções
        """
        tem_gaps = len(gaps) > 0
        tem_correcoes_fora = len(correcoes_fora) > 0
        tem_duplicatas = len(duplicatas) > 0
        
        # Buscar CCO para análise detalhada
        cco = self.db_prd.conta_custo_oleo_entity.find_one({'_id': cco_id})
        if not cco:
            return "CENARIO_COMPLEXO"
        
        # PRIORIDADE 1: Verificar duplicatas ANTES de outros cenários
        if tem_duplicatas:
            return "CENARIO_DUPLICATAS"  # Duplicatas têm prioridade sobre outros problemas
        
        correcoes_monetarias = cco.get('correcoesMonetarias', [])
        
        # Verificar tipos de correções existentes
        tem_recuperacao = any(c.get('tipo') == 'RECUPERACAO' for c in correcoes_monetarias)
        tem_retificacao = any(c.get('tipo') == 'RETIFICACAO' for c in correcoes_monetarias)
        tem_ipca_igpm = any(c.get('tipo') in ['IPCA', 'IGPM'] for c in correcoes_monetarias)
        
        
        # Analisar correções IPCA/IGPM posteriores aos gaps
        tem_correcoes_posteriores_aos_gaps = self._tem_correcoes_posteriores_aos_gaps(
            gaps, correcoes_monetarias, cco
        )
        
        # Lógica de detecção refinada
        if tem_gaps and not tem_correcoes_fora and not tem_recuperacao and not tem_correcoes_posteriores_aos_gaps:
            return "CENARIO_0"  # Gap simples - só falta correção
            
        elif tem_gaps and (tem_correcoes_fora or tem_correcoes_posteriores_aos_gaps) and not tem_recuperacao:
            return "CENARIO_1"  # Gap com correção posterior que precisa ser recalculada
            
        elif (tem_gaps or tem_correcoes_fora) and tem_recuperacao:
            return "CENARIO_2"  # Gap com recuperação
            
        elif tem_correcoes_fora and not tem_gaps and not tem_recuperacao:
            return "CENARIO_CORRECAO_FORA_APENAS"  # Apenas correção fora do prazo
            
        else:
            return "CENARIO_COMPLEXO"  # Outros casos
    
    def _tem_correcoes_posteriores_aos_gaps(self, gaps: List[Dict], 
                                       correcoes_monetarias: List[Dict], 
                                       cco: Dict[str, Any]) -> bool:
        """
        Verifica se existem correções IPCA/IGPM posteriores aos gaps identificados
        """
        try:
            if not gaps or not correcoes_monetarias:
                return False
            
            # Extrair datas dos gaps
            datas_gaps = []
            for cco_gap in gaps:
                if cco_gap['_id'] == cco['_id']:
                    for gap in cco_gap['gaps']:
                        data_gap = datetime(gap['ano'], gap['mes'], 15, tzinfo=timezone.utc)
                        datas_gaps.append(data_gap)
            
            if not datas_gaps:
                return False
            
            # Data do gap mais antigo
            gap_mais_antigo = min(datas_gaps)
            
            # Verificar se há correções IPCA/IGPM posteriores ao gap mais antigo
            for correcao in correcoes_monetarias:
                if correcao.get('tipo') in ['IPCA', 'IGPM']:
                    data_correcao = self.gap_analyzer._extrair_data_correcao(correcao)
                    if data_correcao and data_correcao > gap_mais_antigo:
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Erro ao verificar correções posteriores aos gaps: {e}")
            return False
    
    def _gerar_resumo_analise(self, session: CorrectionSession) -> Dict[str, Any]:
        """
        Gera resumo da análise inicial
        """
        return {
            'cco_id': session.cco_id,
            'scenario': session.scenario_detected,
            'gaps_found': len(session.gaps_identified),
            'corrections_fora_found': len(session.corrections_fora_periodo),
            'analysis_timestamp': session.created_at.isoformat()
        }
    
    def _gerar_propostas_cenario_0(self, session: CorrectionSession) -> List[CorrectionProposal]:
        """
        Gera propostas para Cenário 0 - Gap simples
        """
        propostas = []
        
        
        if not session.gaps_identified:
            return propostas
        
        for cco_gap in session.gaps_identified:
            correcoes_engine = self.correction_engine.calcular_correcao_cenario_0(
                cco_gap['_id'], [cco_gap]
            )
            
            for correcao in correcoes_engine:
                proposta = CorrectionProposal(
                    correction_id=str(uuid.uuid4()),
                    type=CorrectionType.IPCA_ADDITION,
                    scenario="CENARIO_0",
                    target_date=correcao['data_correcao'],
                    target_period=f"{correcao['mes_gap']:02d}/{correcao['ano_gap']}",
                    current_value=correcao['valor_original'],
                    proposed_value=correcao['valor_corrigido'],
                    impact=correcao['impacto'],
                    taxa_aplicada=correcao['taxa_aplicada'],
                    taxa_referencia=correcao['periodo_taxa'],
                    description=correcao['descricao'],
                    dependencies=[],
                    business_rules_applied=['CENARIO_0_GAP_SIMPLES']
                )
                propostas.append(proposta)
        
        return propostas
    
    def _gerar_propostas_cenario_1(self, session: CorrectionSession) -> List[CorrectionProposal]:
        """
        Gera propostas para Cenário 1 - Gap com correção posterior
        """
        propostas = []
        
        if not session.gaps_identified:
            return propostas
        
        for cco_gap in session.gaps_identified:
            correcoes_engine = self.correction_engine.calcular_correcao_cenario_1(
                cco_gap['_id'], [cco_gap], session.corrections_fora_periodo
            )
            
            for correcao in correcoes_engine:
                # Determinar tipo baseado na correção
                if correcao['tipo'] == 'IPCA_ADDITION':
                    correction_type = CorrectionType.IPCA_ADDITION
                    dependencies = []
                elif correcao['tipo'] == 'IPCA_UPDATE':
                    correction_type = CorrectionType.IPCA_UPDATE
                    # Criar dependência da correção de gap
                    dependencies = [f"gap_{correcao.get('ano_gap', '')}{correcao.get('mes_gap', '')}"]
                else:
                    continue
                
                proposta = CorrectionProposal(
                    correction_id=str(uuid.uuid4()),
                    type=correction_type,
                    scenario="CENARIO_1",
                    target_date=correcao['data_correcao'],
                    target_period=correcao.get('periodo_alvo', 'N/A'),
                    current_value=correcao['valor_original'],
                    proposed_value=correcao['valor_corrigido'],
                    impact=correcao['impacto'],
                    taxa_aplicada=correcao['taxa_aplicada'],
                    taxa_referencia=correcao.get('periodo_taxa', 'N/A'),
                    description=correcao['descricao'],
                    dependencies=dependencies,
                    business_rules_applied=['CENARIO_1_GAP_COM_POSTERIOR']
                )
                propostas.append(proposta)
        
        return propostas
    
    def _gerar_propostas_cenario_2(self, session: CorrectionSession) -> List[CorrectionProposal]:
        """
        Gera propostas para Cenário 2 - Gap com recuperação posterior
        """
        propostas = []
        
        if not session.gaps_identified:
            return propostas
        
        for cco_gap in session.gaps_identified:
            correcoes_engine = self.correction_engine.calcular_correcao_cenario_2(
                cco_gap['_id'], [cco_gap], session.corrections_fora_periodo
            )
            
            for correcao in correcoes_engine:
                # Determinar tipo baseado na correção
                if correcao['tipo'] == 'IPCA_ADDITION':
                    correction_type = CorrectionType.IPCA_ADDITION
                elif correcao['tipo'] == 'COMPENSATION':
                    correction_type = CorrectionType.COMPENSATION
                elif correcao['tipo'] == 'REACTIVATION':
                    correction_type = CorrectionType.REACTIVATION
                    valor_saldo_final = sum(c.get('impacto', 0) for c in correcoes_engine if c['tipo'] in ['IPCA_ADDITION', 'IPCA_UPDATE']) # não considerando o 'COMPENSATION'.
                    current_value = 0  # CCO estava zerada
                    proposed_value = valor_saldo_final  # Saldo final após correções
                    impact = 0  # Não impacta valor monetário, apenas flag
                else:
                    current_value = correcao.get('valor_original', 0)
                    proposed_value = correcao.get('valor_corrigido', 0)
                    impact = correcao.get('impacto', 0)
                    continue
                
                if correcao['tipo'] in ['IPCA_ADDITION', 'COMPENSATION']:
                    current_value = correcao.get('valor_original', 0)
                    proposed_value = correcao.get('valor_corrigido', 0)
                    impact = correcao.get('impacto', 0)
                    
                
                proposta = CorrectionProposal(
                    correction_id=str(uuid.uuid4()),
                    type=correction_type,
                    scenario="CENARIO_2",
                    target_date=correcao.get('data_correcao', datetime.now(timezone.utc)),
                    target_period=correcao.get('periodo_alvo', 'N/A'),
                    current_value=current_value,
                    proposed_value=proposed_value,
                    impact=impact,
                    taxa_aplicada=correcao.get('taxa_aplicada', 1.0),
                    taxa_referencia=correcao.get('periodo_taxa', 'N/A'),
                    description=correcao['descricao'],
                    dependencies=correcao.get('dependencies', []),
                    business_rules_applied=['CENARIO_2_GAP_COM_RECUPERACAO']
                )
                propostas.append(proposta)
        
        return propostas
    
    def _gerar_propostas_cenario_duplicatas(self, session: CorrectionSession) -> List[CorrectionProposal]:
        """
        Gera propostas para Cenário Duplicatas - Remoção de correções IPCA/IGPM duplicadas
        """
        propostas = []
        
        # Buscar CCO para análise de duplicatas
        cco = self.db_prd.conta_custo_oleo_entity.find_one({'_id': session.cco_id})
        if not cco:
            return propostas
        
        correcoes_originais = cco['correcoesMonetarias']
        valor_compensacao_total = 0
        
        # Identificar duplicatas
        duplicatas = self.gap_analyzer._identificar_correcoes_duplicadas(cco)
        
        # Iterar duplicatas
        for duplicata in duplicatas:
            
            # Valor da diferença da duplicata
            diferenca_duplicata = self.gap_analyzer._converter_decimal128_para_float(
                duplicata.get('valor_duplicado', 0)
            )
            
            # Encontrar correções IPCA/IGPM posteriores a esta duplicata
            data_duplicata = self.gap_analyzer._extrair_data_correcao(duplicata['correcao_duplicada'])
            correcoes_posteriores = []
            
            
            for i, correcao in enumerate(correcoes_originais):
                if i > duplicata['indice'] and correcao.get('tipo') in ['IPCA', 'IGPM']:
                    data_correcao = self.gap_analyzer._extrair_data_correcao(correcao)
                    if data_correcao and data_correcao > data_duplicata:
                        periodo = f"{data_correcao.month:02d}/{data_correcao.year}",
                        diferenca = self.gap_analyzer._converter_decimal128_para_float(correcao.get('diferencaValor', 0))
                        taxa = self.gap_analyzer._converter_decimal128_para_float(
                            correcao.get('taxaCorrecao', 1.0)
                        )
                        correcoes_posteriores.append({
                            'periodo': periodo,
                            'diferenca': diferenca,
                            'taxa': taxa
                        })
                        
            # Calcular efeito cascata
            valor_cascata = diferenca_duplicata
            for cp in correcoes_posteriores:
                valor_cascata *= cp['taxa']
            
            valor_compensacao_total += valor_cascata
            
            # Criar proposta de remoção da duplicata
            proposta_remocao = CorrectionProposal(
                correction_id=str(uuid.uuid4()),
                type=CorrectionType.DUPLICATA_REMOVAL,  # Novo tipo
                scenario="CENARIO_DUPLICATAS",
                target_date=datetime.now(timezone.utc),
                target_period=duplicata['periodo'],
                current_value=duplicata['valor_duplicado'],  # Valor que será removido
                proposed_value=0,  # Valor final (zero após remoção)
                impact=-duplicata['valor_duplicado'],  # Impacto negativo
                taxa_aplicada=1.0,
                taxa_referencia='N/A',
                description=f"Remoção de correção IPCA duplicada - período {duplicata['periodo']} na data {data_duplicata}",
                dependencies=[],
                business_rules_applied=['CENARIO_DUPLICATAS_REMOCAO']
            )
            
            if correcoes_posteriores and len(correcoes_posteriores) > 0:
                proposta_remocao.taxas_recalculadas = correcoes_posteriores
            
            proposta_remocao.indice_remover = duplicata['indice']
            propostas.append(proposta_remocao)
        
        # Verificar necessidade de ajuste final
        valor_total_removido = sum(dup['valor_duplicado'] for dup in duplicatas)
        print(f"_gerar_propostas_cenario_duplicatas: Valor total removido: {valor_total_removido}")
        print(f"_gerar_propostas_cenario_duplicatas: Valor compensação total: {valor_compensacao_total}")
        
        if valor_total_removido > 0 or valor_compensacao_total > 0:
            # Proposta de ajuste compensatório
            proposta_ajuste = CorrectionProposal(
                correction_id=str(uuid.uuid4()),
                type=CorrectionType.DUPLICATA_ADJUSTMENT,  # Novo tipo
                scenario="CENARIO_DUPLICATAS",
                target_date=datetime.now(timezone.utc),
                target_period='AJUSTE',
                current_value=0,
                proposed_value= -valor_compensacao_total if valor_compensacao_total > valor_total_removido else -valor_total_removido,  # Ajuste negativo
                impact=-valor_compensacao_total,
                taxa_aplicada=1.0,
                taxa_referencia='N/A',
                description=f"Ajuste compensatório por remoção de {len(duplicatas)} duplicata(s)",
                dependencies=[p.correction_id for p in propostas],  # Depende das remoções
                business_rules_applied=['CENARIO_DUPLICATAS_AJUSTE']
            )
            
            if correcoes_posteriores and len(correcoes_posteriores) > 0:
                proposta_ajuste.taxas_recalculadas = correcoes_posteriores
                proposta_ajuste.description += f". Efeito cascata aplicado sobre {len(correcoes_posteriores)} correção(ões) posterior(es)"
                # iterar sobre as correções posteriores e adicionar ao description
                for cp in correcoes_posteriores:
                    proposta_ajuste.description += f". {cp['periodo']} (taxa {cp['taxa']:.4f}): R$ {cp['diferenca']:.2f}"
            
            propostas.append(proposta_ajuste)
        
        # Verificar necessidade de reativação
        valor_final_estimado = self._estimar_valor_final_apos_duplicatas(cco, valor_compensacao_total)
        if cco.get('flgRecuperado', False) and valor_final_estimado != 0:
            proposta_reativacao = CorrectionProposal(
                correction_id=str(uuid.uuid4()),
                type=CorrectionType.REACTIVATION,
                scenario="CENARIO_DUPLICATAS",
                target_date=datetime.now(timezone.utc),
                target_period='REATIVACAO',
                current_value=0,
                proposed_value=valor_final_estimado,
                impact=valor_final_estimado,  # Não impacta valor monetário
                taxa_aplicada=1.0,
                taxa_referencia='N/A',
                description=f"Reativação da CCO após remoção de duplicatas",
                dependencies=[p.correction_id for p in propostas],
                business_rules_applied=['CENARIO_DUPLICATAS_REATIVACAO']
            )
            propostas.append(proposta_reativacao)
        
        return propostas
    
    
    def _estimar_valor_final_apos_duplicatas(self, cco: Dict[str, Any], valor_total_removido: float) -> float:
        """
        Estima valor final da CCO após remoção das duplicatas
        """
        valor_atual = self.gap_analyzer._obter_valor_atual_cco(cco)
        
        return (valor_atual - valor_total_removido)

    
    def _calcular_impacto_financeiro(self, propostas: List[CorrectionProposal]) -> Dict[str, float]:
        """
        Calcula impacto financeiro total das propostas
        """
        if not propostas:
            return {'total_impact': 0.0, 'total_additions': 0.0, 'total_updates': 0.0}
        
        total_impact = sum(p.impact for p in propostas if (p.type == CorrectionType.IPCA_ADDITION or p.type == CorrectionType.IPCA_UPDATE))
        total_additions = sum(p.impact for p in propostas if p.type == CorrectionType.IPCA_ADDITION)
        total_updates = sum(p.impact for p in propostas if p.type == CorrectionType.IPCA_UPDATE)
        
        total_remove = sum(p.proposed_value for p in propostas if p.type == CorrectionType.DUPLICATA_ADJUSTMENT)
        
        return {
            'total_impact': total_impact + total_remove,
            'total_additions': total_additions,
            'total_updates': total_updates,
            'total_remove': total_remove,
            'proposals_count': len(propostas)
        }
    
    def _gerar_preview_data(self, session: CorrectionSession) -> Dict[str, Any]:
        """
        Gera dados para preview das correções
        """
        return {
            'session_id': session.session_id,
            'cco_id': session.cco_id,
            'scenario': session.scenario_detected,
            'corrections_count': len(session.corrections_proposed),
            'financial_impact': session.financial_impact,
            'generated_at': datetime.now(timezone.utc).isoformat()
        }
    
    def _gerar_preview_final(self, session: CorrectionSession) -> Dict[str, Any]:
        """
        Gera preview final das correções aprovadas
        """
        approved_proposals = [
            p for p in session.corrections_proposed 
            if p.correction_id in session.corrections_approved
        ]
        
        
        total_financial_impact = 0
        # verificar se existe alguma correção do tipo COMPENSATION
        if any(p.type == CorrectionType.COMPENSATION for p in approved_proposals):
            total_financial_impact = sum(p.impact for p in approved_proposals if (p.type == CorrectionType.COMPENSATION))
        
        else:
            total_financial_impact = sum(p.impact for p in approved_proposals if (p.type == CorrectionType.IPCA_ADDITION or p.type == CorrectionType.IPCA_UPDATE))
        
        return {
            'approved_corrections': [p.to_dict() for p in approved_proposals],
            'total_financial_impact': total_financial_impact,
            'ready_to_apply': True,
            'estimated_completion_time': '< 1 minuto'
        }
   
    def avaliar_ipca_ano_vigente(self, cco_id: str, user_id: str) -> Dict[str, Any]:
        """
        Avalia possibilidade de aplicar IPCA do ano vigente
        """
        try:
            # TODO verificar coleção
            cco = self.db.conta_custo_oleo_corrigida_entity.find_one({'_id': cco_id})
            if not cco:
                return {'success': False, 'error': 'CCO não encontrada'}
            
            # Verificar se tem saldo positivo
            valor_atual = self.gap_analyzer._obter_valor_atual_cco(cco)
            if valor_atual <= 0:
                return {
                    'success': True,
                    'aplicavel': False,
                    'motivo': 'CCO com saldo zero ou negativo'
                }
            
            # Calcular aniversário do ano vigente
            data_reconhecimento = self.gap_analyzer._extrair_data_reconhecimento(cco)
            if not data_reconhecimento:
                return {'success': False, 'error': 'Data de reconhecimento inválida'}
            
            ano_vigente = datetime.now().year
            mes_aniversario = data_reconhecimento.month + 1
            data_aniversario_vigente = datetime(ano_vigente, mes_aniversario, 16)
            
            # Verificar se aniversário já passou
            if datetime.now() < data_aniversario_vigente:
                return {
                    'success': True,
                    'aplicavel': False,
                    'motivo': f'Aniversário {mes_aniversario:02d}/{ano_vigente} ainda não atingido',
                    'data_aniversario': data_aniversario_vigente.strftime('%d/%m/%Y')
                }
            
            # Verificar se já existe correção para o ano vigente
            if self._existe_correcao_ano_vigente(cco, ano_vigente, mes_aniversario):
                return {
                    'success': True,
                    'aplicavel': False,
                    'motivo': f'Correção IPCA para {mes_aniversario:02d}/{ano_vigente} já existe'
                }
            
            # Calcular proposta de correção
            proposta = self._calcular_proposta_ipca_vigente(cco, ano_vigente, mes_aniversario, valor_atual)
            
    
            if proposta.get('pode_aplicar'):
                # Criar sessão similar ao cenário 0
                session_id = str(uuid.uuid4())
                correction_proposal = []
                # Criar proposta no formato CorrectionProposal
                correction_proposal.append(CorrectionProposal(
                    correction_id=str(uuid.uuid4()),
                    type=CorrectionType.IPCA_ADDITION,
                    scenario="CENARIO_IPCA_VIGENTE",
                    target_date=datetime(proposta['ano_aniversario'], proposta['mes_aniversario'], 16, tzinfo=timezone.utc),
                    target_period=proposta['periodo_aplicacao'],
                    current_value=proposta['valor_atual'],
                    proposed_value=proposta['valor_proposto'],
                    impact=proposta['impacto'],
                    taxa_aplicada=proposta['taxa_aplicada'],
                    taxa_referencia=proposta['periodo_taxa'],
                    description=f"Correção IPCA vigente para {proposta['periodo_aplicacao']}",
                    dependencies=[],
                    business_rules_applied=['IPCA_VIGENTE']
                ))
                
                correcao_mais_recente = cco['correcoesMonetarias'][-1] if cco['correcoesMonetarias'] else None
                
                if correcao_mais_recente and correcao_mais_recente['tipo'] == 'RETIFICACAO':
                    data_correcao_mais_recente = self.gap_analyzer._extrair_data_correcao(correcao_mais_recente)
                    correction_proposal.append(CorrectionProposal(
                        correction_id=str(uuid.uuid4()),
                        type=CorrectionType.CORRECTION_DATE_CHANGE,
                        scenario="CENARIO_IPCA_VIGENTE",
                        target_date=datetime(proposta['ano_aniversario'], proposta['mes_aniversario'], 15, tzinfo=timezone.utc),
                        target_period='CHANGE_ORDER',
                        current_value=0,
                        proposed_value=0,
                        impact=0,
                        taxa_aplicada=0,
                        taxa_referencia="N/A",
                        description=f"Alteração na data da correção, para inverter ordenação de correções da ultima retificação. Data atual: {data_correcao_mais_recente}; Data proposta: {datetime(proposta['ano_aniversario'], proposta['mes_aniversario'], 15, tzinfo=timezone.utc)}",
                        dependencies=[],
                        business_rules_applied=['IPCA_VIGENTE']
                    )) 
                
                # Criar sessão
                session = CorrectionSession(
                    session_id=session_id,
                    cco_id=cco_id,
                    user_id=user_id,
                    status=CorrectionStatus.PREVIEW,
                    gaps_identified=[],
                    corrections_fora_periodo=[],
                    ccos_com_duplicatas=[],
                    corrections_proposed=correction_proposal,
                    corrections_approved=[],
                    financial_impact={'total_impact': proposta['impacto']},
                    scenario_detected="CENARIO_IPCA_VIGENTE",
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc)
                )
                
                self._save_session(session)
                
                return {
                    'success': True,
                    'session_id': session_id,
                    'session_data': session.to_dict(),
                    'aplicavel': True,
                    'propostas': [p.to_dict() for p in correction_proposal],
                    'next_step': 'mostrar_preview'
                }
            
        except Exception as e:
            logger.error(f"Erro ao avaliar IPCA vigente para CCO {cco_id}: {e}")
            return {'success': False, 'error': str(e)}

    def _existe_correcao_ano_vigente(self, cco: Dict[str, Any], ano: int, mes: int) -> bool:
        """Verifica se já existe correção IPCA/IGPM para o período"""
        for correcao in cco.get('correcoesMonetarias', []):
            if correcao.get('tipo') in ['IPCA', 'IGPM']:
                data_correcao = self.gap_analyzer._extrair_data_correcao(correcao)
                if data_correcao and data_correcao.year == ano and data_correcao.month == mes:
                    return True
        return False

    def _calcular_proposta_ipca_vigente(self, cco: Dict[str, Any], ano: int, mes: int, valor_atual: float) -> Dict[str, Any]:
        """Calcula proposta de correção IPCA para ano vigente"""
        # Calcular período da taxa (mês anterior)
        ano_taxa, mes_taxa = self.gap_analyzer._calcular_mes_taxa_aplicacao(ano, mes)
        
        # Buscar taxa histórica
        taxa = self.gap_analyzer._obter_taxa_historica(ano_taxa, mes_taxa, 'IPCA')
        
        if not taxa:
            return {
                'erro': f'Taxa IPCA não encontrada para {mes_taxa:02d}/{ano_taxa}',
                'pode_aplicar': False
            }
        
        # Calcular valores
        novo_valor = valor_atual * taxa
        impacto = novo_valor - valor_atual
        
        return {
            'mes_aniversario': mes,
            'ano_aniversario': ano,
            'periodo_aplicacao': f"{mes:02d}/{ano}",
            'periodo_taxa': f"{mes_taxa:02d}/{ano_taxa}",
            'valor_atual': valor_atual,
            'taxa_aplicada': taxa,
            'valor_proposto': novo_valor,
            'impacto': impacto,
            'pode_aplicar': True,
            'observacao': f"Correção IPCA vigente para {mes:02d}/{ano} sobre saldo de R$ {valor_atual:,.2f}"
        }