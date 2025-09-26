"""
Serviço para promoção de correções IPCA/IGPM para produção
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from bson import ObjectId
from bson.int64 import Int64
from bson.decimal128 import Decimal128
import json

logger = logging.getLogger(__name__)

class IPCAPromocaoService:
    """
    Serviço responsável por gerenciar a promoção de correções IPCA/IGPM
    da coleção temporária para a coleção de produção
    """
    
    def __init__(self, db_connection, db_connection_prd=None):
        """
        Inicializa o serviço
        
        Args:
            db_connection: Conexão com MongoDB
        """
        self.db = db_connection
        self.db_prd = db_connection_prd
        logger.info("IPCAPromocaoService inicializado")
    
    def pesquisar_correcoes_pendentes(self, filtros: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Pesquisa correções IPCA/IGPM pendentes de promoção
        
        Args:
            filtros: Filtros de pesquisa (id, contrato, campo, remessa, status)
            
        Returns:
            Dict com resultados da pesquisa
        """
        try:
            # Construir query MongoDB
            query = {}
            
            if filtros:
                if filtros.get('id'):
                    query['_id'] = filtros['id']
                if filtros.get('contratoCpp'):
                    query['contratoCpp'] = filtros['contratoCpp']
                if filtros.get('campo'):
                    query['campo'] = filtros['campo']
                if filtros.get('remessa'):
                    query['remessa'] = int(filtros['remessa'])
                if filtros.get('status_promocao'):
                    query['status_promocao'] = filtros['status_promocao']
                else:
                    # Por padrão, buscar apenas pendentes
                    query['status_promocao'] = {'$in': ['PENDENTE', None]}
            
            # Buscar registros
            cursor = self.db.conta_custo_oleo_corrigida_entity.find(query)
            registros = list(cursor)
            
            # Processar resultados para resumo
            resultados_processados = []
            for registro in registros:
                resumo = self._criar_resumo_correcao(registro)
                resultados_processados.append(resumo)
            
            return {
                'success': True,
                'total_encontrados': len(resultados_processados),
                'resultados': resultados_processados
            }
            
        except Exception as e:
            logger.error(f"Erro ao pesquisar correções pendentes: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_encontrados': 0,
                'resultados': []
            }
    
    def _criar_resumo_correcao(self, registro: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cria resumo de uma correção para listagem
        
        Args:
            registro: Documento da CCO corrigida
            
        Returns:
            Dict com resumo da correção
        """
        try:
            # Extrair informações básicas
            resumo = {
                'id': registro['_id'],
                'contratoCpp': registro.get('contratoCpp', 'N/A'),
                'campo': registro.get('campo', 'N/A'),
                'remessa': registro.get('remessa', 0),
                'exercicio': registro.get('exercicio', 0),
                'faseRemessa': registro.get('faseRemessa', 'N/A'),
                'session_id': registro.get('session_id', 'N/A'),
                'status_promocao': registro.get('status_promocao', 'PENDENTE'),
                'data_criacao_correcao': registro.get('data_criacao_correcao'),
                'data_promocao': registro.get('data_promocao'),
                'usuario_promocao': registro.get('usuario_promocao')
            }
            
            # Calcular estatísticas das correções
            correcoes = registro.get('correcoesMonetarias', [])
            if correcoes:
                ultima_correcao = correcoes[-1]
                resumo['valor_atual'] = self._converter_decimal128_para_float(
                    ultima_correcao.get('valorReconhecidoComOH', 0)
                )
                resumo['total_correcoes'] = len(correcoes)
                
                # Contar correções IPCA/IGPM
                correcoes_ipca_igpm = [c for c in correcoes if c.get('tipo') in ['IPCA', 'IGPM']]
                resumo['correcoes_ipca_igpm'] = len(correcoes_ipca_igpm)
            else:
                resumo['valor_atual'] = 0
                resumo['total_correcoes'] = 0
                resumo['correcoes_ipca_igpm'] = 0
            
            # Status da CCO original
            resumo['flg_recuperado'] = registro.get('flgRecuperado', False)
            
            return resumo
            
        except Exception as e:
            logger.error(f"Erro ao criar resumo da correção {registro.get('_id', 'N/A')}: {e}")
            return {
                'id': registro.get('_id', 'ERRO'),
                'erro': str(e)
            }
    
    def detalhar_correcao(self, cco_id: str) -> Dict[str, Any]:
        """
        Detalha uma correção específica
        
        Args:
            cco_id: ID da CCO corrigida
            
        Returns:
            Dict com detalhes completos da correção
        """
        try:
            # Buscar CCO corrigida
            cco_corrigida = self.db.conta_custo_oleo_corrigida_entity.find_one({'_id': cco_id})
            if not cco_corrigida:
                return {'success': False, 'error': 'CCO corrigida não encontrada'}
            
            # Buscar CCO original para comparação
            id_original = cco_id
            cco_original = self.db_prd.conta_custo_oleo_entity.find_one({'_id': id_original})
            
            # Buscar sessão de correção se disponível
            session_id = cco_corrigida.get('session_id')
            sessao_correcao = None
            if session_id:
                sessao_correcao = self.db.ipca_correction_sessions.find_one({'session_id': session_id})
                
                
                if sessao_correcao:
                    from dateutil.parser import parse
                    for campo_data in ['created_at', 'updated_at', 'applied_at']:
                        if campo_data in sessao_correcao and isinstance(sessao_correcao[campo_data], str):
                            try:
                                sessao_correcao[campo_data] = parse(sessao_correcao[campo_data])
                            except:
                                sessao_correcao[campo_data] = None
            
            return {
                'success': True,
                'cco_corrigida': cco_corrigida,
                'cco_original': cco_original,
                'sessao_correcao': sessao_correcao,
                'pode_promover': self._validar_promocao(cco_corrigida, cco_original)
            }
            
        except Exception as e:
            logger.error(f"Erro ao detalhar correção {cco_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _validar_promocao(self, cco_corrigida: Dict[str, Any], cco_original: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Valida se uma correção pode ser promovida
        
        Args:
            cco_corrigida: CCO corrigida
            cco_original: CCO original (opcional)
            
        Returns:
            Dict com resultado da validação
        """
        try:
            validacao = {
                'pode_promover': True,
                'motivos_bloqueio': [],
                'avisos': []
            }
            
            # Verificar status
            status = cco_corrigida.get('status_promocao', 'PENDENTE')
            if status == 'PROMOVIDA':
                validacao['pode_promover'] = False
                validacao['motivos_bloqueio'].append('Correção já foi promovida para produção')
            
            # Verificar se CCO original existe
            if not cco_original:
                validacao['avisos'].append('CCO original não encontrada - será necessário verificar')
            
            # Verificar se há sessão de correção
            session_id = cco_corrigida.get('session_id')
            if not session_id:
                validacao['avisos'].append('Sessão de correção não encontrada - memória de cálculo indisponível')
            
            # Verificar versão se CCO original disponível
            if cco_original:
                versao_original = cco_original.get('version', 0)
                versao_corrigida = cco_corrigida.get('version', 0)
                
                if versao_original != versao_corrigida:
                    validacao['pode_promover'] = False
                    validacao['motivos_bloqueio'].append('CCO original tem versão diferente da corrigida - verificar possíveis conflitos')
                
                # Avisar se pode haver conflito de versão
                if versao_original > 1:
                    validacao['avisos'].append(f'CCO original tem versão {versao_original} - verificar possíveis conflitos')
            
            return validacao
            
        except Exception as e:
            logger.error(f"Erro ao validar promoção: {e}")
            return {
                'pode_promover': False,
                'motivos_bloqueio': [f'Erro na validação: {str(e)}'],
                'avisos': []
            }
    
    def promover_correcao(self, cco_id: str, user_id: str, observacoes: str = None) -> Dict[str, Any]:
        """
        Promove uma correção para produção
        
        Args:
            cco_id: ID da CCO corrigida
            user_id: ID do usuário que está promovendo
            observacoes: Observações da promoção
            
        Returns:
            Dict com resultado da promoção
        """
        try:
            # Buscar e validar CCO corrigida
            cco_corrigida = self.db.conta_custo_oleo_corrigida_entity.find_one({'_id': cco_id})
            if not cco_corrigida:
                return {'success': False, 'error': 'CCO corrigida não encontrada'}
            
            # Validar se pode promover
            id_original = cco_id.split('_')[0] if '_' in cco_id else cco_id
            cco_original = self.db_prd.conta_custo_oleo_entity.find_one({'_id': id_original})
            validacao = self._validar_promocao(cco_corrigida, cco_original)
            
            if not validacao['pode_promover']:
                return {
                    'success': False, 
                    'error': 'Promoção não permitida',
                    'motivos': validacao['motivos_bloqueio']
                }
            
            # Preparar observações para o evento
            observacoes_completas = f"Aplcação de correções (via aplicação) referente inconsistencias de IPCA/IGPM (user: {user_id})"
            if observacoes:
                observacoes_completas += f" - {observacoes}"
            
            # Aplicar promoção usando função existente
            resultado_atualizacao = self._atualizar_cco_e_criar_evento(cco_corrigida, observacoes_completas)
            
            if not resultado_atualizacao['success']:
                return {
                    'success': False,
                    'error': f"Erro ao atualizar CCO em produção: {resultado_atualizacao.get('erro', 'Erro desconhecido')}"
                }
            
            # Atualizar status na coleção temporária
            agora = datetime.now()
            self.db.conta_custo_oleo_corrigida_entity.update_one(
                {'_id': cco_id},
                {
                    '$set': {
                        'status_promocao': 'PROMOVIDA',
                        'data_promocao': agora,
                        'usuario_promocao': user_id,
                        'observacoes_promocao': observacoes,
                        'versao_promovida': resultado_atualizacao['nova_versao']
                    }
                }
            )
            
            return {
                'success': True,
                'nova_versao': resultado_atualizacao['nova_versao'],
                'data_promocao': agora.isoformat(),
                'message': 'Correção promovida com sucesso para produção'
            }
            
        except Exception as e:
            logger.error(f"Erro ao promover correção {cco_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def obter_memoria_calculo(self, session_id: str) -> Dict[str, Any]:
        """
        Obtém a memória de cálculo de uma sessão de correção
        
        Args:
            session_id: ID da sessão de correção
            
        Returns:
            Dict com dados da sessão e memória de cálculo
        """
        try:
            sessao = self.db.ipca_correction_sessions.find_one({'session_id': session_id})
            if not sessao:
                return {'success': False, 'error': 'Sessão de correção não encontrada'}
            
            # Processar dados da sessão para apresentação
            memoria_calculo = {
                'session_id': sessao['session_id'],
                'cco_id': sessao['cco_id'],
                'user_id': sessao['user_id'],
                'status': sessao['status'],
                'scenario_detected': sessao['scenario_detected'],
                'created_at': sessao['created_at'],
                'updated_at': sessao['updated_at'],
                'applied_at': sessao.get('applied_at'),
                'gaps_identified': sessao.get('gaps_identified', []),
                'corrections_fora_periodo': sessao.get('corrections_fora_periodo', []),
                'corrections_proposed': sessao.get('corrections_proposed', []),
                'corrections_approved': sessao.get('corrections_approved', []),
                'financial_impact': sessao.get('financial_impact', {}),
                'error_message': sessao.get('error_message')
            }
            
            return {
                'success': True,
                'memoria_calculo': memoria_calculo
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter memória de cálculo {session_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _converter_decimal128_para_float(self, valor) -> float:
        """Converte Decimal128 para float"""
        if isinstance(valor, Decimal128):
            return float(valor.to_decimal())
        return float(valor) if valor is not None else 0.0
    
    def _preparar_filtro_id(self, cco_id) -> Any:
        """Prepara o ID para usar como filtro no MongoDB"""
        try:
            if isinstance(cco_id, ObjectId):
                return cco_id
            
            cco_id_str = str(cco_id)
            if len(cco_id_str) == 24:
                try:
                    return ObjectId(cco_id_str)
                except:
                    return cco_id_str
            else:
                return cco_id_str
        except:
            return cco_id
    
    def _atualizar_cco_e_criar_evento(self, cco_recalculada: Dict[str, Any], observacoes: str) -> Dict[str, Any]:
        """Atualiza CCO na base principal e cria evento de atualização"""
        try:
            cco_id = cco_recalculada['_id']
            
            # Extrair ID original se necessário
            id_original = cco_id
            
            # Incrementar versão
            cco_atual = self.db_prd.conta_custo_oleo_entity.find_one({'_id': id_original})
            if not cco_atual:
                return {'success': False, 'erro': f'CCO original {id_original} não encontrada'}
                
            nova_versao = (cco_atual.get('version', 0) + 1)
            
            # Preparar dados para atualização
            dados_atualizacao = {k: v for k, v in cco_recalculada.items() 
                            if k not in ['_id', 'session_id', 'status_promocao', 'data_criacao_correcao', 
                                       'data_promocao', 'usuario_promocao', 'observacoes_promocao', 'versao_promovida']}
            
            # Limpar metadadosRetificacao das correções monetárias se existir
            if 'correcoesMonetarias' in dados_atualizacao and dados_atualizacao['correcoesMonetarias']:
                for correcao in dados_atualizacao['correcoesMonetarias']:
                    if 'metadadosRetificacao' in correcao:
                        del correcao['metadadosRetificacao']
                        
            # Atualizar versão
            dados_atualizacao['version'] = Int64(nova_versao)
            
            # Atualizar documento principal
            filter_id = self._preparar_filtro_id(id_original)
            resultado_update = self.db_prd.conta_custo_oleo_entity.update_one(
                {'_id': filter_id},
                {'$set': dados_atualizacao}
            )
            
            if resultado_update.modified_count != 1:
                return {'success': False, 'erro': 'Falha ao atualizar documento principal'}
            
            # Buscar evento base para criar novo evento
            evento_base = self.db_prd.event.find_one({
                'aggregateId': id_original,
                'aggregateType': 'sgpp.services.contacustooleo.ContaCustoOleoEntity'
            }, sort=[('version', -1)])
            
            if evento_base:
                # Criar novo evento baseado no anterior
                novo_evento = evento_base.copy()
                del novo_evento['_id']
                
                novo_evento['version'] = Int64(nova_versao)
                novo_evento['name'] = 'sgpp.services.contacustooleo.ContaCustoOleoEntityUpdatedEvent'
                novo_evento['eventDate'] = datetime.now()
                novo_evento['creationDate'] = datetime.now()
                novo_evento['username'] = 'system_ipca_correcao'
                novo_evento['createSystem'] = observacoes
                
                # Atualizar contaCustoOleoEntity com a nova versão da CCO
                if 'contaCustoOleoEntity' in novo_evento:
                    cco_para_evento = self._converter_cco_para_evento(cco_recalculada)
                    cco_para_evento['version'] = nova_versao - 1  # versão anterior no evento
                    novo_evento['contaCustoOleoEntity'] = cco_para_evento
                
                self.db_prd.event.insert_one(novo_evento)
            
            return {'success': True, 'nova_versao': nova_versao}
            
        except Exception as e:
            logger.error(f"Erro ao atualizar CCO: {e}")
            return {'success': False, 'erro': str(e)}
    
    def _converter_cco_para_evento(self, cco: Dict[str, Any]) -> Dict[str, Any]:
        """Converte CCO da estrutura da coleção para a estrutura do evento"""
        cco_evento = {}
        
        # Campos que devem ser convertidos de Decimal128 para float
        campos_decimal_para_float = [
            'valorLancamentoTotal', 'valorNaoReconhecido', 'valorReconhecido',
            'valorReconhecivel', 'valorNaoPassivelRecuperacao', 'valorReconhecidoExploracao',
            'valorReconhecidoProducao', 'valorRecusado', 'overHeadExploracao',
            'overHeadProducao', 'overHeadTotal', 'valorReconhecidoComOH','valorReconhecidoComOHOriginal','diferencaValor'
        ]
        
        # Campos que devem ser convertidos de NumberLong para int
        campos_long_para_int = [
            'remessa', 'remessaExposicao', 'exercicio', 'periodo', 'quantidadeLancamento',
            'versionRemessaGeradora', 'mesReconhecimento', 'anoReconhecimento'
        ]
        
        for campo, valor in cco.items():
            # Pular campos que não devem estar no evento
            if campo in ['_id', '_class', 'session_id', 'status_promocao', 'data_criacao_correcao', 
                        'data_promocao', 'usuario_promocao', 'observacoes_promocao', 'versao_promovida']:
                continue
                
            # ID da CCO vira 'id' no evento
            if campo == '_id':
                # Usar ID original sem sufixos
                id_original = str(valor).split('_')[0] if '_' in str(valor) else str(valor)
                cco_evento['id'] = id_original
                continue
            
            # Conversão de tipos específicos
            if campo in campos_decimal_para_float:
                if isinstance(valor, Decimal128):
                    cco_evento[campo] = float(valor.to_decimal())
                else:
                    cco_evento[campo] = float(valor) if valor is not None else 0.0
            elif campo in campos_long_para_int:
                if hasattr(valor, 'as_int64'):  # NumberLong
                    cco_evento[campo] = int(valor.as_int64())
                else:
                    cco_evento[campo] = int(valor) if valor is not None else 0
            elif campo == 'correcoesMonetarias' and valor:
                # Processar array de correções monetárias
                cco_evento[campo] = []
                for correcao in valor:
                    correcao_evento = {}
                    for campo_corr, valor_corr in correcao.items():
                        if campo_corr in campos_decimal_para_float:
                            if isinstance(valor_corr, Decimal128):
                                correcao_evento[campo_corr] = float(valor_corr.to_decimal())
                            else:
                                correcao_evento[campo_corr] = float(valor_corr) if valor_corr is not None else 0.0
                        elif campo_corr in campos_long_para_int:
                            if hasattr(valor_corr, 'as_int64'):
                                correcao_evento[campo_corr] = int(valor_corr.as_int64())
                            else:
                                correcao_evento[campo_corr] = int(valor_corr) if valor_corr is not None else 0
                        elif campo_corr == 'dataCriacaoCorrecao' and isinstance(valor_corr, datetime):
                            # Converter datetime para formato ISO string
                            correcao_evento[campo_corr] = valor_corr.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                        elif campo_corr == 'dataCorrecao' and isinstance(valor_corr, datetime):
                            # Converter datetime para formato ISO string
                            correcao_evento[campo_corr] = valor_corr.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                        else:
                            correcao_evento[campo_corr] = valor_corr
                    cco_evento[campo].append(correcao_evento)
            elif isinstance(valor, datetime):
                # Manter datetimes como estão
                cco_evento[campo] = valor
            elif isinstance(valor, ObjectId):
                # Converter ObjectId para string
                cco_evento[campo] = str(valor)
            else:
                # Manter outros tipos como estão
                cco_evento[campo] = valor
        
        # Adicionar campo id se não existir
        if 'id' not in cco_evento:
            id_original = str(cco.get('_id', '')).split('_')[0] if '_' in str(cco.get('_id', '')) else str(cco.get('_id', ''))
            cco_evento['id'] = id_original
        
        return cco_evento
    
    # Métodos para estatísticas e relatórios
    def obter_estatisticas_promocao(self) -> Dict[str, Any]:
        """
        Obtém estatísticas das promoções
        
        Returns:
            Dict com estatísticas
        """
        try:
            pipeline = [
                {
                    '$group': {
                        '_id': '$status_promocao',
                        'count': {'$sum': 1},
                        'contratos': {'$addToSet': '$contratoCpp'}
                    }
                }
            ]
            
            stats = list(self.db.conta_custo_oleo_corrigida_entity.aggregate(pipeline))
            
            return {
                'success': True,
                'estatisticas': stats,
                'total_registros': sum(s['count'] for s in stats)
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {'success': False, 'error': str(e)}