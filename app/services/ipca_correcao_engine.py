"""
IPCACorrectionEngine - Motor de Correção IPCA/IGPM
Implementa a lógica específica de correção para cada cenário identificado
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from copy import deepcopy

from bson import Decimal128

from app.services.ipca_correcao_orquestrador import CorrectionType

logger = logging.getLogger(__name__)

class IPCACorrectionEngine:
    """
    Motor de correção IPCA/IGPM
    Implementa lógica específica para cada cenário de correção
    """
    
    def __init__(self, db_connection, gap_analyzer):
        """
        Inicializa o motor de correção
        
        Args:
            db_connection: Conexão com MongoDB
            gap_analyzer: Instância do IPCAGapAnalyzer para reutilizar funções
        """
        self.db = db_connection
        self.gap_analyzer = gap_analyzer
        
        logger.info("IPCACorrectionEngine inicializado")
    
    def _converter_decimal128_para_float(self, valor) -> float:
        """
        Converte Decimal128 para float de forma segura
        """
        if valor is None:
            return 0.0
        
        try:
            if hasattr(valor, 'to_decimal'):
                return float(valor.to_decimal())
            return float(valor)
        except:
            return 0.0
    
    def calcular_correcao_cenario_0(self, cco_id: str, gaps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calcula correções para Cenário 0 - Gap simples
        
        CCO sem correção no aniversário, apenas adicionar IPCA faltante
        
        Args:
            cco_id: ID da CCO
            gaps: Lista de gaps identificados
            
        Returns:
            Lista de correções calculadas
        """
        try:
            logger.info(f"Calculando correções Cenário 0 para CCO {cco_id}")
            
            correcoes_calculadas = []
            
            if not gaps or len(gaps) == 0:
                logger.warning(f"Nenhum gap fornecido para CCO {cco_id}")
                return []
    
            
            # Buscar CCO original
            cco = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            if cco.get('flgRecuperado', False):
                logger.error(f"CCO {cco_id} está recuperada - Cenário 0 pode não ser apropriado")
            correcao_anterior = None
            for cco_gap in gaps:
                if cco_gap['_id'] == cco_id:
                    for gap in cco_gap['gaps']:
                        correcao = self._calcular_correcao_individual_gap(cco, gap, correcao_anterior=correcao_anterior)
                        correcao_anterior = correcao
                        if correcao:
                            correcoes_calculadas.append(correcao)
                        
            
            return correcoes_calculadas
            
        except Exception as e:
            logger.error(f"Erro ao calcular correções Cenário 0 para CCO {cco_id}: {e}")
            raise
        
    def aplicar_correcoes_cenario_0(self, cco_id: str, correcoes_aprovadas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aplica correções do Cenário 0 na CCO real
        """
        try:
            # Buscar CCO original
            cco_original = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco_original:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            # Aplicar cada correção
            for correcao in correcoes_aprovadas:
                nova_correcao_monetaria = self._criar_correcao_monetaria_real(correcao, cco_original)
                
                # Inserir correção na CCO
                self.db.conta_custo_oleo_entity.update_one(
                    {'_id': cco_id},
                    {
                        '$push': {'correcoesMonetarias': nova_correcao_monetaria}
                        
                    }
                )
            
            return {'success': True, 'correcoes_aplicadas': len(correcoes_aprovadas)}
            
        except Exception as e:
            logger.error(f"Erro ao aplicar correções Cenário 0: {e}")
            return {'success': False, 'error': str(e)}

    def aplicar_correcoes_cenario_1(self, cco_id: str, correcoes_aprovadas: List[Dict[str, Any]]) -> Dict[str, Any]:
        
        """
        Aplica correções do Cenário 1 na CCO real
        Reconstrói a lista de correções monetárias em ordem cronológica
        """
        try:
            # Buscar CCO original
            cco_original = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco_original:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            # Separar correções por tipo
            gaps_adicoes = []
            correcoes_updates = []
            
            for c in correcoes_aprovadas:
                tipo_correcao = c.get('type')
                if hasattr(tipo_correcao, 'value'):
                    tipo_str = tipo_correcao.value
                else:
                    tipo_str = str(tipo_correcao)
                
                if tipo_str == 'IPCA_ADDITION':
                    gaps_adicoes.append(c)
                elif tipo_str == 'IPCA_UPDATE':
                    correcoes_updates.append(c)
            
            logger.info(f"Reconstruindo lista de correções: {len(gaps_adicoes)} gaps + {len(correcoes_updates)} updates")
            
            # Reconstruir lista de correções monetárias
            nova_lista_correcoes = self._reconstruir_lista_correcoes(
                cco_original, gaps_adicoes, correcoes_updates
            )
            
            # Atualizar CCO com nova lista de correções
            resultado = self.db.conta_custo_oleo_entity.update_one(
                {'_id': cco_id},
                {'$set': {'correcoesMonetarias': nova_lista_correcoes}}
            )
            
            if resultado.modified_count == 0:
                logger.warning(f"Nenhuma modificação realizada na CCO {cco_id}")
            
            return {
                'success': True, 
                'correcoes_aplicadas': len(correcoes_aprovadas),
                'gaps_adicionados': len(gaps_adicoes),
                'correcoes_atualizadas': len(correcoes_updates),
                'total_correcoes_final': len(nova_lista_correcoes)
            }
            
        except Exception as e:
            logger.error(f"Erro ao aplicar correções Cenário 1: {e}")
            return {'success': False, 'error': str(e)}

    def _reconstruir_lista_correcoes(self, cco_original: Dict[str, Any], 
                               gaps_adicoes: List[Dict[str, Any]], 
                               correcoes_updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Reconstrói lista de correções monetárias em ordem cronológica
        """
        try:
            # Correções originais da CCO
            correcoes_originais = cco_original.get('correcoesMonetarias', [])
            
            # Criar mapa de correções que serão atualizadas
            updates_map = {}
            for update in correcoes_updates:
                target_period = update.get('target_period', '')
                if '/' in target_period:
                    mes_str, ano_str = target_period.split('/')
                    chave = (int(ano_str), int(mes_str))
                    updates_map[chave] = update
            
            # Criar mapa de gaps que serão inseridos
            gaps_map = {}
            for gap in gaps_adicoes:
                data_gap = gap.get('target_date')
                chave = (data_gap.year, data_gap.month)
                gaps_map[chave] = self._criar_correcao_monetaria_real(gap)
            
            # Combinar todas as correções
            todas_correcoes = []
            
            # PRIMEIRA PASSADA: Adicionar correções originais (atualizadas se necessário)
            for correcao_orig in correcoes_originais:
                data_correcao = self.gap_analyzer._extrair_data_correcao(correcao_orig)
                if data_correcao:
                    chave = (data_correcao.year, data_correcao.month)
                    
                    if chave in updates_map:
                        # Esta correção será atualizada
                        update = updates_map[chave]
                        correcao_atualizada = correcao_orig.copy()
                        correcao_atualizada['valorReconhecidoComOH'] = Decimal128(str(update.get('proposed_value', 0)))
                        correcao_atualizada['observacoes'] = update.get('description', '') + f" - Recalculado em {datetime.now().strftime('%d/%m/%Y')}"
                        correcao_atualizada['dataCriacaoCorrecao'] = datetime.now(timezone.utc)
                        
                        todas_correcoes.append({
                            'correcao': correcao_atualizada,
                            'data': data_correcao,
                            'tipo': 'ATUALIZADA'
                        })
                    else:
                        # Correção original sem alteração
                        todas_correcoes.append({
                            'correcao': correcao_orig,
                            'data': data_correcao,
                            'tipo': 'ORIGINAL'
                        })
            
            # SEGUNDA PASSADA: Adicionar gaps apenas se não existir correção para aquele período
            periodos_existentes = set()
            for item in todas_correcoes:
                data = item['data']
                periodos_existentes.add((data.year, data.month))
            
            for chave, gap_correcao in gaps_map.items():
                if chave not in periodos_existentes:
                    # Reconstruir data do gap para ordenação
                    ano, mes = chave
                    data_gap = datetime(ano, mes, 16, tzinfo=timezone.utc)
                    
                    todas_correcoes.append({
                        'correcao': gap_correcao,
                        'data': data_gap,
                        'tipo': 'GAP_ADICIONADO'
                    })
                else:
                    logger.warning(f"Gap para {chave[1]:02d}/{chave[0]} não foi adicionado - já existe correção para este período")
            
            # Ordenar por data
            todas_correcoes.sort(key=lambda x: x['data'])
            
            # Extrair apenas as correções ordenadas
            correcoes_finais = [item['correcao'] for item in todas_correcoes]
            
            logger.info(f"Lista reconstruída com {len(correcoes_finais)} correções:")
            for i, item in enumerate(todas_correcoes):
                logger.info(f"  {i+1}. {item['data'].strftime('%m/%Y')} - {item['tipo']}")
            
            return correcoes_finais
            
        except Exception as e:
            logger.error(f"Erro ao reconstruir lista de correções: {e}")
            raise

    def _calcular_valor_final_cco(self, cco_id: str, correcoes_aplicadas: List[Dict[str, Any]]) -> float:
        """
        Calcula valor final da CCO após todas as correções
        """
        try:
            # Buscar CCO atualizada
            cco = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco:
                return 0.0
            
            # Pegar valor da última correção monetária
            correcoes = cco.get('correcoesMonetarias', [])
            if correcoes:
                ultima_correcao = max(correcoes, 
                                    key=lambda x: self.gap_analyzer._extrair_data_correcao(x) or datetime.min)
                return self.gap_analyzer._converter_decimal128_para_float(
                    ultima_correcao.get('valorReconhecidoComOH', 0)
                )
            
            return self.gap_analyzer._converter_decimal128_para_float(
                cco.get('valorReconhecidoComOH', 0)
            )
            
        except Exception as e:
            logger.error(f"Erro ao calcular valor final da CCO: {e}")
            return 0.0

    def _criar_correcao_monetaria_real(self, correcao: Dict[str, Any], cco_original: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cria correção monetária com estrutura completa para inserção no MongoDB
        """
        # A data pode vir como datetime object
        data_correcao = correcao.get('target_date')
        if hasattr(data_correcao, 'isoformat'):
            data_correcao_str = data_correcao.isoformat()
        else:
            data_correcao_str = str(data_correcao)
        
        # Extrair informações do contrato da descrição ou usar padrão
        descricao = correcao.get('description', '')
        # calcular diferença entre valores
        diferencaValor = correcao.get('proposed_value', 0) - correcao.get('current_value', 0)
        
        
        return {
            'tipo': 'IPCA',
            'subTipo': 'RETIFICACAO',
            "contrato" : cco_original.get('contratoCpp', ''),
            "campo" : cco_original.get('campo', ''),
            'dataCorrecao': data_correcao_str,
            'dataCriacaoCorrecao': datetime.now(timezone.utc),
            "valorReconhecido" : cco_original.get('valorReconhecido', Decimal128("0")),
            'valorReconhecidoComOH': Decimal128(str(correcao.get('proposed_value', 0))),
            "overHeadExploracao" : cco_original.get('overHeadExploracao', Decimal128("0")),
            "overHeadProducao" : cco_original.get('overHeadProducao', Decimal128("0")),
            "overHeadTotal" : cco_original.get('overHeadTotal', Decimal128("0")),
            "diferencaValor" : Decimal128(str(diferencaValor)),
            'valorReconhecidoComOhOriginal': Decimal128(str(correcao.get('current_value', 0))),
            "faseRemessa" : cco_original.get('faseRemessa', ''),
            'taxaCorrecao': Decimal128(str(correcao.get('taxa_aplicada', 1.0))),
            "ativo" : True,
            "quantidadeLancamento" : cco_original.get('quantidadeLancamento', 0),
            "valorLancamentoTotal" : cco_original.get('valorLancamentoTotal', Decimal128("0")),
            "valorNaoPassivelRecuperacao" : cco_original.get('valorNaoPassivelRecuperacao', Decimal128("0")),
            "valorReconhecivel" : cco_original.get('valorReconhecivel', Decimal128("0")),
            "valorNaoReconhecido" : cco_original.get('valorNaoReconhecido', Decimal128("0")),
            "valorReconhecidoExploracao" : cco_original.get('valorReconhecidoExploracao', Decimal128("0")),
            "valorReconhecidoProducao" : cco_original.get('valorReconhecidoProducao', Decimal128("0")),
            "igpmAcumulado" : Decimal128("0"),
            "igpmAcumuladoReais" : Decimal128("0"),
            'observacoes': f"{descricao} - Aplicado em {datetime.now().strftime('%d/%m/%Y')}",
            "transferencia" : False
        }
    
    def calcular_correcao_cenario_1(self, cco_id: str, gaps: List[Dict[str, Any]], 
                               correcoes_fora: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calcula correções para Cenário 1 - Gap com correção posterior
        """
        try:
            logger.info(f"Calculando correções Cenário 1 para CCO {cco_id}")
            
            correcoes_calculadas = []
            
            # Buscar CCO original
            cco = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            # 1. Calcular correções para gaps (igual ao Cenário 0)
            gaps_correcoes = []
            correcao_anterior = None
            for cco_gap in gaps:
                if cco_gap['_id'] == cco_id:
                    for gap in cco_gap['gaps']:
                        correcao = self._calcular_correcao_individual_gap(cco, gap,correcao_anterior=correcao_anterior)
                        correcao_anterior = correcao
                        if correcao:
                            correcao['gap_id'] = f"gap_{gap['ano']}{gap['mes']:02d}"
                            gaps_correcoes.append(correcao)
                            correcoes_calculadas.append(correcao)
            
            logger.info(f"Gaps calculados: {len(gaps_correcoes)}")
            
            # 2. IMPORTANTE: Buscar correções posteriores na própria CCO
            correcoes_posteriores_cco = self._identificar_correcoes_posteriores_cco(cco, gaps_correcoes)
            
            # 3. Calcular recálculo das correções posteriores (de correcoes_fora E da CCO)
            todas_correcoes_posteriores = correcoes_fora + correcoes_posteriores_cco
            
            for cco_corr in todas_correcoes_posteriores:
                if isinstance(cco_corr, dict) and cco_corr.get('_id') == cco_id:
                    # Processar correções fora do período
                    for correcao_fora in cco_corr.get('correcoes_fora_periodo', []):
                        recalculo = self._calcular_recalculo_correcao_posterior(
                            cco, correcao_fora, gaps_correcoes
                        )
                        if recalculo:
                            correcoes_calculadas.append(recalculo)
                else:
                    # Processar correções da própria CCO
                    recalculo = self._calcular_recalculo_correcao_cco(
                        cco, cco_corr, gaps_correcoes
                    )
                    if recalculo:
                        correcoes_calculadas.append(recalculo)
            
            logger.info(f"Total de correções calculadas: {len(correcoes_calculadas)}")
            return correcoes_calculadas
            
        except Exception as e:
            logger.error(f"Erro ao calcular correções Cenário 1 para CCO {cco_id}: {e}")
            raise
    
    def _identificar_correcoes_posteriores_cco(self, cco: Dict[str, Any], 
                                          gaps_correcoes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Identifica correções IPCA/IGPM na própria CCO que são posteriores aos gaps
        """
        try:
            if not gaps_correcoes:
                return []
            
            # Data do gap mais antigo
            gap_mais_antigo = min(
                datetime(gap['ano_gap'], gap['mes_gap'], 1, tzinfo=timezone.utc) 
                for gap in gaps_correcoes
            )
            
            correcoes_posteriores = []
            correcoes_monetarias = cco.get('correcoesMonetarias', [])
            
            for correcao in correcoes_monetarias:
                if correcao.get('tipo') in ['IPCA', 'IGPM']:
                    data_correcao = self.gap_analyzer._extrair_data_correcao(correcao)
                    
                    if data_correcao and data_correcao > gap_mais_antigo:
                        # Formatar como correção posterior
                        correcao_posterior = {
                            'ano_aplicado': data_correcao.year,
                            'mes_aplicado': data_correcao.month,
                            'tipo_correcao': correcao.get('tipo'),
                            'taxa_aplicada': self.gap_analyzer._converter_decimal128_para_float(
                                correcao.get('taxaCorrecao', 1.0)
                            ),
                            'valor_base_na_aplicacao': self.gap_analyzer._converter_decimal128_para_float(
                                correcao.get('valorReconhecidoComOhOriginal', 0)
                            ),
                            'valor_atual': self.gap_analyzer._converter_decimal128_para_float(
                                correcao.get('valorReconhecidoComOH', 0)
                            ),
                            'correcao_original': correcao
                        }
                        correcoes_posteriores.append(correcao_posterior)
            
            logger.info(f"Identificadas {len(correcoes_posteriores)} correções posteriores aos gaps")
            return correcoes_posteriores
            
        except Exception as e:
            logger.error(f"Erro ao identificar correções posteriores: {e}")
            return []

    def _calcular_recalculo_correcao_cco(self, cco: Dict[str, Any], 
                                        correcao_posterior: Dict[str, Any],
                                        gaps_corrigidos: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Calcula recálculo de correção que existe na própria CCO
        """
        try:
            # Data da correção posterior
            data_correcao_posterior = datetime(
                correcao_posterior['ano_aplicado'], 
                correcao_posterior['mes_aplicado'], 
                1, 
                tzinfo=timezone.utc
            )
            
            # Filtrar gaps que deveriam ter sido aplicados antes desta correção
            gaps_anteriores = [
                gap for gap in gaps_corrigidos 
                if datetime(gap['ano_gap'], gap['mes_gap'], 1, tzinfo=timezone.utc) < data_correcao_posterior
            ]
            
            if not gaps_anteriores:
                return None
            
            # Valor base original usado na correção
            valor_base_incorreto = correcao_posterior.get('valor_base_na_aplicacao', 0)
            
            # Calcular valor base correto (somar impactos dos gaps anteriores)
            ajuste_gaps = sum(gap.get('impacto', 0) for gap in gaps_anteriores)
            valor_base_correto = valor_base_incorreto + ajuste_gaps
            
            # Taxa aplicada na correção original
            taxa_aplicada = correcao_posterior.get('taxa_aplicada', 1.0)
            
            # Calcular novo valor corrigido
            novo_valor_corrigido = valor_base_correto * taxa_aplicada
            valor_atual_incorreto = correcao_posterior.get('valor_atual', 0)
            
            # Impacto da correção
            impacto = novo_valor_corrigido - valor_atual_incorreto
            
            return {
                'tipo': 'IPCA_UPDATE',
                'subtipo': 'RETIFICACAO',
                'ano_aplicado': correcao_posterior['ano_aplicado'],
                'mes_aplicado': correcao_posterior['mes_aplicado'],
                'periodo_alvo': f"{correcao_posterior['mes_aplicado']:02d}/{correcao_posterior['ano_aplicado']}",
                'data_correcao': datetime.now(timezone.utc),
                'valor_original': valor_atual_incorreto,
                'valor_corrigido': novo_valor_corrigido,
                'valor_base_incorreto': valor_base_incorreto,
                'valor_base_correto': valor_base_correto,
                'ajuste_gaps': ajuste_gaps,
                'impacto': impacto,
                'taxa_aplicada': taxa_aplicada,
                'descricao': f"Recálculo de correção {correcao_posterior['tipo_correcao']} {correcao_posterior['mes_aplicado']:02d}/{correcao_posterior['ano_aplicado']}",
                'observacoes': f"Correção atualizada devido a inclusão manual de Correção por IPCA ref aos períodos anteriores",
                'gaps_relacionados': [gap['gap_id'] for gap in gaps_anteriores]
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular recálculo de correção da CCO: {e}")
            return None
    
    def calcular_correcao_cenario_2(self, cco_id: str, gaps: List[Dict[str, Any]], 
                                   correcoes_fora: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calcula correções para Cenário 2 - Gap com recuperação posterior
        
        Gap + recuperação posterior + CCO possivelmente zerada
        Necessário: adicionar gap + compensar diferença + reativar CCO
        
        Args:
            cco_id: ID da CCO
            gaps: Lista de gaps identificados
            correcoes_fora: Lista de correções fora do período
            
        Returns:
            Lista de correções calculadas
        """
        try:
            logger.info(f"Calculando correções Cenário 2 para CCO {cco_id}")
            
            correcoes_calculadas = []
            
            # Buscar CCO original
            cco = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            # 1. Calcular correções para gaps
            gaps_correcoes = []
            correcao_anterior = None
            for cco_gap in gaps:
                if cco_gap['_id'] == cco_id:
                    for gap in cco_gap['gaps']:
                        correcao = self._calcular_correcao_individual_gap(cco, gap,correcao_anterior=correcao_anterior)
                        correcao_anterior = correcao
                        if correcao:
                            correcao['gap_id'] = f"gap_{gap['ano']}{gap['mes']:02d}"
                            gaps_correcoes.append(correcao)
                            correcoes_calculadas.append(correcao)
            
            # 2. Identificar recuperações
            recuperacoes = self._identificar_recuperacoes(cco)
            
            # 3. Calcular compensação por recuperação
            if recuperacoes and gaps_correcoes:
                compensacao = self._calcular_compensacao_recuperacao_cenario2(
                    cco, gaps_correcoes, recuperacoes
                )
                if compensacao:
                    correcoes_calculadas.append(compensacao)
            
            # 4. Calcular reativação se necessário
            if cco.get('flgRecuperado', False):
                valor_total_adicoes = sum(corr.get('impacto', 0) for corr in correcoes_calculadas)
                if valor_total_adicoes > 0:
                    reativacao = self._calcular_reativacao_cco_cenario2(cco, valor_total_adicoes)
                    if reativacao:
                        correcoes_calculadas.append(reativacao)
            
            logger.info(f"Cenário 2 - {len(correcoes_calculadas)} correções calculadas")
            return correcoes_calculadas
            
        except Exception as e:
            logger.error(f"Erro ao calcular correções Cenário 2 para CCO {cco_id}: {e}")
            raise
    
    def calcular_correcao_cenario_duplicatas(self, cco_id: str, duplicatas: List[Dict]) -> List[Dict]:
        """Calcula correção para remoção de duplicatas"""
        cco = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
        correcoes_calculadas = []
        
        for duplicata in duplicatas:
            valor_a_remover = duplicata['valor_duplicado']
            
            correcao_remocao = {
                'tipo': 'DUPLICATA_REMOVAL',
                'periodo_duplicado': duplicata['periodo'],
                'valor_original': 0,
                'valor_corrigido': -valor_a_remover,
                'impacto': -valor_a_remover,
                'descricao': f"Remoção de duplicata IPCA/IGPM {duplicata['periodo']}",
                'indice_remover': duplicata['indice']
            }
            correcoes_calculadas.append(correcao_remocao)
        
        return correcoes_calculadas
    
    def _identificar_recuperacoes(self, cco: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Identifica recuperações na CCO
        """
        recuperacoes = []
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            if correcao.get('tipo') == 'RECUPERACAO':
                data_recuperacao = self.gap_analyzer._extrair_data_correcao(correcao)
                recuperacao_info = {
                    'data_recuperacao': data_recuperacao,
                    'valor_recuperado': self.gap_analyzer._converter_decimal128_para_float(
                        correcao.get('valorRecuperado', 0)
                    ),
                    'valor_antes': self.gap_analyzer._converter_decimal128_para_float(
                        correcao.get('valorReconhecidoComOhOriginal', 0)
                    ),
                    'valor_depois': self.gap_analyzer._converter_decimal128_para_float(
                        correcao.get('valorReconhecidoComOH', 0)
                    ),
                    'correcao_original': correcao
                }
                recuperacoes.append(recuperacao_info)
        
        return recuperacoes
    
    def _calcular_compensacao_recuperacao_cenario2(self, cco: Dict[str, Any], 
                                             gaps_corrigidos: List[Dict[str, Any]],
                                             recuperacoes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Calcula compensação necessária devido a recuperação posterior aos gaps
        """
        try:
            if not recuperacoes:
                return None
            
            # Pegar a recuperação mais recente
            ultima_recuperacao = max(recuperacoes, key=lambda x: x['data_recuperacao'])
            data_recuperacao = ultima_recuperacao['data_recuperacao']
            
            gaps_anteriores_recuperacao = [
                gap for gap in gaps_corrigidos
                if (datetime(gap['ano_gap'], gap['mes_gap'], 1, tzinfo=timezone.utc) < data_recuperacao 
                    and gap.get('impacto', 0) > 0)
            ]
            
            if not gaps_anteriores_recuperacao:
                print("Nenhum gap válido anterior à recuperação encontrado")
                logger.info("Nenhum gap válido anterior à recuperação encontrado")
                return None
            
            valor_total_gaps = sum(gap.get('impacto', 0) for gap in gaps_anteriores_recuperacao)
            
            if valor_total_gaps <= 0:
                return None
            
            # ADIÇÃO: Calcular efeito cascata em correções posteriores
            data_gap_mais_recente = max(
                datetime(gap['ano_gap'], gap['mes_gap'], 1, tzinfo=timezone.utc) 
                for gap in gaps_anteriores_recuperacao
            )
            
            correcoes_posteriores = self._identificar_correcoes_ipca_posteriores_gap(
                cco, data_gap_mais_recente
            )
            
            valor_compensacao_final = self._calcular_efeito_cascata_gaps(
                valor_total_gaps, correcoes_posteriores
            )
            
            # Gerar observação detalhada
            observacao_detalhada = self._gerar_observacao_compensacao_cascata(
                valor_total_gaps, correcoes_posteriores, valor_compensacao_final
            )
            
            return {
                'tipo': 'COMPENSATION',
                'subtipo': 'RETIFICACAO',
                'data_correcao': datetime.now(timezone.utc),
                'periodo_alvo': 'COMPENSACAO',
                'valor_original': 0,
                'valor_corrigido': valor_compensacao_final,  # ALTERADO: valor com cascata
                'impacto': valor_compensacao_final,          # ALTERADO: valor com cascata
                'taxa_aplicada': 1.0,
                'descricao': observacao_detalhada,
                'observacoes': observacao_detalhada,
                'dependencies': [gap['gap_id'] for gap in gaps_anteriores_recuperacao],
                'recuperacao_relacionada': ultima_recuperacao['valor_recuperado'],
                # ADIÇÃO: Campos informativos
                'valor_base_gaps': valor_total_gaps,
                'valor_efeito_cascata': valor_compensacao_final - valor_total_gaps if correcoes_posteriores else 0
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular compensação por recuperação: {e}")
            return None
        
    
        
    def _calcular_reativacao_cco_cenario2(self, cco: Dict[str, Any], 
                                    valor_total_adicoes: float) -> Optional[Dict[str, Any]]:
        """
        Calcula reativação da CCO se estava recuperada e agora tem saldo positivo
        """
        try:
            if not cco.get('flgRecuperado', False):
                return None
            
            if valor_total_adicoes <= 0:
                return None
            
            return {
                'tipo': 'REACTIVATION',
                'subtipo': 'AJUSTE_FLAG',
                'data_correcao': datetime.now(timezone.utc),
                'periodo_alvo': 'REATIVACAO',
                'valor_original': 0,
                'valor_corrigido': valor_total_adicoes,
                'impacto': 0,  # Não impacta valor monetário, apenas flag
                'taxa_aplicada': 1.0,
                'descricao': f"Reativação da CCO - flgRecuperado: true → false",
                'observacoes': f"CCO reativada devido a saldo positivo de R$ {valor_total_adicoes:,.2f} após correções",
                'dependencies': [],
                'flag_change': {'flgRecuperado': False}
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular reativação da CCO: {e}")
            return None
       
    def _identificar_correcoes_ipca_posteriores_gap(self, cco: Dict[str, Any], 
                                              data_gap: datetime) -> List[Dict[str, Any]]:
        """
        Identifica correções IPCA/IGPM posteriores à data do gap
        """
        correcoes_posteriores = []
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            if correcao.get('tipo') in ['IPCA', 'IGPM']:
                data_correcao = self.gap_analyzer._extrair_data_correcao(correcao)
                
                if data_correcao and data_correcao > data_gap:
                    taxa = self.gap_analyzer._converter_decimal128_para_float(
                        correcao.get('taxaCorrecao', 1.0)
                    )
                    
                    correcoes_posteriores.append({
                        'data_correcao': data_correcao,
                        'taxa_correcao': taxa,
                        'tipo': correcao.get('tipo')
                    })
        
        return sorted(correcoes_posteriores, key=lambda x: x['data_correcao'])

    def _calcular_efeito_cascata_gaps(self, valor_base_gaps: float, 
                                    correcoes_posteriores: List[Dict[str, Any]]) -> float:
        """
        Calcula efeito cascata aplicando taxas das correções posteriores sobre gaps
        """
        if not correcoes_posteriores:
            return valor_base_gaps
        
        valor_acumulado = valor_base_gaps
        
        for correcao_posterior in correcoes_posteriores:
            taxa = correcao_posterior['taxa_correcao']
            valor_antes_taxa = valor_acumulado
            valor_apos_taxa = valor_acumulado * taxa
            incremento = valor_apos_taxa - valor_antes_taxa
            
            logger.info(f"Efeito cascata - Taxa {taxa:.4f}: R$ {valor_antes_taxa:,.2f} → R$ {valor_apos_taxa:,.2f} (+R$ {incremento:,.2f})")
            
            valor_acumulado = valor_apos_taxa
        
        return valor_acumulado
     
    def _gerar_observacao_compensacao_cascata(self, valor_base_gaps: float, 
                                        correcoes_posteriores: List[Dict[str, Any]], 
                                        valor_final: float) -> str:
        """
        Gera observação detalhada do cálculo de compensação com efeito cascata
        """
        observacao = f"Compensação por gaps IPCA/IGPM aplicados após recuperação. "
        observacao += f"Valor base gaps: R$ {valor_base_gaps:,.2f}"
        
        if correcoes_posteriores:
            observacao += f". Efeito cascata aplicado sobre {len(correcoes_posteriores)} correção(ões) posterior(es): "
            
            valor_acumulado = valor_base_gaps
            detalhes_cascata = []
            
            for i, correcao in enumerate(correcoes_posteriores):
                taxa = correcao['taxa_correcao']
                data_correcao = correcao['data_correcao']
                valor_anterior = valor_acumulado
                valor_posterior = valor_acumulado * taxa
                
                detalhes_cascata.append(
                    f"{data_correcao.strftime('%m/%Y')} (taxa {taxa:.4f}): "
                    f"R$ {valor_anterior:,.2f} → R$ {valor_posterior:,.2f}"
                )
                
                valor_acumulado = valor_posterior
            
            observacao += "; ".join(detalhes_cascata)
            observacao += f". Compensação final: R$ {valor_final:,.2f}"
        else:
            observacao += ". Nenhuma correção posterior identificada"
        
        return observacao
     
    def aplicar_correcoes_cenario_2(self, cco_id: str, correcoes_aprovadas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aplica correções do Cenário 2 na CCO real
        """
        try:
            # Buscar CCO original
            cco_original = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco_original:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            # Separar correções por tipo
            gaps_adicoes = []
            compensacoes = []
            reativacoes = []
            
            for c in correcoes_aprovadas:
                tipo_correcao = c.get('type')
                if hasattr(tipo_correcao, 'value'):
                    tipo_str = tipo_correcao.value
                else:
                    tipo_str = str(tipo_correcao)
                
                if tipo_str == 'IPCA_ADDITION':
                    gaps_adicoes.append(c)
                elif tipo_str == 'COMPENSATION':
                    compensacoes.append(c)
                elif tipo_str == 'REACTIVATION':
                    reativacoes.append(c)
            
            logger.info(f"Cenário 2 - Aplicando: {len(gaps_adicoes)} gaps, {len(compensacoes)} compensações, {len(reativacoes)} reativações")
            
            # Reconstruir lista de correções (similar ao Cenário 1, mas incluindo compensações)
            nova_lista_correcoes = self._reconstruir_lista_correcoes_cenario2(
                cco_original, gaps_adicoes, compensacoes
            )
            
            lista_correcoes_ajustada = self._ajustar_atributos_correcoes_ipca(nova_lista_correcoes)
            
            # Preparar update
            update_data = {'correcoesMonetarias': lista_correcoes_ajustada}
            
            # Adicionar reativação se necessário
            if reativacoes:
                update_data['flgRecuperado'] = False
            
            
            # TODO criando novos registros, ao invés de alterar os existentes. Ajustar isso futuramente
            sufixo = "" #"_corrigida_cenario_2"
            novo_id = cco_id + sufixo

            if self.db.conta_custo_oleo_corrigida_entity.find_one({'_id': novo_id}):
                self.db.conta_custo_oleo_corrigida_entity.delete_one({'_id': novo_id})

            cco_corrigida = cco_original.copy()
            cco_corrigida['_id'] = novo_id
            cco_corrigida['correcoesMonetarias'] = nova_lista_correcoes
            if reativacoes:
                cco_corrigida['flgRecuperado'] = False
            
            # Inserir CCO corrigida
            resultado = self.db.conta_custo_oleo_corrigida_entity.insert_one(cco_corrigida)
        
            # # Atualizar CCO
            # resultado = self.db.conta_custo_oleo_entity.update_one(
            #     {'_id': cco_id},
            #     {'$set': update_data}
            # )
            
            
            
            return {
                'success': True, 
                'correcoes_aplicadas': len(correcoes_aprovadas),
                'gaps_adicionados': len(gaps_adicoes),
                'compensacoes_aplicadas': len(compensacoes),
                'cco_reativada': len(reativacoes) > 0,
                'total_correcoes_final': len(nova_lista_correcoes)
            }
            
        except Exception as e:
            logger.error(f"Erro ao aplicar correções Cenário 2: {e}")
            return {'success': False, 'error': str(e)}
        
    def aplicar_correcoes_cenario_duplicatas(self, cco_id: str, correcoes_aprovadas: List[Dict]) -> Dict:
        """Aplica correção de duplicatas"""
        cco_original = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
        
        # Remover correções duplicadas (índices em ordem reversa)
        correcoes_monetarias = cco_original['correcoesMonetarias'].copy()
        correcoes_com_indice = [c for c in correcoes_aprovadas if c.get('indice_remover') is not None]
        indices_remover = sorted([c['indice_remover'] for c in correcoes_com_indice], reverse=True)

        resumo_correcoes_removidas = [{'target_period': p['target_period'],
                                'target_date': p['target_date'],
                                'current_value': p['current_value']
                                } for p in correcoes_com_indice]
        for indice in indices_remover:
            del correcoes_monetarias[indice]
        
        # recuperar correcao do tipo DUPLICATA_ADJUSTMENT da lista de correções aprovadas
        correcoes_ajustes_duplicadas = [c for c in correcoes_aprovadas if c.get('type') == CorrectionType.DUPLICATA_ADJUSTMENT]
        valor_total_removido = sum([c.get('proposed_value', 0) for c in correcoes_aprovadas if c.get('type') == CorrectionType.DUPLICATA_ADJUSTMENT])
        print(f"aplicar_correcoes_cenario_duplicatas: Valor total a ser considerado na compensação (removido total): {valor_total_removido}")
        
        # Criar correção de ajuste se necessário
        if valor_total_removido != 0:
            correcoes_monetarias.append(self._criar_correcao_ajuste_duplicata(cco_original, correcoes_monetarias[-1], valor_total_removido, resumo_correcoes_removidas, correcoes_ajustes_duplicadas))
        
        valor_atual = self._converter_decimal128_para_float(correcoes_monetarias[-1]['valorReconhecidoComOH']) 
        
        flag_recuperado = cco_original.get('flgRecuperado', False)
        reativacao = False
        
        if valor_atual != 0 and flag_recuperado:
            flag_recuperado = False
            reativacao = True
        
        
        #Inserir registro na coeção temporaria de CCO
        if self.db.conta_custo_oleo_corrigida_entity.find_one({'_id': cco_id}):
            self.db.conta_custo_oleo_corrigida_entity.delete_one({'_id': cco_id})

        cco_corrigida = cco_original.copy()
        cco_corrigida['_id'] = cco_id
        cco_corrigida['correcoesMonetarias'] = correcoes_monetarias
        
        cco_corrigida['flgRecuperado'] = flag_recuperado
        
        # Inserir CCO corrigida
        resultado = self.db.conta_custo_oleo_corrigida_entity.insert_one(cco_corrigida)
    
        
        return {
            'success': True, 
            'correcoes_aplicadas': len(correcoes_aprovadas),
            'gaps_adicionados': 0,
            'compensacoes_aplicadas': len(correcoes_com_indice),
            'cco_reativada': 1 if reativacao else 0,
            'total_correcoes_final': len(correcoes_monetarias)
        }
        
        # Atualizar CCO
        # self.db.conta_custo_oleo_entity.update_one(
        #     {'_id': cco_id + '_corrigida_duplicatas'},
        #     {'$set': {
        #         'correcoesMonetarias': correcoes_monetarias,
        #         'flgRecuperado': flag_recuperado
        #     }},
        #     upsert=True
        # )
        
    def aplicar_correcoes_cenario_ipca_vigente(self, cco_id: str, correcoes_aprovadas: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aplica correções do Cenário 0 na CCO real
        """
        try:
            # Buscar CCO original TODO verificar coleção
            cco_original = self.db.conta_custo_oleo_corrigida_entity.find_one({'_id': cco_id})
            if not cco_original:
                raise ValueError(f"CCO {cco_id} não encontrada")
            
            correcoes_atuais = cco_original.get('correcoesMonetarias', [])
            
            # recuperar da lista de correções aprovadas apenas as de IPCA (CorrectionType.IPCA_ADDITION)
            correcoes_aprovadas_ipca = [c for c in correcoes_aprovadas if c.get('type') == CorrectionType.IPCA_ADDITION]
            
            # recuperar da lista de correções aprovadas apenas as de alteração de data (CorrectionType.CORRECTION_DATE_CHANGE)
            correcoes_aprovadas_alteracao_data = [c for c in correcoes_aprovadas if c.get('type') == CorrectionType.CORRECTION_DATE_CHANGE]
            
            # Verifica se existe mais de uma correção na lista de correções aprovadas
            if len(correcoes_aprovadas_ipca) > 1:
                return {'success': False, 'error': 'Mais de uma correção na lista de correções aprovadas'}
            
            if correcoes_aprovadas_alteracao_data:
                print("Alterando data ultima correção de retificação da CCO")
                data_correcao = correcoes_aprovadas_alteracao_data[0]['target_date']
                if hasattr(data_correcao, 'isoformat'):
                    data_correcao_str = data_correcao.isoformat()
                else:
                    data_correcao_str = str(data_correcao)
                correcoes_atuais[-1]['dataCorrecao'] = data_correcao_str
                
            
            # Aplicar cada correção
            for correcao in correcoes_aprovadas_ipca:
                correcoes_atuais.append(self._criar_correcao_monetaria_real(correcao, cco_original))
                
            lista_correcoes_ajustada = self._ajustar_atributos_correcoes_ipca(correcoes_atuais)   
            
            
                
            # Atualizar CCO com nova lista de correções
            resultado = self.db.conta_custo_oleo_corrigida_entity.update_one(
                {'_id': cco_id},
                {'$set': {'correcoesMonetarias': lista_correcoes_ajustada}}
            )
            
            return {'success': True, 'correcoes_aplicadas': len(correcoes_aprovadas)}
            
        except Exception as e:
            logger.error(f"Erro ao aplicar correções Cenário 0: {e}")
            return {'success': False, 'error': str(e)}
    
    def _reconstruir_lista_correcoes_cenario2(self, cco_original: Dict[str, Any], 
                                            gaps_adicoes: List[Dict[str, Any]], 
                                            compensacoes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Reconstrói lista para Cenário 2 - inclui gaps e compensações
        """
        try:
            correcoes_originais = cco_original.get('correcoesMonetarias', [])
            todas_correcoes = []
            
            # Adicionar correções originais
            for correcao_orig in correcoes_originais:
                data_correcao = self.gap_analyzer._extrair_data_correcao(correcao_orig)
                if data_correcao:
                    todas_correcoes.append({
                        'correcao': correcao_orig,
                        'data': data_correcao,
                        'tipo': 'ORIGINAL'
                    })
            
            # Adicionar gaps
            for gap in gaps_adicoes:
                nova_correcao = self._criar_correcao_monetaria_real(gap, cco_original)
                data_gap = gap.get('target_date')
                
                todas_correcoes.append({
                    'correcao': nova_correcao,
                    'data': data_gap,
                    'tipo': 'GAP_ADICIONADO'
                })
            
            # Adicionar compensações (sempre no final)
            for compensacao in compensacoes:
                nova_compensacao = self._criar_compensacao_monetaria(compensacao, cco_original)
                data_compensacao = compensacao.get('target_date', datetime.now(timezone.utc))
                
                todas_correcoes.append({
                    'correcao': nova_compensacao,
                    'data': data_compensacao,
                    'tipo': 'COMPENSACAO'
                })
            
            # Ordenar por data
            todas_correcoes.sort(key=lambda x: x['data'])
            
            correcoes_finais = [item['correcao'] for item in todas_correcoes]
            
            logger.info(f"Cenário 2 - Lista reconstruída com {len(correcoes_finais)} correções")
            return correcoes_finais
            
        except Exception as e:
            logger.error(f"Erro ao reconstruir lista Cenário 2: {e}")
            raise
    
    def _ajustar_atributos_correcoes_ipca(self, correcoes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ajusta atributos específicos das correções IPCA/IGPM conforme regra de acumulação
        """
        # Inicializar variáveis
        valorLancamentoTotal = 0
        valorNaoReconhecido = 0
        valorReconhecivel = 0
        valorNaoPassivelRecuperacao = 0
        igpmAcumulado = 0
        igpmAcumuladoReais = 0
        
        correcoes_ipca_igpm = []
        
        # Filtrar apenas correções IPCA/IGPM
        for i, correcao in enumerate(correcoes):
            if correcao.get('tipo') in ['IPCA', 'IGPM']:
                correcoes_ipca_igpm.append((i, correcao))
        
        for idx, (indice_original, correcao) in enumerate(correcoes_ipca_igpm):
            # Recuperar diferença de valor
            diferencaValor = self._converter_decimal128_para_float(
                correcao.get('diferencaValor', Decimal128("0"))
            )
            
            # Recuperar taxa de correção
            taxaCorrecao = self._converter_decimal128_para_float(
                correcao.get('taxaCorrecao', Decimal128("1"))
            )
            
            if idx == 0:  # Primeira correção IPCA/IGPM
                # Recuperar valores base da correção e aplicar taxa
                valorLancamentoTotal = self._converter_decimal128_para_float(
                    correcao.get('valorLancamentoTotal', Decimal128("0"))
                ) * taxaCorrecao
                
                valorNaoReconhecido = self._converter_decimal128_para_float(
                    correcao.get('valorNaoReconhecido', Decimal128("0"))
                ) * taxaCorrecao
                
                valorReconhecivel = self._converter_decimal128_para_float(
                    correcao.get('valorReconhecivel', Decimal128("0"))
                ) * taxaCorrecao
                
                valorNaoPassivelRecuperacao = self._converter_decimal128_para_float(
                    correcao.get('valorNaoPassivelRecuperacao', Decimal128("0"))
                ) * taxaCorrecao
                
                # Atualizar correção
                correcao['valorLancamentoTotal'] = Decimal128(str(round(valorLancamentoTotal, 15)))
                correcao['valorNaoReconhecido'] = Decimal128(str(round(valorNaoReconhecido, 15)))
                correcao['valorReconhecivel'] = Decimal128(str(round(valorReconhecivel, 15)))
                correcao['valorNaoPassivelRecuperacao'] = Decimal128(str(round(valorNaoPassivelRecuperacao, 15)))
                
                # Valores de IGPM
                igpmAcumulado = taxaCorrecao
                igpmAcumuladoReais = diferencaValor
                
                correcao['igpmAcumulado'] = Decimal128(str(round(igpmAcumulado, 15)))
                correcao['igpmAcumuladoReais'] = Decimal128(str(round(igpmAcumuladoReais, 15)))
                
            else:  # Correções subsequentes (índice > 0)
                # Aplicar taxa sobre valores acumulados
                valorLancamentoTotal = valorLancamentoTotal * taxaCorrecao
                valorNaoReconhecido = valorNaoReconhecido * taxaCorrecao
                valorReconhecivel = valorReconhecivel * taxaCorrecao
                valorNaoPassivelRecuperacao = valorNaoPassivelRecuperacao * taxaCorrecao
                
                # Atualizar correção
                correcao['valorLancamentoTotal'] = Decimal128(str(round(valorLancamentoTotal, 15)))
                correcao['valorNaoReconhecido'] = Decimal128(str(round(valorNaoReconhecido, 15)))
                correcao['valorReconhecivel'] = Decimal128(str(round(valorReconhecivel, 15)))
                correcao['valorNaoPassivelRecuperacao'] = Decimal128(str(round(valorNaoPassivelRecuperacao, 15)))
                
                # Valores de IGPM acumulados
                igpmAcumulado = igpmAcumulado + taxaCorrecao
                igpmAcumuladoReais = igpmAcumuladoReais + diferencaValor
                
                correcao['igpmAcumulado'] = Decimal128(str(round(igpmAcumulado, 15)))
                correcao['igpmAcumuladoReais'] = Decimal128(str(round(igpmAcumuladoReais, 15)))
        
        return correcoes
    
    # def simular_cco_com_correcoes(self, cco_original: Dict[str, Any], 
    #                              correcoes: List[Dict[str, Any]]) -> Dict[str, Any]:
    #     """
    #     Simula como ficaria a CCO após aplicar as correções
        
    #     Args:
    #         cco_original: CCO original
    #         correcoes: Lista de correções a aplicar
            
    #     Returns:
    #         CCO simulada com correções aplicadas
    #     """
    #     try:
    #         # Fazer cópia profunda da CCO original
    #         cco_simulada = deepcopy(cco_original)
            
    #         # Aplicar cada correção na simulação
    #         for correcao in correcoes:
    #             self._aplicar_correcao_simulacao(cco_simulada, correcao)
            
    #         return cco_simulada
            
    #     except Exception as e:
    #         logger.error(f"Erro ao simular CCO com correções: {e}")
    #         raise
    
    def validar_correcoes(self, cco_id: str, correcoes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Valida se as correções são consistentes e aplicáveis
        
        Args:
            cco_id: ID da CCO
            correcoes: Lista de correções a validar
            
        Returns:
            Resultado da validação
        """
        try:
            logger.info(f"Validando {len(correcoes)} correções para CCO {cco_id}")
            
            validacao = {
                'valido': True,
                'warnings': [],
                'errors': [],
                'correcoes_validadas': len(correcoes)
            }
            
            # Buscar CCO original
            cco = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco:
                validacao['valido'] = False
                validacao['errors'].append(f"CCO {cco_id} não encontrada")
                return validacao
            
            # Validações específicas
            validacao = self._validar_sequencia_temporal(correcoes, validacao)
            validacao = self._validar_valores_positivos(correcoes, validacao)
            validacao = self._validar_taxas_aplicadas(correcoes, validacao)
            
            return validacao
            
        except Exception as e:
            logger.error(f"Erro ao validar correções para CCO {cco_id}: {e}")
            return {
                'valido': False,
                'errors': [f"Erro interno: {str(e)}"],
                'warnings': [],
                'correcoes_validadas': 0
            }
    
    def _calcular_correcao_individual_gap(self, cco: Dict[str, Any], gap: Dict[str, Any], correcao_anterior: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Calcula correção individual para um gap específico
        """
        try:
            ano_gap = gap['ano']
            mes_gap = gap['mes']
            
            if correcao_anterior:
                valor_base = correcao_anterior['valor_corrigido']
            else:
                valor_base = gap['valor_base']
                
            # VALIDAÇÃO: Se valor base é zero ou negativo, não aplicar correção
            if valor_base <= 0:
                print(f"Gap {mes_gap:02d}/{ano_gap} ignorado - valor base é zero ou negativo: {valor_base}")
                logger.info(f"Gap {mes_gap:02d}/{ano_gap} ignorado - valor base é zero ou negativo: {valor_base}")
                return None
            
            # Calcular período da taxa (mês anterior ao gap)
            ano_taxa, mes_taxa = self.gap_analyzer._calcular_mes_taxa_aplicacao(ano_gap, mes_gap)
            
            data_correcao = datetime(ano_gap, mes_gap, 16, tzinfo=timezone.utc)
            
            # Buscar taxa histórica
            taxa = self.gap_analyzer._obter_taxa_historica(ano_taxa, mes_taxa, 'IPCA')
            if not taxa:
                logger.error(f"Taxa IPCA não encontrada para {mes_taxa:02d}/{ano_taxa}")
                return {
                    'tipo': 'IPCA_ADDITION',
                    'subtipo': 'RETIFICACAO',
                    'ano_gap': ano_gap,
                    'mes_gap': mes_gap,
                    'periodo_taxa': f"{mes_taxa:02d}/{ano_taxa}",
                    'periodo_alvo': f"{mes_gap:02d}/{ano_gap}",
                    'data_correcao': data_correcao,
                    'valor_original': 0,
                    'valor_corrigido': 0,
                    'impacto': 0,
                    'taxa_aplicada': 0,
                    'descricao': f"Correção IPCA faltante para {mes_gap:02d}/{ano_gap}",
                    'observacoes': f"Incluída via retificação manual em {datetime.now(timezone.utc).strftime('%d/%m/%Y')}",
                    'erro': f"Taxa IPCA não encontrada para {mes_taxa:02d}/{ano_taxa}"
                }
            
            # Calcular novo valor
            novo_valor = valor_base * taxa
            impacto = novo_valor - valor_base
            
            # Data da correção (dia 16 do mês/ano do gap)
            
            
            return {
                'tipo': 'IPCA_ADDITION',
                'subtipo': 'RETIFICACAO',
                'ano_gap': ano_gap,
                'mes_gap': mes_gap,
                'periodo_taxa': f"{mes_taxa:02d}/{ano_taxa}",
                'periodo_alvo': f"{mes_gap:02d}/{ano_gap}",
                'data_correcao': data_correcao,
                'valor_original': valor_base,
                'valor_corrigido': novo_valor,
                'impacto': impacto,
                'taxa_aplicada': taxa,
                'descricao': f"Correção IPCA faltante para {mes_gap:02d}/{ano_gap}",
                'observacoes': f"Incluída via retificação manual em {datetime.now(timezone.utc).strftime('%d/%m/%Y')}"
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular correção individual: {e}")
            return None
    
    def _calcular_recalculo_correcao_posterior(self, cco: Dict[str, Any], 
                                         correcao_fora: Dict[str, Any],
                                         gaps_corrigidos: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Calcula recálculo de correção posterior que usou valor base incorreto
        """
        try:
            # Data da correção fora do prazo
            data_correcao_fora = datetime(
                correcao_fora['ano_aplicado'], 
                correcao_fora['mes_aplicado'], 
                1, 
                tzinfo=timezone.utc
            )
            
            # Filtrar gaps que deveriam ter sido aplicados antes desta correção
            gaps_anteriores = [
                gap for gap in gaps_corrigidos 
                if datetime(gap['ano_gap'], gap['mes_gap'], 1) < data_correcao_fora
            ]
            
            if not gaps_anteriores:
                logger.info("Nenhum gap anterior encontrado para recálculo")
                return None
            
            # Valor base original usado na correção
            valor_base_incorreto = correcao_fora.get('valor_base_na_aplicacao', 0)
            
            # Calcular valor base correto (somar impactos dos gaps anteriores)
            ajuste_gaps = sum(gap.get('impacto', 0) for gap in gaps_anteriores)
            valor_base_correto = valor_base_incorreto + ajuste_gaps
            
            # Taxa aplicada na correção original
            taxa_aplicada = correcao_fora.get('taxa_aplicada', 1.0)
            
            # Calcular novo valor corrigido
            novo_valor_corrigido = valor_base_correto * taxa_aplicada
            valor_atual_incorreto = valor_base_incorreto * taxa_aplicada
            
            # Impacto da correção
            impacto = novo_valor_corrigido - valor_atual_incorreto
            
            return {
                'tipo': 'IPCA_UPDATE',
                'subtipo': 'RETIFICACAO',
                'ano_aplicado': correcao_fora['ano_aplicado'],
                'mes_aplicado': correcao_fora['mes_aplicado'],
                'periodo_alvo': f"{correcao_fora['mes_aplicado']:02d}/{correcao_fora['ano_aplicado']}",
                'data_correcao': datetime.now(timezone.utc),  # Data atual para a atualização
                'valor_original': valor_atual_incorreto,
                'valor_corrigido': novo_valor_corrigido,
                'valor_base_incorreto': valor_base_incorreto,
                'valor_base_correto': valor_base_correto,
                'ajuste_gaps': ajuste_gaps,
                'impacto': impacto,
                'taxa_aplicada': taxa_aplicada,
                'descricao': f"Recálculo de correção {correcao_fora['tipo_correcao']} {correcao_fora['mes_aplicado']:02d}/{correcao_fora['ano_aplicado']}",
                'observacoes': f"Correção atualizada devido a inclusão manual de Correção por IPCA ref aos períodos anteriores",
                'gaps_relacionados': [gap['gap_id'] for gap in gaps_anteriores]
            }
            
        except Exception as e:
            logger.error(f"Erro ao calcular recálculo de correção posterior: {e}")
            return None
    
    # def _calcular_compensacao_recuperacao(self, cco: Dict[str, Any], 
    #                                     gaps_corrigidos: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    #     """
    #     Calcula compensação necessária devido a recuperação posterior aos gaps
    #     """
    #     try:
    #         # Encontrar última recuperação
    #         correcoes = cco.get('correcoesMonetarias', [])
    #         recuperacoes = [c for c in correcoes if c.get('tipo') == 'RECUPERACAO']
            
    #         if not recuperacoes:
    #             return None
            
    #         # Pegar a recuperação mais recente
    #         ultima_recuperacao = max(recuperacoes, 
    #                                key=lambda x: self.gap_analyzer._extrair_data_correcao(x) or datetime.min)
            
    #         # Calcular valor total dos gaps corrigidos
    #         valor_total_gaps = sum(gap.get('impacto', 0) for gap in gaps_corrigidos)
            
    #         if valor_total_gaps <= 0:
    #             return None
            
    #         # Data da compensação (data atual)
    #         data_compensacao = datetime.now(timezone.utc)
            
    #         return {
    #             'tipo': 'COMPENSATION',
    #             'subtipo': 'RETIFICACAO',
    #             'data_correcao': data_compensacao,
    #             'valor_original': 0,
    #             'valor_corrigido': valor_total_gaps,
    #             'impacto': valor_total_gaps,
    #             'descricao': f"Compensação por gaps aplicados após recuperação",
    #             'observacoes': f"Ajuste devido a inclusão manual de Correção por IPCA após recuperação"
    #         }
            
    #     except Exception as e:
    #         logger.error(f"Erro ao calcular compensação por recuperação: {e}")
    #         return None
    
    # def _calcular_reativacao_cco(self, cco: Dict[str, Any], 
    #                            correcoes_calculadas: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    #     """
    #     Calcula reativação da CCO se estava recuperada e agora tem saldo positivo
    #     """
    #     try:
    #         if not cco.get('flgRecuperado', False):
    #             return None
            
    #         # Calcular saldo final após todas as correções
    #         saldo_final = sum(corr.get('impacto', 0) for corr in correcoes_calculadas)
            
    #         if saldo_final <= 0:
    #             return None
            
    #         return {
    #             'tipo': 'REACTIVATION',
    #             'subtipo': 'AJUSTE_FLAG',
    #             'valor_original': 0,
    #             'valor_corrigido': saldo_final,
    #             'impacto': 0,  # Não impacta valor, apenas flag
    #             'descricao': f"Reativação da CCO - flgRecuperado: true → false",
    #             'observacoes': f"CCO reativada devido a saldo positivo após correções"
    #         }
            
    #     except Exception as e:
    #         logger.error(f"Erro ao calcular reativação da CCO: {e}")
    #         return None
    
    def _calcular_valor_base_correto(self, cco: Dict[str, Any], 
                                   correcao_fora: Dict[str, Any],
                                   gaps_corrigidos: List[Dict[str, Any]]) -> float:
        """
        Calcula valor base correto considerando gaps que deveriam ter sido aplicados antes
        """
        try:
            # Valor base original da correção
            valor_base_original = correcao_fora.get('valor_base_na_aplicacao', 0)
            
            # Somar impacto dos gaps que deveriam ter sido aplicados antes
            data_correcao_fora = datetime(
                correcao_fora['ano_aplicado'], 
                correcao_fora['mes_aplicado'], 
                1, 
                tzinfo=timezone.utc
            )
            
            # Filtrar gaps que deveriam ter sido aplicados antes desta correção
            gaps_anteriores = [
                gap for gap in gaps_corrigidos 
                if datetime(gap['ano_gap'], gap['mes_gap'], 1, tzinfo=timezone.utc) < data_correcao_fora
            ]
            
            # Somar impactos dos gaps anteriores
            ajuste_gaps = sum(gap.get('impacto', 0) for gap in gaps_anteriores)
            
            return valor_base_original + ajuste_gaps
            
        except Exception as e:
            logger.error(f"Erro ao calcular valor base correto: {e}")
            return correcao_fora.get('valor_base_na_aplicacao', 0)
    
    # def _aplicar_correcao_simulacao(self, cco_simulada: Dict[str, Any], 
    #                               correcao: Dict[str, Any]):
    #     """
    #     Aplica uma correção na simulação da CCO
    #     """
    #     try:
    #         # Criar nova correção monetária
    #         nova_correcao = self._criar_correcao_monetaria(correcao)
            
    #         # Adicionar à lista de correções monetárias
    #         if 'correcoesMonetarias' not in cco_simulada:
    #             cco_simulada['correcoesMonetarias'] = []
            
    #         cco_simulada['correcoesMonetarias'].append(nova_correcao)
            
    #         # Atualizar valor atual da CCO
    #         if correcao['tipo'] == 'REACTIVATION':
    #             cco_simulada['flgRecuperado'] = False
    #         else:
    #             # Atualizar valorReconhecidoComOH
    #             valor_atual = self.gap_analyzer._converter_decimal128_para_float(
    #                 cco_simulada.get('valorReconhecidoComOH', 0)
    #             )
    #             novo_valor = valor_atual + correcao.get('impacto', 0)
    #             cco_simulada['valorReconhecidoComOH'] = novo_valor
            
    #     except Exception as e:
    #         logger.error(f"Erro ao aplicar correção na simulação: {e}")
    
    # def _criar_correcao_monetaria(self, correcao: Dict[str, Any]) -> Dict[str, Any]:
    #     """
    #     Cria estrutura de correção monetária baseada nos dados calculados
    #     """
    #     return {
    #         'tipo': correcao.get('subtipo', 'RETIFICACAO'),
    #         'subTipo': 'DEFAULT',
    #         'dataCorrecao': correcao.get('data_correcao', datetime.now(timezone.utc)).isoformat(),
    #         'dataCriacaoCorrecao': datetime.now(timezone.utc),
    #         'valorReconhecidoComOH': correcao.get('valor_corrigido', 0),
    #         'valorReconhecidoComOhOriginal': correcao.get('valor_original', 0),
    #         'taxaCorrecao': correcao.get('taxa_aplicada', 1.0),
    #         'ativo': True,
    #         'observacoes': correcao.get('observacoes', ''),
    #         'transferencia': False
    #     }
        
    def _criar_compensacao_monetaria(self, compensacao: Dict[str, Any], cco_original: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cria correção monetária de compensação
        """
        
        
        # Extrair informações do contrato da descrição ou usar padrão
        descricao = compensacao.get('description', '')
        # calcular diferença entre valores
        diferencaValor = compensacao.get('proposed_value', 0) - compensacao.get('current_value', 0)
        
        
        return {
            'tipo': 'RETIFICACAO',
            'subTipo': 'COMPENSACAO',
            "contrato" : cco_original.get('contratoCpp', ''),
            "campo" : cco_original.get('campo', ''),
            'dataCorrecao': datetime.now(timezone.utc).isoformat(),
            'dataCriacaoCorrecao': datetime.now(timezone.utc),
            "valorReconhecido" : cco_original.get('valorReconhecido', Decimal128("0")),
            'valorReconhecidoComOH': Decimal128(str(compensacao.get('proposed_value', 0))),
            "overHeadExploracao" : cco_original.get('overHeadExploracao', Decimal128("0")),
            "overHeadProducao" : cco_original.get('overHeadProducao', Decimal128("0")),
            "overHeadTotal" : cco_original.get('overHeadTotal', Decimal128("0")),
            "diferencaValor" : Decimal128(str(diferencaValor)),
            'valorReconhecidoComOhOriginal': Decimal128(str(compensacao.get('current_value', 0))),
            "faseRemessa" : cco_original.get('faseRemessa', ''),
            'taxaCorrecao': Decimal128(str(compensacao.get('taxa_aplicada', 1.0))),
            "ativo" : True,
            "quantidadeLancamento" : cco_original.get('quantidadeLancamento', 0),
            "valorLancamentoTotal" : cco_original.get('valorLancamentoTotal', Decimal128("0")),
            "valorNaoPassivelRecuperacao" : cco_original.get('valorNaoPassivelRecuperacao', Decimal128("0")),
            "valorReconhecivel" : cco_original.get('valorReconhecivel', Decimal128("0")),
            "valorNaoReconhecido" : cco_original.get('valorNaoReconhecido', Decimal128("0")),
            "valorReconhecidoExploracao" : cco_original.get('valorReconhecidoExploracao', Decimal128("0")),
            "valorReconhecidoProducao" : cco_original.get('valorReconhecidoProducao', Decimal128("0")),
            "igpmAcumulado" : Decimal128("0"),
            "igpmAcumuladoReais" : Decimal128("0"),
            'observacoes': f"{descricao} - Aplicado em {datetime.now().strftime('%d/%m/%Y')}",
            "transferencia" : False
        }
        
    def _criar_correcao_ajuste_duplicata(self, cco_original: Dict[str, Any],  ultima_correcao: Dict[str, Any],  valor_total_removido: Decimal128, resumo_correcoes_removidas: Dict[str, Any], correcoes_ajustes_duplicatas: Dict[str, Any]) -> Dict[str, Any]:  
        # calcular diferença entre valores
        if valor_total_removido < 0:
            novo_valor = self._converter_decimal128_para_float(ultima_correcao.get('valorReconhecidoComOH', 0)) + float(valor_total_removido)
        else:
            novo_valor = self._converter_decimal128_para_float(ultima_correcao.get('valorReconhecidoComOH', 0)) - float(valor_total_removido)
        
        
        # itera no resumo_correcoes_removidas, e criar um texto formatado com as informçaões, conforme estrutura seguir {'target_period': "01/2022",'target_date': "2025-09-24T14:23:44.676287+00:00",'current_value': "222.0"}
        
        detalhes_correcoes_removidas = ''
        for resumo in resumo_correcoes_removidas:
            detalhes_correcoes_removidas += f"periodo {resumo.get('target_period', '')}, data {resumo.get('target_date', '')}, valor {resumo.get('current_value', '')}; "
                  
            
        # iterar nas correções de ajuste de duplicata e consolidar as descrições (description)
        descricao = ''
        for correcao in correcoes_ajustes_duplicatas:
            descricao += f"{correcao.get('description', '')}; "
            
        retificacao = {
            "tipo": "RETIFICACAO",
            "subTipo": "COMPENSACAO",
            "contrato": cco_original.get('contratoCpp', ''),
            "campo": cco_original.get('campo', ''),
            "dataCorrecao": datetime.now(),
            "dataCriacaoCorrecao": datetime.now(),
            "valorReconhecido": ultima_correcao['valorReconhecido'],
            "valorReconhecidoComOH": Decimal128(str(round(novo_valor, 15))),
            "overHeadExploracao": ultima_correcao['overHeadExploracao'],
            "overHeadProducao": ultima_correcao['overHeadProducao'],
            "overHeadTotal": ultima_correcao['overHeadTotal'],
            "diferencaValor": Decimal128(str(round(valor_total_removido, 15))),
            "valorReconhecidoComOhOriginal": ultima_correcao.get('valorReconhecidoComOH', 0),
            "valorRecuperado": Decimal128("0"),
            "valorRecuperadoTotal": ultima_correcao.get('valorRecuperadoTotal', Decimal128("0")),
            "faseRemessa": cco_original.get('faseRemessa', ''),
            "ativo": True,
            "quantidadeLancamento": cco_original.get('quantidadeLancamento', 0),
            "valorLancamentoTotal": ultima_correcao['valorLancamentoTotal'],
            "valorNaoPassivelRecuperacao": ultima_correcao['valorNaoPassivelRecuperacao'],
            "valorReconhecivel": ultima_correcao['valorReconhecivel'],
            "valorNaoReconhecido": ultima_correcao['valorNaoReconhecido'],
            "valorReconhecidoExploracao": ultima_correcao['valorReconhecidoExploracao'],
            "valorReconhecidoProducao": ultima_correcao['valorReconhecidoProducao'],
            "igpmAcumulado": ultima_correcao['igpmAcumulado'],
            "igpmAcumuladoReais": ultima_correcao['igpmAcumuladoReais'],
            "observacao": f"Retificacao - Ajuste de valor devido a remoção de duplicidades de correção de IPCA/IGPM, referente aos periodos [{detalhes_correcoes_removidas}], removendo o valor total de {valor_total_removido}. Detalhes: {descricao}",
            "transferencia": False
        }
        
        return retificacao
    
    def _validar_sequencia_temporal(self, correcoes: List[Dict[str, Any]], 
                                  validacao: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida se as correções estão em sequência temporal correta
        """
        try:
            correcoes_com_data = [
                c for c in correcoes 
                if c.get('data_correcao') and c['tipo'] != 'REACTIVATION'
            ]
            
            # Ordenar por data
            correcoes_ordenadas = sorted(
                correcoes_com_data, 
                key=lambda x: x['data_correcao']
            )
            
            # Verificar se há sobreposições problemáticas
            for i in range(len(correcoes_ordenadas) - 1):
                atual = correcoes_ordenadas[i]
                proxima = correcoes_ordenadas[i + 1]
                
                if atual['data_correcao'] >= proxima['data_correcao']:
                    validacao['warnings'].append(
                        f"Sobreposição temporal entre correções: {atual['descricao']} e {proxima['descricao']}"
                    )
            
            return validacao
            
        except Exception as e:
            logger.error(f"Erro na validação temporal: {e}")
            validacao['errors'].append(f"Erro na validação temporal: {str(e)}")
            return validacao
    
    def _validar_valores_positivos(self, correcoes: List[Dict[str, Any]], 
                                 validacao: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida se os valores calculados são positivos e consistentes
        """
        try:
            for correcao in correcoes:
                if correcao['tipo'] != 'REACTIVATION':
                    valor_corrigido = correcao.get('valor_corrigido', 0)
                    
                    if valor_corrigido < 0:
                        validacao['errors'].append(
                            f"Valor corrigido negativo na correção: {correcao['descricao']}"
                        )
                        validacao['valido'] = False
                    
                    if valor_corrigido == 0:
                        validacao['warnings'].append(
                            f"Valor corrigido zerado na correção: {correcao['descricao']}"
                        )
            
            return validacao
            
        except Exception as e:
            logger.error(f"Erro na validação de valores: {e}")
            validacao['errors'].append(f"Erro na validação de valores: {str(e)}")
            return validacao
    
    def _validar_taxas_aplicadas(self, correcoes: List[Dict[str, Any]], 
                               validacao: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida se as taxas aplicadas são razoáveis
        """
        try:
            for correcao in correcoes:
                taxa = correcao.get('taxa_aplicada', 1.0)
                
                # Taxa deve estar entre 0.5 e 2.0 (entre -50% e +100%)
                if taxa < 0.5 or taxa > 2.0:
                    validacao['warnings'].append(
                        f"Taxa aplicada fora do intervalo esperado ({taxa:.4f}) na correção: {correcao['descricao']}"
                    )
                
                # Taxa exatamente 1.0 pode indicar problema
                if taxa == 1.0 and correcao['tipo'] in ['IPCA_ADDITION', 'IPCA_UPDATE']:
                    validacao['warnings'].append(
                        f"Taxa aplicada igual a 1.0 (sem correção) na correção: {correcao['descricao']}"
                    )
            
            return validacao
            
        except Exception as e:
            logger.error(f"Erro na validação de taxas: {e}")
            validacao['errors'].append(f"Erro na validação de taxas: {str(e)}")
            return validacao
        
    def aplicar_ipca_ano_vigente(self, cco_id: str, proposta: Dict[str, Any]) -> Dict[str, Any]:
        """Aplica correção IPCA do ano vigente"""
        try:
            cco_original = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco_original:
                return {'success': False, 'error': 'CCO não encontrada'}
            
            # Criar nova correção IPCA
            nova_correcao = {
                "tipo": "IPCA",
                "subTipo": "VIGENTE",
                "contrato": cco_original.get('contratoCpp', ''),
                "campo": cco_original.get('campo', ''),
                "dataCorrecao": datetime.now(timezone.utc).isoformat(),
                "dataCriacaoCorrecao": datetime.now(timezone.utc),
                "valorReconhecidoComOH": Decimal128(str(proposta['valor_proposto'])),
                "valorReconhecidoComOhOriginal": Decimal128(str(proposta['valor_atual'])),
                "diferencaValor": Decimal128(str(proposta['impacto'])),
                "taxaCorrecao": Decimal128(str(proposta['taxa_aplicada'])),
                "ativo": True,
                "observacao": f"Correção IPCA ano vigente - {proposta['observacao']}",
                "transferencia": False
            }
            
            # Adicionar correção à CCO
            correcoes_atualizadas = cco_original['correcoesMonetarias'].copy()
            correcoes_atualizadas.append(nova_correcao)
            
            # Salvar na coleção corrigida
            cco_corrigida = cco_original.copy()
            cco_corrigida['_id'] = cco_id + '_ipca_vigente'
            cco_corrigida['correcoesMonetarias'] = correcoes_atualizadas
            
            self.db.conta_custo_oleo_corrigida_entity.replace_one(
                {'_id': cco_corrigida['_id']},
                cco_corrigida,
                upsert=True
            )
            
            return {
                'success': True,
                'valor_anterior': proposta['valor_atual'],
                'valor_final': proposta['valor_proposto'],
                'impacto': proposta['impacto'],
                'periodo_aplicado': proposta['periodo_aplicacao']
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}