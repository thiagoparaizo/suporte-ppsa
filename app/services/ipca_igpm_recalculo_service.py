"""
Serviço de Recálculo IPCA/IGPM
Implementa recálculo de correções monetárias por IPCA/IGPM baseado na regra de aniversário.
"""

import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from bson import ObjectId, Decimal128
from bson.int64 import Int64
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from copy import deepcopy

logger = logging.getLogger(__name__)

class TipoRecalculo:
    IPCA_IGPM = 'IPCA_IGPM'

class ModoRecalculo:
    CORRECAO_SIMPLES = 'CORRECAO_SIMPLES'  # Apenas adiciona a correção no ponto correto
    RECALCULO_COMPLETO = 'RECALCULO_COMPLETO'  # Recalcula tudo após a inserção

class IPCAIGPMRecalculoService:
    """
    Serviço para recálculo de correções monetárias IPCA/IGPM
    """
    
    def __init__(self, db_connection, db_connection_prd=None):
        """
        Inicializa o serviço de recálculo IPCA/IGPM
        
        Args:
            db_local_connection: Conexão com MongoDB local (temporário)
            db_connection_prd: Conexão com MongoDB principal
            
        """
        self.db_local = db_connection
        self.db_prd = db_connection_prd
        
        
    # def identificar_gaps_ipca_igpm(self, filtros: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    #     """
    #     Identifica CCOs com gaps de correção IPCA/IGPM baseado na regra de aniversário
        
    #     Args:
    #         filtros: Filtros para busca de CCOs (contrato, campo, etc.)
            
    #     Returns:
    #         Lista de CCOs com gaps identificados
    #     """
    #     try:
    #         # Construir query base
    #         query = {'flgRecuperado': False}  # Apenas CCOs não recuperadas
    #         if filtros:
    #             query.update(filtros)
            
    #         ccos_com_gaps = []
    #         data_atual = datetime.now(timezone.utc)
            
    #         # Buscar CCOs
    #         cursor = self.db.conta_custo_oleo_entity.find(query)
            
    #         for cco in cursor:
    #             gaps = self._analisar_gaps_cco(cco, data_atual)
    #             if gaps:
    #                 ccos_com_gaps.append({
    #                     '_id': cco['_id'],
    #                     'contratoCpp': cco.get('contratoCpp'),
    #                     'campo': cco.get('campo'),
    #                     'remessa': cco.get('remessa'),
    #                     'dataReconhecimento': cco.get('dataReconhecimento'),
    #                     'valorAtual': self._obter_valor_atual_cco(cco),
    #                     'gaps': gaps
    #                 })
            
    #         return ccos_com_gaps
            
    #     except Exception as e:
    #         logger.error(f"Erro ao identificar gaps IPCA/IGPM: {e}")
    #         return []
    
    def _analisar_gaps_cco(self, cco: Dict[str, Any], data_atual: datetime) -> List[Dict[str, Any]]:
        """
        Analisa uma CCO específica para identificar gaps de correção
        
        Args:
            cco: Documento da CCO
            data_atual: Data atual para comparação
            
        Returns:
            Lista de gaps encontrados
        """
        gaps = []
        
        # Obter data de reconhecimento
        data_reconhecimento = cco.get('dataReconhecimento')
        if not data_reconhecimento:
            return gaps
        
        if isinstance(data_reconhecimento, str):
            data_reconhecimento = datetime.fromisoformat(data_reconhecimento.replace('Z', '+00:00'))
        
        # Obter correções existentes
        correcoes_existentes = cco.get('correcoesMonetarias', [])
        datas_ipca_igpm = set()
        
        for correcao in correcoes_existentes:
            if correcao.get('tipo') in ['IPCA', 'IGPM']:
                data_correcao = correcao.get('dataCorrecao')
                if data_correcao:
                    if isinstance(data_correcao, str):
                        data_correcao = datetime.fromisoformat(data_correcao.replace('Z', '+00:00'))
                    datas_ipca_igpm.add((data_correcao.year, data_correcao.month))
        
        # Calcular datas de aniversário esperadas (mês e ano específicos)
        mes_reconhecimento = data_reconhecimento.month
        ano_reconhecimento = data_reconhecimento.year
        
        # Primeiro aniversário: mesmo mês do ano seguinte
        ano_aniversario = ano_reconhecimento + 1
        
        while ano_aniversario <= data_atual.year:
            # Se for o ano atual, verificar se o mês já passou
            if ano_aniversario == data_atual.year and mes_reconhecimento > data_atual.month:
                break
                
            chave_data = (ano_aniversario, mes_reconhecimento)
            
            if chave_data not in datas_ipca_igpm:
                # Gap encontrado!
                gaps.append({
                    'ano': data_aniversario.year,
                    'mes': data_aniversario.month,
                    'data_esperada': data_aniversario,
                    'tipo_sugerido': 'IPCA'  # Pode ser configurável
                })
            
            data_aniversario += relativedelta(years=1)
        
        return gaps
    
    def executar_recalculo_ipca_igpm(self, cco_id: str, ano: int, mes: int, 
                                    taxa_correcao, tipo: str = 'IPCA',
                                    modo: ModoRecalculo = ModoRecalculo.CORRECAO_SIMPLES,
                                    observacoes: str = "") -> Dict[str, Any]:
        """
        Executa recálculo de IPCA/IGPM para uma CCO específica
        
        Args:
            cco_id: ID da CCO
            ano: Ano da correção a ser aplicada
            mes: Mês da correção a ser aplicada
            taxa_correcao: Taxa de correção (ex: 1.0424 para 4.24%) ou 'auto' para usar histórica
            tipo: Tipo de correção ('IPCA' ou 'IGPM')
            modo: Modo de recálculo
            observacoes: Observações do recálculo
            
        Returns:
            Resultado do recálculo
        """
        try:
            # Buscar CCO original
            cco_original = self.db.conta_custo_oleo_entity.find_one({'_id': cco_id})
            if not cco_original:
                return {'success': False, 'error': f'CCO {cco_id} não encontrada'}
            
            # Validar se CCO não está recuperada
            if cco_original.get('flgRecuperado', False):
                return {'success': False, 'error': 'CCO já está totalmente recuperada'}
            
            # Determinar taxa a aplicar
            if taxa_correcao == 'auto' or taxa_correcao is None:
                taxa_final = self._obter_taxa_historica(ano, mes, tipo)
                observacoes_final = f"{observacoes} - Taxa histórica {tipo} {mes:02d}/{ano}: {(taxa_final-1)*100:.4f}%"
            else:
                taxa_final = float(taxa_correcao)
                observacoes_final = observacoes
            
            # Obter valor base para correção
            valor_base = self._obter_valor_base_correcao(cco_original, ano, mes)
            if valor_base <= 0:
                return {'success': False, 'error': 'Valor base para correção é zero ou negativo'}
            
            # Calcular valores da correção
            taxa_decimal = Decimal(str(taxa_final))
            diferenca_correcao = float(valor_base * (taxa_decimal - 1))
            novo_valor_com_oh = valor_base + diferenca_correcao
            
            # Criar CCO recalculada
            cco_recalculada = deepcopy(cco_original)
            
            # Criar correção IPCA/IGPM
            data_correcao = datetime(ano, mes, 16, tzinfo=timezone.utc)  # Dia 16 como padrão
            correcao_ipca_igpm = self._criar_correcao_ipca_igpm(
                cco_original, valor_base, taxa_decimal, diferenca_correcao, 
                data_correcao, tipo
            )
            
            # Inserir correção no ponto correto da timeline
            if 'correcoesMonetarias' not in cco_recalculada:
                cco_recalculada['correcoesMonetarias'] = []
            
            posicao_insercao = self._encontrar_posicao_insercao(
                cco_recalculada['correcoesMonetarias'], data_correcao
            )
            
            # Inserir correção IPCA/IGPM
            cco_recalculada['correcoesMonetarias'].insert(posicao_insercao, correcao_ipca_igpm)
            
            # Criar correção de retificação para compensação
            correcao_retificacao = self._criar_correcao_retificacao_compensacao(
                cco_original, diferenca_correcao, taxa_decimal, tipo, observacoes_final
            )
            cco_recalculada['correcoesMonetarias'].append(correcao_retificacao)
            
            # Atualizar flag de recuperação se necessário
            valor_final = self._obter_valor_atual_cco(cco_recalculada)
            if cco_original.get('flgRecuperado', False) and valor_final > 0:
                cco_recalculada['flgRecuperado'] = False
            
            # Preparar metadata do recálculo
            metadata_recalculo = {
                'tipo_recalculo': TipoRecalculo.IPCA_IGPM,
                'modo_recalculo': modo,
                'ano_correcao': ano,
                'mes_correcao': mes,
                'taxa_correcao': float(taxa_decimal),
                'taxa_percentual': f"{(taxa_final-1)*100:.4f}%",
                'tipo_indice': tipo,
                'valor_base': valor_base,
                'diferenca_correcao': diferenca_correcao,
                'observacoes': observacoes_final,
                'data_recalculo': datetime.now(),
                'usuario': 'system',
                'fonte_taxa': 'historica' if taxa_correcao == 'auto' else 'manual'
            }
            
            cco_recalculada['metadata_recalculo'] = metadata_recalculo
            
            # Preparar resultado comparativo
            resultado = self._preparar_resultado_comparativo(
                cco_original, cco_recalculada, metadata_recalculo
            )
            
            return {
                'success': True,
                'resultado': resultado
            }
            
        except Exception as e:
            logger.error(f"Erro ao executar recálculo IPCA/IGPM: {e}")
            return {'success': False, 'error': str(e)}
    
    def _obter_taxa_historica(self, ano: int, mes: int, tipo: str) -> float:
        """
        Obtém taxa histórica das coleções ipca_entity ou igpm_entity
        """
        try:
            if tipo == 'IPCA':
                colecao = self.db.ipca_entity
            elif tipo == 'IGPM':
                colecao = self.db.igpm_entity
            else:
                logger.warning(f"Tipo de índice não reconhecido: {tipo}. Usando IPCA.")
                colecao = self.db.ipca_entity
            
            # Buscar taxa na coleção
            documento = colecao.find_one({
                'anoReferencia': ano,
                'mesReferencia': mes
            })
            
            if documento:
                valor_percentual = self._converter_decimal128_para_float(documento['valor'])
                # Converter de percentual para fator (ex: 4.47% -> 1.0447)
                taxa_fator = 1 + (valor_percentual / 100)
                
                logger.info(f"Taxa {tipo} encontrada para {mes:02d}/{ano}: {valor_percentual}% (fator: {taxa_fator})")
                return taxa_fator
            else:
                logger.warning(f"Taxa {tipo} não encontrada para {mes:02d}/{ano}. Usando taxa padrão.")
                return None  # 4% como fallback
                
        except Exception as e:
            logger.error(f"Erro ao buscar taxa {tipo} para {mes:02d}/{ano}: {e}")
            return None  # 4% como fallback
    
    def consultar_taxa_disponivel(self, ano: int, mes: int, tipo: str) -> Dict[str, Any]:
        """
        Consulta taxa disponível na base de dados com informações detalhadas
        """
        try:
            if tipo == 'IPCA':
                colecao = self.db.ipca_entity
            elif tipo == 'IGPM':
                colecao = self.db.igpm_entity
            else:
                return {'success': False, 'error': f'Tipo de índice não reconhecido: {tipo}'}
            
            documento = colecao.find_one({
                'anoReferencia': ano,
                'mesReferencia': mes
            })
            
            if documento:
                valor_percentual = self._converter_decimal128_para_float(documento['valor'])
                taxa_fator = 1 + (valor_percentual / 100)
                
                return {
                    'success': True,
                    'encontrada': True,
                    'tipo': tipo,
                    'ano': ano,
                    'mes': mes,
                    'valor_percentual': valor_percentual,
                    'taxa_fator': taxa_fator,
                    'documento_id': str(documento['_id']),
                    'version': documento.get('version'),
                    'formatado': f"{valor_percentual:.4f}%"
                }
            else:
                return {'success': False, 'error': str(e)}
                
        except Exception as e:
            logger.error(f"Erro ao consultar taxa {tipo} para {mes:02d}/{ano}: {e}")
            return {'success': False, 'error': str(e)}
    
    def listar_taxas_disponiveis(self, tipo: str, ano_inicio: int = None, ano_fim: int = None) -> Dict[str, Any]:
        """
        Lista todas as taxas disponíveis para um tipo específico
        """
        try:
            if tipo == 'IPCA':
                colecao = self.db.ipca_entity
            elif tipo == 'IGPM':
                colecao = self.db.igpm_entity
            else:
                return {'success': False, 'error': f'Tipo de índice não reconhecido: {tipo}'}
            
            # Construir filtro
            filtro = {}
            if ano_inicio and ano_fim:
                filtro['anoReferencia'] = {'$gte': ano_inicio, '$lte': ano_fim}
            elif ano_inicio:
                filtro['anoReferencia'] = {'$gte': ano_inicio}
            elif ano_fim:
                filtro['anoReferencia'] = {'$lte': ano_fim}
            
            # Buscar documentos ordenados
            cursor = colecao.find(filtro).sort([('anoReferencia', 1), ('mesReferencia', 1)])
            
            taxas = []
            for doc in cursor:
                valor_percentual = self._converter_decimal128_para_float(doc['valor'])
                taxa_fator = 1 + (valor_percentual / 100)
                
                taxas.append({
                    'ano': doc['anoReferencia'],
                    'mes': doc['mesReferencia'],
                    'valor_percentual': valor_percentual,
                    'taxa_fator': taxa_fator,
                    'periodo': f"{doc['mesReferencia']}/{doc['anoReferencia']}",
                    'formatado': f"{valor_percentual:.4f}%",
                    'documento_id': str(doc['_id'])
                })
            
            return {
                'success': True,
                'tipo': tipo,
                'total_registros': len(taxas),
                'periodo_inicio': f"{taxas[0]['periodo']}" if taxas else None,
                'periodo_fim': f"{taxas[-1]['periodo']}" if taxas else None,
                'taxas': taxas
            }
            
        except Exception as e:
            logger.error(f"Erro ao listar taxas {tipo}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _obter_valor_base_correcao(self, cco: Dict[str, Any], ano_correcao: int, mes_correcao: int) -> float:
        """
        Obtém o valor base para aplicação da correção IPCA/IGPM
        """
        data_correcao = datetime(ano_correcao, mes_correcao, 1, tzinfo=timezone.utc)
        
        # Se não há correções, usar valor da raiz
        correcoes = cco.get('correcoesMonetarias', [])
        if not correcoes:
            return self._converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0))
        
        # Encontrar a correção imediatamente anterior à data de correção
        valor_base = self._converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0))
        
        for correcao in correcoes:
            data_corr = self._extrair_data_correcao(correcao)
            if data_corr and data_corr < data_correcao:
                valor_base = self._converter_decimal128_para_float(
                    correcao.get('valorReconhecidoComOH', 0)
                )
        
        return valor_base
    
    def _criar_correcao_ipca_igpm(self, cco_original: Dict[str, Any], valor_base: float, 
                                 taxa_correcao: Decimal, diferenca_correcao: float,
                                 data_correcao: datetime, tipo: str) -> Dict[str, Any]:
        """
        Cria correção monetária do tipo IPCA/IGPM
        """
        # Obter valores base da CCO ou última correção
        valores_base = self._extrair_valores_base_cco(cco_original)
        
        if hasattr(data_correcao, 'isoformat'):
            data_correcao_str = data_correcao.isoformat()
        else:
            data_correcao_str = str(data_correcao)
            
        correcao = {
            "tipo": tipo,
            "subTipo": "DEFAULT",
            "contrato": cco_original.get('contratoCpp', ''),
            "campo": cco_original.get('campo', ''),
            "dataCorrecao": data_correcao_str,
            "dataCriacaoCorrecao": datetime.now(),
            "valorReconhecido": valores_base['valorReconhecido'],
            "valorReconhecidoComOH": Decimal128(str(round(valor_base + diferenca_correcao, 15))),
            "overHeadExploracao": valores_base['overHeadExploracao'],
            "overHeadProducao": valores_base['overHeadProducao'],
            "overHeadTotal": valores_base['overHeadTotal'],
            "diferencaValor": Decimal128(str(round(diferenca_correcao, 15))),
            "valorReconhecidoComOhOriginal": Decimal128(str(round(valor_base, 15))),
            "valorRecuperado": Decimal128("0"),
            "valorRecuperadoTotal": valores_base.get('valorRecuperadoTotal', Decimal128("0")),
            "faseRemessa": cco_original.get('faseRemessa', ''),
            "taxaCorrecao": Decimal128(str(taxa_correcao)),
            "ativo": True,
            "quantidadeLancamento": cco_original.get('quantidadeLancamento', 0),
            "valorLancamentoTotal": valores_base['valorLancamentoTotal'],
            "valorNaoPassivelRecuperacao": valores_base['valorNaoPassivelRecuperacao'],
            "valorReconhecivel": valores_base['valorReconhecivel'],
            "valorNaoReconhecido": valores_base['valorNaoReconhecido'],
            "valorReconhecidoExploracao": valores_base['valorReconhecidoExploracao'],
            "valorReconhecidoProducao": valores_base['valorReconhecidoProducao'],
            "igpmAcumulado": taxa_correcao,
            "igpmAcumuladoReais": Decimal128(str(round(diferenca_correcao, 15))),
            "transferencia": False
        }
        
        return correcao
    
    def _criar_correcao_retificacao_compensacao(self, cco_original: Dict[str, Any], 
                                              diferenca_correcao: float, taxa_correcao: Decimal,
                                              tipo_indice: str, observacoes: str) -> Dict[str, Any]:
        """
        Cria correção de retificação para compensar a inclusão da correção IPCA/IGPM
        """
        # Obter valor atual (última correção ou raiz)
        valor_atual = self._obter_valor_atual_cco(cco_original)
        valores_base = self._extrair_valores_base_cco(cco_original)
        
        # Calcular novo valor com a compensação
        novo_valor = valor_atual + diferenca_correcao
        
        retificacao = {
            "tipo": "RETIFICACAO",
            "subTipo": "DEFAULT",
            "contrato": cco_original.get('contratoCpp', ''),
            "campo": cco_original.get('campo', ''),
            "dataCorrecao": datetime.now().isoformat(),
            "dataCriacaoCorrecao": datetime.now(),
            "valorReconhecido": valores_base['valorReconhecido'],
            "valorReconhecidoComOH": Decimal128(str(round(novo_valor, 15))),
            "overHeadExploracao": valores_base['overHeadExploracao'],
            "overHeadProducao": valores_base['overHeadProducao'],
            "overHeadTotal": valores_base['overHeadTotal'],
            "diferencaValor": Decimal128(str(round(diferenca_correcao, 15))),
            "valorReconhecidoComOhOriginal": Decimal128(str(round(valor_atual, 15))),
            "valorRecuperado": Decimal128("0"),
            "valorRecuperadoTotal": valores_base.get('valorRecuperadoTotal', Decimal128("0")),
            "faseRemessa": cco_original.get('faseRemessa', ''),
            "ativo": True,
            "quantidadeLancamento": cco_original.get('quantidadeLancamento', 0),
            "valorLancamentoTotal": valores_base['valorLancamentoTotal'],
            "valorNaoPassivelRecuperacao": valores_base['valorNaoPassivelRecuperacao'],
            "valorReconhecivel": valores_base['valorReconhecivel'],
            "valorNaoReconhecido": valores_base['valorNaoReconhecido'],
            "valorReconhecidoExploracao": valores_base['valorReconhecidoExploracao'],
            "valorReconhecidoProducao": valores_base['valorReconhecidoProducao'],
            "igpmAcumulado": taxa_correcao,
            "igpmAcumuladoReais": Decimal128(str(round(diferenca_correcao, 15))),
            "observacao": f"Retificacao - Inclusao de {tipo_indice} referente a {observacoes}, taxa {taxa_correcao}, valor diferenca {diferenca_correcao}.",
            "transferencia": False
        }
        
        return retificacao
    
    
    
    def _encontrar_posicao_insercao(self, correcoes: List[Dict[str, Any]], 
                                   data_nova_correcao: datetime) -> int:
        """
        Encontra a posição correta para inserir a nova correção na timeline
        """
        for i, correcao in enumerate(correcoes):
            data_corr = self._extrair_data_correcao(correcao)
            if data_corr and data_corr > data_nova_correcao:
                return i
        
        return len(correcoes)  # Inserir no final se for a mais recente
    
    def _extrair_data_correcao(self, correcao: Dict[str, Any]) -> Optional[datetime]:
        """
        Extrai e converte data de correção para datetime
        """
        data_correcao = correcao.get('dataCorrecao') or correcao.get('dataCriacaoCorrecao')
        
        if not data_correcao:
            return None
        
        try:
            if isinstance(data_correcao, str):
                # Remover 'Z' e adicionar timezone UTC
                data_str = data_correcao.replace('Z', '+00:00')
                return datetime.fromisoformat(data_str)
            elif hasattr(data_correcao, 'replace'):
                # Já é datetime, garantir timezone
                if data_correcao.tzinfo is None:
                    return data_correcao.replace(tzinfo=timezone.utc)
                return data_correcao
            else:
                return None
        except Exception as e:
            logger.warning(f"Erro ao extrair data de correção: {e}")
            return None
    
    def _obter_valor_atual_cco(self, cco: Dict[str, Any]) -> float:
        """
        Obtém o valor atual da CCO (última correção ou raiz)
        """
        correcoes = cco.get('correcoesMonetarias', [])
        if correcoes:
            return self._converter_decimal128_para_float(
                correcoes[-1].get('valorReconhecidoComOH', 0)
            )
        
        return self._converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0))
    
    def _extrair_valores_base_cco(self, cco: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extrai valores base da CCO (da raiz, não das correções)
        """
        return {
            'valorReconhecido': cco.get('valorReconhecido', Decimal128("0")),
            'overHeadExploracao': cco.get('overHeadExploracao', Decimal128("0")),
            'overHeadProducao': cco.get('overHeadProducao', Decimal128("0")),
            'overHeadTotal': cco.get('overHeadTotal', Decimal128("0")),
            'valorLancamentoTotal': cco.get('valorLancamentoTotal', Decimal128("0")),
            'valorNaoPassivelRecuperacao': cco.get('valorNaoPassivelRecuperacao', Decimal128("0")),
            'valorReconhecivel': cco.get('valorReconhecivel', Decimal128("0")),
            'valorNaoReconhecido': cco.get('valorNaoReconhecido', Decimal128("0")),
            'valorReconhecidoExploracao': cco.get('valorReconhecidoExploracao', Decimal128("0")),
            'valorReconhecidoProducao': cco.get('valorReconhecidoProducao', Decimal128("0")),
            'valorRecuperadoTotal': self._obter_valor_recuperado_total(cco)
        }
    
    def _obter_valor_recuperado_total(self, cco: Dict[str, Any]) -> Decimal128:
        """
        Obtém valor total recuperado da CCO
        """
        correcoes = cco.get('correcoesMonetarias', [])
        if not correcoes:
            return Decimal128("0")
        
        # Buscar na última correção de recuperação
        for correcao in reversed(correcoes):
            if correcao.get('tipo') == 'RECUPERACAO':
                return correcao.get('valorRecuperadoTotal', Decimal128("0"))
        
        return Decimal128("0")
    
    def _converter_decimal128_para_float(self, valor) -> float:
        """
        Converte Decimal128 para float de forma segura
        """
        if valor is None:
            return 0.0
        if isinstance(valor, Decimal128):
            return float(valor.to_decimal())
        return float(valor)
    
    def _preparar_resultado_comparativo(self, cco_original: Dict[str, Any], 
                                       cco_recalculada: Dict[str, Any],
                                       metadata_recalculo: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara resultado comparativo do recálculo
        """
        valor_original = self._obter_valor_atual_cco(cco_original)
        valor_recalculado = self._obter_valor_atual_cco(cco_recalculada)
        diferenca = valor_recalculado - valor_original
        
        return {
            'cco_original': cco_original,
            'cco_recalculada': cco_recalculada,
            'metadata_recalculo': metadata_recalculo,
            'comparativo': {
                'valorReconhecidoComOH': {
                    'valor_original': valor_original,
                    'valor_recalculado': valor_recalculado,
                    'diferenca': diferenca,
                    'percentual_variacao': (diferenca / valor_original * 100) if valor_original != 0 else 0
                }
            },
            'resumo': {
                'tipo_recalculo': 'IPCA/IGPM',
                'valor_correcao_aplicada': metadata_recalculo['diferenca_correcao'],
                'taxa_aplicada': metadata_recalculo['taxa_correcao'],
                'ano_mes_correcao': f"{metadata_recalculo['mes_correcao']}/{metadata_recalculo['ano_correcao']}"
            }
        }