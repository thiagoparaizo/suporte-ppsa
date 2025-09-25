"""
Analisador de Gaps IPCA/IGPM
Utilitário para identificar CCOs que precisam de correção monetária baseado na regra de aniversário.
"""

import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from bson import ObjectId
from typing import Dict, Any, List, Optional, Tuple
import csv
import json

logger = logging.getLogger(__name__)

class IPCAGapAnalyzer:
    """
    Analisador de gaps de correção IPCA/IGPM
    """
    
    def __init__(self, db_connection):
        """
        Inicializa o analisador
        
        Args:
            db_connection: Conexão com MongoDB
        """
        self.db = db_connection
        
        # Parâmetro configurável para ajuste do mês da taxa
        # 0 = mesmo mês, -1 = mês anterior, +1 = mês posterior
        self.OFFSET_MES_TAXA_APLICACAO = -1  # Atualmente: mês anterior ao aniversário
    
     
    def _calcular_mes_taxa_aplicacao(self, ano_aniversario: int, mes_aniversario: int) -> tuple:
        """
        Calcula o ano e mês da taxa a ser aplicada baseado no offset (OFFSET_MES_TAXA_APLICACAO) configurado
        
        Args:
            ano_aniversario: Ano do aniversário
            mes_aniversario: Mês do aniversário
            
        Returns:
            Tuple (ano_taxa, mes_taxa)
        """
        mes_taxa = mes_aniversario + self.OFFSET_MES_TAXA_APLICACAO
        ano_taxa = ano_aniversario
        
        # Ajustar ano se o mês sair dos limites
        if mes_taxa <= 0:
            # Mês negativo ou zero - volta para dezembro do ano anterior
            mes_taxa = 12 + mes_taxa  # ex: mes_taxa = -1 -> 12 + (-1) = 11 (novembro)
            ano_taxa = ano_aniversario - 1
        elif mes_taxa > 12:
            # Mês maior que 12 - vai para janeiro do próximo ano
            mes_taxa = mes_taxa - 12  # ex: mes_taxa = 13 -> 13 - 12 = 1 (janeiro)
            ano_taxa = ano_aniversario + 1
        
        return ano_taxa, mes_taxa
        
    def analisar_gaps_sistema(self, filtros: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Analisa gaps de IPCA/IGPM em todo o sistema
        
        Args:
            filtros: Filtros opcionais (contrato, campo, período, etc.)
            
        Returns:
            Relatório completo de gaps
        """
        try:
            #Ordenação
            sort = [('contratoCpp', 1),  ('campo', 1),('dataReconhecimento', 1)]
            
            # Query base: CCOs ativas (não totalmente recuperadas)
            query = {
                # '$or': [
                #     {'flgRecuperado': False},
                #     {'flgRecuperado': {'$exists': False}}
                # ]
            }
            
            # Aplicar filtros específicos
            if filtros:
                if filtros.get('_id'):
                    query['_id'] = filtros['_id']
                if filtros.get('contratoCpp'):
                    query['contratoCpp'] = filtros['contratoCpp']
                if filtros.get('campo'):
                    query['campo'] = filtros['campo']
                if filtros.get('anoReconhecimento'):
                    query['anoReconhecimento'] = filtros['anoReconhecimento']
                if filtros.get('origemDosGastos'):
                    query['origemDosGastos'] = filtros['origemDosGastos']
            
            # Executar análise
            data_atual = datetime.now(timezone.utc)
            estatisticas = {
                'total_ccos_analisadas': 0,
                'ccos_com_gaps': 0,
                'total_gaps_identificados': 0,
                'ccos_com_correcoes_fora_periodo': 0,
                'total_correcoes_fora_periodo': 0,
                'gaps_por_ano': {},
                'gaps_por_contrato': {},
                'correcoes_fora_por_contrato': {},
                'valor_total_impactado': 0.0
            }
            
            ccos_com_gaps = []
            ccos_com_correcoes_fora = []
            ccos_com_duplicatas = []
            
            cursor = self.db.conta_custo_oleo_entity.find(query).sort(sort)
            
            for cco in cursor:
                estatisticas['total_ccos_analisadas'] += 1
                
                gaps, correcoes_fora, duplicatas = self._analisar_cco_individual(cco, data_atual)
                
                contrato = cco.get('contratoCpp', 'N/A')
                valor_atual = self._obter_valor_atual_cco(cco)
                
                # Processar gaps
                if gaps:
                    estatisticas['ccos_com_gaps'] += 1
                    estatisticas['total_gaps_identificados'] += len(gaps)
                    estatisticas['valor_total_impactado'] += valor_atual
                    
                    if contrato not in estatisticas['gaps_por_contrato']:
                        estatisticas['gaps_por_contrato'][contrato] = 0
                    estatisticas['gaps_por_contrato'][contrato] += len(gaps)
                    
                    for gap in gaps:
                        ano = gap['ano']
                        if ano not in estatisticas['gaps_por_ano']:
                            estatisticas['gaps_por_ano'][ano] = 0
                        estatisticas['gaps_por_ano'][ano] += 1
                    
                    # Adicionar à lista de CCOs com gaps
                    ccos_com_gaps.append({
                        '_id': str(cco['_id']),
                        'contratoCpp': cco.get('contratoCpp'),
                        'campo': cco.get('campo'),
                        'remessa': cco.get('remessa'),
                        'remessaExposicao': cco.get('remessaExposicao'),
                        'faseRemessa': cco.get('faseRemessa'),
                        'dataReconhecimento': cco.get('dataReconhecimento'),
                        'valorAtual': valor_atual,
                        'gaps': gaps,
                        'totalGaps': len(gaps)
                    })
                
                # Processar correções fora do período
                if correcoes_fora:
                    estatisticas['ccos_com_correcoes_fora_periodo'] += 1
                    estatisticas['total_correcoes_fora_periodo'] += len(correcoes_fora)
                    
                    if contrato not in estatisticas['correcoes_fora_por_contrato']:
                        estatisticas['correcoes_fora_por_contrato'][contrato] = 0
                    estatisticas['correcoes_fora_por_contrato'][contrato] += len(correcoes_fora)
                    
                    # Adicionar à lista de CCOs com correções fora do período
                    ccos_com_correcoes_fora.append({
                        '_id': str(cco['_id']),
                        'contratoCpp': cco.get('contratoCpp'),
                        'campo': cco.get('campo'),
                        'remessa': cco.get('remessa'),
                        'remessaExposicao': cco.get('remessaExposicao'),
                        'faseRemessa': cco.get('faseRemessa'),
                        'dataReconhecimento': cco.get('dataReconhecimento'),
                        'valorAtual': valor_atual,
                        'correcoes_fora_periodo': correcoes_fora,
                        'totalCorrecoesFora': len(correcoes_fora)
                    })
                    
                if duplicatas:
                    ccos_com_duplicatas.append({
                        '_id': str(cco['_id']),
                        'contratoCpp': cco.get('contratoCpp'),
                        'duplicatas': duplicatas,
                        'totalDuplicatas': len(duplicatas)
                    })
            estatisticas['ccos_com_duplicatas'] = len(ccos_com_duplicatas)
            estatisticas['total_duplicatas'] = sum(len(cco['duplicatas']) for cco in ccos_com_duplicatas)
                
            
            return {
                'data_analise': data_atual.isoformat(),
                'filtros_aplicados': filtros or {},
                'estatisticas': estatisticas,
                'ccos_com_gaps': ccos_com_gaps,
                'ccos_com_correcoes_fora_periodo': ccos_com_correcoes_fora,
                'ccos_com_duplicatas': ccos_com_duplicatas
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar gaps do sistema: {e}")
            return {'error': str(e)}
    
    # def _analisar_cco_individual(self, cco: Dict[str, Any], data_atual: datetime) -> List[Dict[str, Any]]:
    #     """
    #     Analisa uma CCO individual para identificar gaps
    #     """
    #     gaps = []
    #     correcoes_fora_do_periodo = []
        
    #     # Garantir que data_atual tenha timezone
    #     if data_atual.tzinfo is None:
    #         data_atual = data_atual.replace(tzinfo=timezone.utc)
        
    #     # Validar data de reconhecimento
    #     data_reconhecimento = self._extrair_data_reconhecimento(cco)
    #     if not data_reconhecimento:
    #         return gaps
        
    #     # Mapear correções IPCA/IGPM existentes
    #     correcoes_existentes = self._mapear_correcoes_ipca_igpm(cco)
        
    #     # Calcular primeiro aniversário: 1 mês após o reconhecimento
    #     mes_reconhecimento = data_reconhecimento.month
    #     ano_reconhecimento = data_reconhecimento.year
        
    #     # Primeiro aniversário: mês seguinte ao reconhecimento
    #     if mes_reconhecimento == 12:
    #         # Dezembro -> Janeiro do ano seguinte
    #         mes_aniversario = 1
    #         ano_aniversario = ano_reconhecimento + 2  # +2 porque vai para janeiro do segundo ano
    #     else:
    #         # Outros meses -> mês seguinte do ano seguinte
    #         mes_aniversario = mes_reconhecimento + 1
    #         ano_aniversario = ano_reconhecimento + 1
        
    #     logger.info(f"Analisando CCO {cco['_id']} - Reconhecimento: {mes_reconhecimento:02d}/{ano_reconhecimento}, Primeiro aniversário: {mes_aniversario:02d}/{ano_aniversario}")
        
    #     # Primeiro, mapear TODAS as correções por ano para melhor análise
    #     correcoes_por_ano = self._mapear_correcoes_por_ano(cco)
        
    #     while ano_aniversario <= data_atual.year:
    #         chave_periodo = (ano_aniversario, mes_aniversario)
    #         ano_taxa, mes_taxa = self._calcular_mes_taxa_aplicacao(ano_aniversario, mes_aniversario)
    #         logger.info(f"CCO {cco['_id']} - Aniversário: {mes_aniversario:02d}/{ano_aniversario}, Taxa período: {mes_taxa:02d}/{ano_taxa}")
            
    #         # Verificar data limite
    #         if ano_aniversario == data_atual.year and mes_aniversario >= data_atual.month:
    #             if mes_aniversario == data_atual.month and data_atual.day < 16:
    #                 break
    #             elif mes_aniversario > data_atual.month:
    #                 break
            
    #         # Verificar se já existe correção EXATA para este período
    #         if chave_periodo not in correcoes_existentes:
    #             logger.warning(f"Correção IPCA/IGPM para {mes_aniversario:02d}/{ano_aniversario} não encontrada para CCO {cco['_id']}")
                
    #             # Buscar correções em anos próximos (ano do aniversário e seguinte)
    #             correcao_encontrada = self._buscar_correcao_para_aniversario(
    #                 cco, ano_aniversario, mes_aniversario, correcoes_por_ano
    #             )
                
    #             if correcao_encontrada:
    #                 # Existe correção, mas fora do período
    #                 correcao_info = correcao_encontrada
    #                 correcao = correcao_info['correcao']
                    
    #                 # Calcular diferença de tempo entre aniversário e aplicação
    #                 data_aniversario_esperada = datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc)
    #                 data_aplicacao = correcao_info['data_correcao']
                    
    #                 # Obter taxas
    #                 taxa_aplicada = self._converter_decimal128_para_float(correcao.get('taxaCorrecao', 1.0))
    #                 taxa_esperada = self._obter_taxa_esperada_periodo(ano_taxa, mes_taxa, correcao.get('tipo', 'IPCA'))
                    
    #                 correcoes_fora_do_periodo.append({
    #                     'ano_aniversario': ano_aniversario,
    #                     'mes_aniversario': mes_aniversario,
    #                     'ano_taxa_esperada': ano_taxa,
    #                     'mes_taxa_esperada': mes_taxa,
    #                     'ano_aplicado': data_aplicacao.year,
    #                     'mes_aplicado': data_aplicacao.month,
    #                     'data_aniversario_esperada': data_aniversario_esperada.isoformat(),
    #                     'data_aplicacao_real': data_aplicacao.isoformat(),
    #                     'atraso_meses': self._calcular_atraso_meses(data_aniversario_esperada, data_aplicacao),
    #                     'taxa_aplicada': taxa_aplicada,
    #                     'taxa_esperada': taxa_esperada,
    #                     'diferenca_taxa': (taxa_esperada - taxa_aplicada) if taxa_esperada else 0,
    #                     'valor_atual': self._converter_decimal128_para_float(correcao.get('valorReconhecidoComOH', 0)),
    #                     'tipo_correcao': correcao.get('tipo', 'IPCA'),
    #                     'necessita_ajuste': abs(taxa_esperada - taxa_aplicada) > 0.001 if taxa_esperada else False
    #                 })
    #             else:
    #                 # Gap real - não existe correção
    #                 valor_na_data = self._calcular_valor_cco_na_data(cco, datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc))
                    
    #                 if valor_na_data > 0:
    #                     gaps.append({
    #                         'ano': ano_aniversario,
    #                         'mes': mes_aniversario,
    #                         'ano_taxa_esperada': ano_taxa,
    #                         'mes_taxa_esperada': mes_taxa,
    #                         'data_aniversario': datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc).isoformat(),
    #                         'valor_base': valor_na_data,
    #                         'tipo_sugerido': 'IPCA',
    #                         'prioridade': self._calcular_prioridade_gap(datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc), valor_na_data)
    #                     })
            
    #         ano_aniversario += 1
        
    #     return gaps, correcoes_fora_do_periodo
    
    def _analisar_cco_individual(self, cco: Dict[str, Any], data_atual: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Analisa uma CCO individual para identificar gaps e correções fora do período
        
        NOVO CENÁRIO IMPLEMENTADO:
        - Identifica quando uma correção IPCA/IGPM foi aplicada após o período correto
        - Verifica se houve alterações (recuperação/retificação) entre a data devida e aplicada
        - Reporta essas situações com detalhes das alterações encontradas
        """
        gaps = []
        correcoes_fora_do_periodo = []
        
        # Garantir que data_atual tenha timezone
        if data_atual.tzinfo is None:
            data_atual = data_atual.replace(tzinfo=timezone.utc)
        
        # Validar data de reconhecimento
        data_reconhecimento = self._extrair_data_reconhecimento(cco)
        if not data_reconhecimento:
            return gaps, correcoes_fora_do_periodo
        
        # Mapear correções IPCA/IGPM existentes
        correcoes_existentes = self._mapear_correcoes_ipca_igpm(cco)
        
        # NOVA FUNCIONALIDADE: Mapear TODAS as correções por data para análise temporal
        todas_correcoes_cronologicas = self._mapear_todas_correcoes_cronologicas(cco)
        
        # Calcular primeiro aniversário: 1 mês após o reconhecimento
        mes_reconhecimento = data_reconhecimento.month
        ano_reconhecimento = data_reconhecimento.year
        
        # Primeiro aniversário: mês seguinte ao reconhecimento
        if mes_reconhecimento == 12:
            mes_aniversario = 1
            ano_aniversario = ano_reconhecimento + 2
        else:
            mes_aniversario = mes_reconhecimento + 1
            ano_aniversario = ano_reconhecimento + 1
        
        logger.info(f"Analisando CCO {cco['_id']} - Reconhecimento: {mes_reconhecimento:02d}/{ano_reconhecimento}, Primeiro aniversário: {mes_aniversario:02d}/{ano_aniversario}")
        
        # Mapear correções por ano para melhor análise
        correcoes_por_ano = self._mapear_correcoes_por_ano(cco)
        
        while ano_aniversario <= data_atual.year:
            chave_periodo = (ano_aniversario, mes_aniversario)
            ano_taxa, mes_taxa = self._calcular_mes_taxa_aplicacao(ano_aniversario, mes_aniversario)
            
            # Verificar data limite
            if ano_aniversario == data_atual.year and mes_aniversario >= data_atual.month:
                if mes_aniversario == data_atual.month and data_atual.day < 16:
                    logger.info(f"CCO {cco['_id']} - Aniversário {mes_aniversario:02d}/{ano_aniversario} ainda não atingiu prazo limite")
                    break
                elif mes_aniversario > data_atual.month:
                    logger.info(f"CCO {cco['_id']} - Aniversário {mes_aniversario:02d}/{ano_aniversario} é futuro")
                    break
            
            # Data limite para aplicação da correção (dia 15 do mês seguinte ao aniversário)
            data_limite_aplicacao = self._calcular_data_limite_aplicacao(ano_aniversario, mes_aniversario)
            
            correcao_encontrada = correcoes_existentes.get(chave_periodo)
            
            if not correcao_encontrada:
                # Buscar correções em anos próximos (ano do aniversário e seguinte)
                correcao_encontrada = self._buscar_correcao_para_aniversario(
                    cco, ano_aniversario, mes_aniversario, correcoes_por_ano
                )
            
            
            if not correcao_encontrada:
                
                # Cenário 1: GAP - Correção não aplicada
                valor_base = self._obter_valor_base_para_gap(cco, ano_aniversario, mes_aniversario)
                # regra para ignorar gaps com valor base 0, pois não são relevantes para o processo de correção de IPCA/IGPM de valores menor ou iguais a zero
                if valor_base <= 0:
                    logger.info(f"CCO {cco['_id']} - GAP ignorado: {mes_aniversario:02d}/{ano_aniversario} - Valor base: {valor_base}")
                    print(f"_analisar_cco_individual: VERIFIQUE: CCO {cco['_id']} - GAP ignorado: {mes_aniversario:02d}/{ano_aniversario} - Valor base: {valor_base}")
                    # Próximo aniversário
                    ano_aniversario += 1
                    continue
                
                gap_info = {
                    'ano': ano_aniversario,
                    'mes': mes_aniversario,
                    'data_aniversario': f"{mes_aniversario:02d}/{ano_aniversario}",
                    'ano_taxa': ano_taxa,
                    'mes_taxa': mes_taxa,
                    'periodo_taxa': f"{mes_taxa:02d}/{ano_taxa}",
                    'valor_base': valor_base,
                    'data_limite': data_limite_aplicacao.strftime('%d/%m/%Y'),
                    'prioridade': self._calcular_prioridade_gap(datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc), data_atual)
                }
                
                gaps.append(gap_info)
                logger.info(f"CCO {cco['_id']} - GAP identificado: {gap_info['data_aniversario']}")
                
            else:
                # Cenário 2: Correção existe - verificar se foi aplicada no prazo
                data_aplicacao = correcao_encontrada['data_aplicacao']
                
                if data_aplicacao > data_limite_aplicacao:
                    # Correção aplicada fora do prazo
                    logger.info(f"CCO {cco['_id']} - Correção fora do prazo: {mes_aniversario:02d}/{ano_aniversario}")
                    
                    # NOVO CENÁRIO: Verificar alterações entre data devida e data aplicada
                    alteracoes_no_periodo = self._identificar_alteracoes_entre_datas(
                        todas_correcoes_cronologicas,
                        data_limite_aplicacao,
                        data_aplicacao,
                        correcao_encontrada['tipo']
                    )
                    
                    # Obter informações da taxa esperada vs aplicada
                    taxa_esperada = self._obter_taxa_historica(ano_taxa, mes_taxa, correcao_encontrada['tipo'])
                    if taxa_esperada is None:
                        logger.error(f"CCO {cco['_id']} - Correção fora do prazo inconsistente (não foi possivel recuperar informações da taxa): {mes_aniversario:02d}/{ano_aniversario}")
                        
                    taxa_aplicada = correcao_encontrada.get('taxa_correcao', 1.0)
                    
                    if alteracoes_no_periodo:
                        logger.info(f"CCO {cco['_id']} - Correção fora do prazo com alterações: {mes_aniversario:02d}/{ano_aniversario}")

                        # Calcular valor base considerando alterações no período
                        valor_base_original, valor_antes, valor_depois = self._obter_valor_base_original_para_correcao(
                            cco, ano_aniversario, correcoes_por_ano, alteracoes_no_periodo
                        )
                    else:
                        path_correcao_original = 'correcao' if 'correcao' in correcao_encontrada else 'correcao_original'
                        if path_correcao_original not in correcao_encontrada:
                            logger.error(f"CCO {cco['_id']} - Correção fora do prazo inconsistente (não foi possivel recuperar informações da correção): {mes_aniversario:02d}/{ano_aniversario}")
                            continue
                        
                        valor_base_original = 0
                        valor_antes = correcao_encontrada[path_correcao_original]['valorReconhecidoComOhOriginal']
                        valor_depois = correcao_encontrada[path_correcao_original]['valorReconhecidoComOH']
                        
                    
                    correcao_fora_info = {
                        'ano_aniversario': ano_aniversario,
                        'mes_aniversario': mes_aniversario,
                        'ano_aplicado': data_aplicacao.year,
                        'mes_aplicado': data_aplicacao.month,
                        'data_limite': data_limite_aplicacao.strftime('%d/%m/%Y'),
                        'data_aplicacao': data_aplicacao.strftime('%d/%m/%Y'),
                        'dias_atraso': (data_aplicacao - data_limite_aplicacao).days,
                        'tipo_correcao': correcao_encontrada['tipo'],
                        'taxa_aplicada': taxa_aplicada,
                        'taxa_esperada': taxa_esperada,
                        'diferenca_taxa': abs(taxa_aplicada - taxa_esperada) if taxa_esperada else 0,
                        'necessita_ajuste': abs(taxa_aplicada - taxa_esperada) > 0.0001 if taxa_esperada else True,
                        # NOVOS CAMPOS: Informações sobre alterações no período
                        'teve_alteracoes_no_periodo': len(alteracoes_no_periodo) > 0,
                        'alteracoes_no_periodo': alteracoes_no_periodo,
                        'valor_base_antes_alteracoes': valor_antes if valor_antes else valor_base_original,
                        'valor_base_na_aplicacao': valor_depois if valor_depois else valor_base_original,
                    }
                    
                    correcoes_fora_do_periodo.append(correcao_fora_info)
                    
                    # Log detalhado para o novo cenário
                    if alteracoes_no_periodo:
                        logger.warning(f"CCO {cco['_id']} - Correção {correcao_encontrada['tipo']} aplicada com {len(alteracoes_no_periodo)} alteração(ões) no período:")
                        for alteracao in alteracoes_no_periodo:
                            logger.warning(f"  - {alteracao['tipo']} em {alteracao['data_aplicacao']} (valor: R$ {alteracao['valor_impacto']:,.2f})")
            
            # Próximo aniversário
            ano_aniversario += 1
            
        duplicatas = self._identificar_correcoes_duplicadas(cco)
        
        return gaps, correcoes_fora_do_periodo, duplicatas
    
    def _obter_valor_base_para_gap(self, cco: Dict[str, Any], ano_aniversario: int, mes_aniversario: int) -> float:
        """
        Obtém o valor base que deveria ser usado para calcular o gap
        
        Args:
            cco: Documento da CCO
            ano_aniversario: Ano do aniversário
            mes_aniversario: Mês do aniversário
        
        Returns:
            Valor base para o gap (valor da última correção anterior ao aniversário)
        """
        # Data limite de referência: dia 15 do mês/ano do aniversário ********************
        data_limite_aniversario = datetime(ano_aniversario, mes_aniversario, 15, tzinfo=timezone.utc)
        
        correcoes_anteriores = []
        correcoes = cco.get('correcoesMonetarias', [])
        
        # Iterar sobre todas as correções monetárias
        for correcao in correcoes:
            data_correcao = self._extrair_data_correcao(correcao)
            
            if data_correcao and data_correcao < data_limite_aniversario:
                correcoes_anteriores.append({
                    'data': data_correcao,
                    'correcao': correcao
                })
        
        # Se existem correções anteriores, pegar a mais recente
        if correcoes_anteriores:
            # Ordenar por data e pegar a mais recente
            ultima_correcao_anterior = max(correcoes_anteriores, key=lambda x: x['data'])
            valor_base = self._converter_decimal128_para_float(
                ultima_correcao_anterior['correcao'].get('valorReconhecidoComOH', 0)
            )
            return valor_base
        
        # Se não há correções anteriores, usar valor original da CCO
        valor_original = self._obter_valor_raiz_cco(cco)
        return valor_original
    
    def _calcular_data_limite_aplicacao(self, ano_aniversario: int, mes_aniversario: int) -> datetime:
        """
        Calcula a data limite para aplicação da correção IPCA/IGPM
        
        REGRA: A correção deve ser aplicada até o dia 15 do mês de aniversário
        
        Args:
            ano_aniversario: Ano do aniversário da CCO
            mes_aniversario: Mês do aniversário da CCO
        
        Returns:
            Data limite para aplicação (dia 15 do mês de aniversário)
        
        Exemplo:
            - Aniversário em setembro/2023 (09/2023)
            - Data limite: 15/setembro/2023 (15/10/2023 23:59:59 UTC)
        """
        
        # Data limite: dia 15 do mês de aniversário, final do dia # TODO Revisar se a data limite deve ser essa
        data_limite = datetime(ano_aniversario, mes_aniversario, 19, 23, 59, 59, tzinfo=timezone.utc)
        
        return data_limite
    
    def _mapear_correcoes_por_ano(self, cco: Dict[str, Any]) -> Dict[int, List[Dict]]:
        """
        Mapeia correções IPCA/IGPM por ano
        """
        correcoes_por_ano = {}
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            if correcao.get('tipo') in ['IPCA', 'IGPM']:
                data_correcao = self._extrair_data_correcao(correcao)
                if data_correcao:
                    ano = data_correcao.year
                    if ano not in correcoes_por_ano:
                        correcoes_por_ano[ano] = []
                    correcoes_por_ano[ano].append({
                        'correcao': correcao,
                        'tipo': correcao.get('tipo'),
                        'data_correcao': data_correcao,
                        'data_aplicacao': data_correcao,
                        'mes': data_correcao.month,
                        'taxa_correcao': self._converter_decimal128_para_float(
                            correcao.get('taxaCorrecao', 1.0)
                        ),
                    })
        
        return correcoes_por_ano

    def _buscar_correcao_para_aniversario(self, cco: Dict[str, Any], ano_aniversario: int, 
                                        mes_aniversario: int, correcoes_por_ano: Dict) -> Optional[Dict]:
        """
        Busca correção que pode estar relacionada a um aniversário específico
        Considera correções no ano do aniversário e no ano seguinte
        """
        data_aniversario = datetime(ano_aniversario, mes_aniversario, 15, tzinfo=timezone.utc)
        
        # Buscar em anos próximos (ano do aniversário e seguinte)
        for ano_busca in [ano_aniversario, ano_aniversario + 1]:
            if ano_busca in correcoes_por_ano:
                correcoes_ano = correcoes_por_ano[ano_busca]
                
                # Encontrar correção mais próxima APÓS o aniversário
                for correcao_info in correcoes_ano:
                    data_correcao = correcao_info['data_correcao']
                    
                    # Se a correção é posterior ao aniversário (máximo 18 meses de atraso)
                    if data_correcao > data_aniversario:
                        diff_meses = (data_correcao.year - data_aniversario.year) * 12 + (data_correcao.month - data_aniversario.month)
                        
                        # Considerar apenas correções com até 11 meses de atraso # TODO parametrizar
                        if diff_meses < 12:
                            return correcao_info
                        else:
                            logger.warning(f"Correção com mais de 11 meses de atraso (ano: {data_correcao.year}, mes: {data_correcao.month}). Ignorando.")
        
        return None
    
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
                logger.error(f"Taxa {tipo} não encontrada para {mes:02d}/{ano}. Usando taxa padrão.")
                return None  # 4% como fallback
                
        except Exception as e:
            logger.error(f"Erro ao buscar taxa {tipo} para {mes:02d}/{ano}: {e}")
            return None  # 4% como fallback

    def _calcular_atraso_meses(self, data_esperada: datetime, data_real: datetime) -> int:
        """
        Calcula atraso em meses entre duas datas
        """
        return (data_real.year - data_esperada.year) * 12 + (data_real.month - data_esperada.month)
        
    def _extrair_data_reconhecimento(self, cco: Dict[str, Any]) -> Optional[datetime]:
        """
        Extrai data de reconhecimento da CCO de forma segura
        """
        data_reconhecimento = cco.get('dataReconhecimento')
        if not data_reconhecimento:
            return None
        
        try:
            if isinstance(data_reconhecimento, str):
                # Tratar formato específico: '2025-07-28T17:28:13-0300'
                data_str = data_reconhecimento
                
                # Se tem timezone no formato -HHMM ou +HHMM, adicionar dois pontos
                import re
                if re.search(r'[+-]\d{4}$', data_str):
                    # Converter -0300 para -03:00
                    data_str = data_str[:-2] + ':' + data_str[-2:]
                
                # Converter para datetime com timezone
                dt = datetime.fromisoformat(data_str)
                
                # Se não tem timezone, assumir UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
                
            elif isinstance(data_reconhecimento, datetime):
                # Se não tem timezone, assumir UTC
                if data_reconhecimento.tzinfo is None:
                    return data_reconhecimento.replace(tzinfo=timezone.utc)
                return data_reconhecimento
                
        except Exception as e:
            logger.warning(f"Erro ao processar data de reconhecimento: {e}")
            # Fallback: tentar parsing manual
            try:
                if isinstance(data_reconhecimento, str):
                    # Remover timezone e assumir UTC
                    data_limpa = re.sub(r'[+-]\d{4}$', '', data_reconhecimento)
                    dt = datetime.fromisoformat(data_limpa)
                    return dt.replace(tzinfo=timezone.utc)
            except:
                pass
            return None
        
        return None
    
    def _mapear_correcoes_ipca_igpm(self, cco: Dict[str, Any]) -> Dict[tuple, Dict[str, Any]]:
        """
        Mapeia todas as correções IPCA/IGPM existentes na CCO
        """
        correcoes_mapeadas = {}  # Mudar de set() para {}
        correcoes = cco.get('correcoesMonetarias', [])

        for correcao in correcoes:
            tipo = correcao.get('tipo', '')
            if tipo in ['IPCA', 'IGPM']:
                data_correcao = self._extrair_data_correcao(correcao)
                if data_correcao:
                    chave = (data_correcao.year, data_correcao.month)
                    correcoes_mapeadas[chave] = {  # Adicionar esta estrutura
                        'tipo': tipo,
                        'data_aplicacao': data_correcao,
                        'taxa_correcao': self._converter_decimal128_para_float(
                            correcao.get('taxaCorrecao', 1.0)
                        ),
                        'correcao_original': correcao
                    }

        return correcoes_mapeadas
    
    def _identificar_correcoes_duplicadas(self, cco: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identifica correções IPCA/IGPM duplicadas no mesmo período"""
        duplicatas = []
        correcoes_ipca_igmp = {}
        
        for i, correcao in enumerate(cco.get('correcoesMonetarias', [])):
            if correcao.get('tipo') in ['IPCA', 'IGPM']:
                data_correcao = self._extrair_data_correcao(correcao)
                if data_correcao:
                    chave = (data_correcao.year, data_correcao.month)
                    
                    if chave in correcoes_ipca_igmp:
                        # Duplicata encontrada - manter a mais antiga
                        correcao_anterior = correcoes_ipca_igmp[chave]
                        if data_correcao > correcao_anterior['data']:
                            duplicatas.append({
                                'contratoCpp': cco.get('contratoCpp'),
                                'campo': cco.get('campo'),  
                                'remessa': cco.get('remessa'),
                                'faseRemessa': cco.get('faseRemessa'),
                                'dataReconhecimento': cco.get('dataReconhecimento'),
                                'indice': i,
                                'periodo': f"{data_correcao.month:02d}/{data_correcao.year}",
                                'valor_duplicado': self._converter_decimal128_para_float(
                                    correcao.get('diferencaValor', 0)
                                ),
                                'correcao_duplicada': correcao
                            })
                    else:
                        correcoes_ipca_igmp[chave] = {'data': data_correcao, 'indice': i}
        
        return duplicatas
    
    def _extrair_data_correcao(self, correcao: Dict[str, Any]) -> Optional[datetime]:
        """
        Extrai data de correção de forma segura
        """
        data_correcao = correcao.get('dataCorrecao') or correcao.get('dataCriacaoCorrecao')
        if not data_correcao:
            return None
        
        try:
            if isinstance(data_correcao, str):
                # Tratar formato específico: '2025-07-28T17:28:13-0300'
                data_str = data_correcao
                
                # Se tem timezone no formato -HHMM ou +HHMM, adicionar dois pontos
                import re
                if re.search(r'[+-]\d{4}$', data_str):
                    # Converter -0300 para -03:00
                    data_str = data_str[:-2] + ':' + data_str[-2:]
                
                # Converter para datetime com timezone
                dt = datetime.fromisoformat(data_str)
                
                # Se não tem timezone, assumir UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
                
            elif isinstance(data_correcao, datetime):
                # Se não tem timezone, assumir UTC
                if data_correcao.tzinfo is None:
                    return data_correcao.replace(tzinfo=timezone.utc)
                return data_correcao
                
        except Exception as e:
            logger.warning(f"Erro ao processar data de reconhecimento: {e}")
            # Fallback: tentar parsing manual
            try:
                if isinstance(data_correcao, str):
                    # Remover timezone e assumir UTC
                    data_limpa = re.sub(r'[+-]\d{4}$', '', data_correcao)
                    dt = datetime.fromisoformat(data_limpa)
                    return dt.replace(tzinfo=timezone.utc)
            except:
                pass
            return None
        
        return None
    
    def _calcular_valor_cco_na_data(self, cco: Dict[str, Any], data_referencia: datetime) -> float:
        """
        Calcula o valor da CCO em uma data específica
        """
        # Começar com valor da raiz
        valor = self._converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0))
        
        # Aplicar correções anteriores à data de referência
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            data_correcao = self._extrair_data_correcao(correcao)
            
            if data_correcao and data_correcao <= data_referencia:
                valor = self._converter_decimal128_para_float(
                    correcao.get('valorReconhecidoComOH', valor)
                )
        
        return valor
    
    def _recuperar_correcao_por_periodo(self, cco: Dict[str, Any], chave_periodo: tuple) -> Optional[Dict[str, Any]]:
        """
        Recupera a correção IPCA/IGPM para um período específico (ano, mês)
        
        Args:
            cco: Documento da CCO
            chave_periodo: Tupla (ano, mês) do período desejado
            
        Returns:
            Dicionário da correção encontrada ou None
        """
        ano_desejado, mes_desejado = chave_periodo
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            tipo = correcao.get('tipo', '')
            if tipo in ['IPCA', 'IGPM']:
                data_correcao = self._extrair_data_correcao(correcao)
                
                if data_correcao:
                    # Comparar ano e mês da correção
                    if data_correcao.year == ano_desejado and data_correcao.month == mes_desejado:
                        return correcao
        
        return None

    def _recuperar_correcao_no_ano(self, cco: Dict[str, Any], ano: int) -> List[Dict[str, Any]]:
        """
        Recupera todas as correções IPCA/IGPM em um ano específico
        
        Args:
            cco: Documento da CCO
            ano: Ano desejado
            
        Returns:
            Lista de correções encontradas no ano
        """
        correcoes_no_ano = []
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            tipo = correcao.get('tipo', '')
            if tipo in ['IPCA', 'IGPM']:
                data_correcao = self._extrair_data_correcao(correcao)
                
                if data_correcao and data_correcao.year == ano:
                    correcoes_no_ano.append({
                        'correcao': correcao,
                        'mes': data_correcao.month,
                        'data_correcao': data_correcao
                    })
        
        return sorted(correcoes_no_ano, key=lambda x: x['mes'])

    def _obter_taxa_esperada_periodo(self, ano: int, mes: int, tipo: str = 'IPCA') -> Optional[float]:
        """
        Obtém a taxa que deveria ter sido aplicada para um período específico
        """
        try:
            if tipo == 'IPCA':
                colecao = self.db.ipca_entity
            elif tipo == 'IGPM':
                colecao = self.db.igpm_entity
            else:
                return None
            
            documento = colecao.find_one({
                'anoReferencia': ano,
                'mesReferencia': mes
            })
            
            if documento:
                valor_percentual = self._converter_decimal128_para_float(documento['valor'])
                taxa_fator = 1 + (valor_percentual / 100)
                return taxa_fator
            
            return None
            
        except Exception as e:
            logger.error(f"Erro ao obter taxa esperada para {mes:02d}/{ano}: {e}")
            return None
    
    def _calcular_prioridade_gap(self, data_gap: datetime, valor_base: float) -> str:
        """
        Calcula prioridade do gap baseado na data e valor
        """
        data_atual = datetime.now(timezone.utc)
        
        # Garantir que data_gap também tenha timezone para comparação
        if data_gap.tzinfo is None:
            data_gap = data_gap.replace(tzinfo=timezone.utc)
            
        anos_atraso = (data_atual - data_gap).days / 365.25
        
        if anos_atraso > 3:
            return 'ALTA'
        elif anos_atraso > 1:
            return 'MEDIA'
        else:
            return 'BAIXA'
    
    def _obter_valor_atual_cco(self, cco: Dict[str, Any]) -> float:
        """
        Obtém o valor atual da CCO
        """
        correcoes = cco.get('correcoesMonetarias', [])
        if correcoes:
            return self._converter_decimal128_para_float(
                correcoes[-1].get('valorReconhecidoComOH', 0)
            )
        
        return self._converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0))
    
    def _obter_valor_raiz_cco(self, cco: Dict[str, Any]) -> float:
        """
        Obtém o valor da raiz da CCO
        """
        return self._converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0))
    
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
        
    def _mapear_todas_correcoes_cronologicas(self, cco: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        NOVA FUNÇÃO: Mapeia todas as correções em ordem cronológica para análise temporal
        
        Returns:
            Lista de correções ordenadas cronologicamente com tipo e impacto
        """
        correcoes_cronologicas = []
        
        correcoes_monetarias = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes_monetarias:
            data_correcao = self._extrair_data_correcao(correcao)
            if not data_correcao:
                continue
                
            tipo_correcao = correcao.get('tipo', '').upper()
            
            # Calcular impacto da correção
            valor_antes = correcao.get('valorReconhecidoComOhOriginal', 0)
            valor_depois = correcao.get('valorReconhecidoComOH', 0)
            valor_impacto = self._converter_decimal128_para_float(valor_depois) - self._converter_decimal128_para_float(valor_antes)
            
            correcao_info = {
                'data_aplicacao': data_correcao,
                'tipo': tipo_correcao,
                'valor_antes': self._converter_decimal128_para_float(valor_antes),
                'valor_depois': self._converter_decimal128_para_float(valor_depois),
                'valor_impacto': valor_impacto,
                'taxa_correcao': self._converter_decimal128_para_float(correcao.get('taxaCorrecao', 1.0)),
                'ativo': correcao.get('ativo', True)
            }
            
            correcoes_cronologicas.append(correcao_info)
        
        # Ordenar por data de aplicação
        return sorted(correcoes_cronologicas, key=lambda x: x['data_aplicacao'])

    def _identificar_alteracoes_entre_datas(self, correcoes_cronologicas: List[Dict[str, Any]], 
                                      data_inicio: datetime, data_fim: datetime,
                                      tipo_correcao_principal: str) -> List[Dict[str, Any]]:
        """
        NOVA FUNÇÃO: Identifica alterações (recuperação/retificação) entre duas datas
        
        Args:
            correcoes_cronologicas: Lista de correções em ordem cronológica
            data_inicio: Data de início do período (data limite para aplicação)
            data_fim: Data de fim do período (data efetiva de aplicação)
            tipo_correcao_principal: Tipo da correção principal (IPCA/IGPM)
        
        Returns:
            Lista de alterações encontradas no período
        """
        alteracoes_no_periodo = []
        
        for correcao in correcoes_cronologicas:
            data_correcao = correcao['data_aplicacao']
            tipo_correcao = correcao['tipo']
            
            # Verificar se a correção está no período de interesse
            if data_inicio <= data_correcao <= data_fim:
                # Excluir a própria correção IPCA/IGPM que estamos analisando
                if tipo_correcao not in ['IPCA', 'IGPM']:
                    alteracao_info = {
                        'tipo': tipo_correcao,
                        'data_aplicacao': data_correcao.strftime('%d/%m/%Y'),
                        'valor_antes': correcao['valor_antes'],
                        'valor_depois': correcao['valor_depois'],
                        'valor_impacto': correcao['valor_impacto'],
                        'impacto_percentual': (correcao['valor_impacto'] / correcao['valor_antes'] * 100) if correcao['valor_antes'] > 0 else 0
                    }
                    
                    alteracoes_no_periodo.append(alteracao_info)
        
        return alteracoes_no_periodo

    def _obter_valor_base_original_para_correcao(self, cco: Dict[str, Any], ano_aniversario: int, 
                                           correcoes_por_ano: Dict[int, List], 
                                           alteracoes_no_periodo: List[Dict[str, Any]]) -> float:
        """
        NOVA FUNÇÃO: Obtém o valor base que deveria ter sido usado para a correção,
        considerando que pode ter havido alterações no período
        
        Args:
            cco: Documento da CCO
            ano_aniversario: Ano do aniversário da correção
            correcoes_por_ano: Mapeamento de correções por ano
            alteracoes_no_periodo: Lista de alterações identificadas no período
        
        Returns:
            Valor base original (antes das alterações no período)
        """
        
        # Se houve alterações, calcular o valor antes da primeira alteração
        valor_atual = self._obter_valor_atual_cco(cco)
        
        # Reverter as alterações para obter o valor original
        for alteracao in reversed(alteracoes_no_periodo):  # Reverter em ordem inversa
            valor_atual -= alteracao['valor_impacto']
            
        # recuperar o ultimo item alteracoes_no_periodo
        if alteracoes_no_periodo and len(alteracoes_no_periodo) == 1:   
            valor_antes = alteracoes_no_periodo[0]['valor_antes']
            valor_depois = alteracoes_no_periodo[0]['valor_depois']
        else:
            valor_antes = 0
            valor_depois = 0
        
        return valor_atual, valor_antes, valor_depois
    
    def exportar_gaps_csv(self, resultado_analise: Dict[str, Any], 
                     arquivo_saida: str = "gaps_ipca_igpm.csv") -> bool:
        """
        Exporta resultado da análise para CSV (incluindo correções fora do período e NOVO CENÁRIO)
        
        ATUALIZAÇÃO: Inclui informações sobre alterações encontradas entre o período devido e aplicado
        """
        try:
            with open(arquivo_saida, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'cco_id', 'contrato', 'campo', 'remessa', 'fase', 
                    'data_reconhecimento', 'valor_atual', 'tipo_problema',
                    # Campos de gap
                    'gap_ano', 'gap_mes', 'gap_data_aniversario', 
                    'gap_valor_base', 'gap_prioridade',
                    # Campos de correção fora do período
                    'correcao_ano_aniversario', 'correcao_mes_aniversario',
                    'correcao_ano_aplicado', 'correcao_mes_aplicado',
                    'data_limite_aplicacao', 'data_efetiva_aplicacao', 'dias_atraso',
                    'tipo_correcao', 'taxa_aplicada', 'taxa_esperada',
                    'diferenca_taxa', 'necessita_ajuste',
                    # NOVOS CAMPOS: Alterações no período
                    'teve_alteracoes_no_periodo', 'qtd_alteracoes_no_periodo',
                    'valor_base_original', 'valor_base_na_aplicacao',
                    'tipos_alteracoes_encontradas', 'datas_alteracoes',
                    'valores_impacto_alteracoes', 'impacto_total_alteracoes',
                    'duplicata_periodo', 'duplicata_valor_removido'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Exportar gaps (sem alteração)
                for cco in resultado_analise.get('ccos_com_gaps', []):
                    for gap in cco['gaps']:
                        try:
                            writer.writerow({
                                'cco_id': cco['_id'],
                                'contrato': cco.get('contratoCpp', ''), 
                                'campo': cco.get('campo', ''),
                                'remessa': cco.get('remessa', ''),
                                'fase': cco.get('faseRemessa', ''),
                                'data_reconhecimento': cco.get('dataReconhecimento', ''),
                                'valor_atual': cco.get('valorAtual', 0),
                                'tipo_problema': 'GAP',
                                'gap_ano': gap['ano'],
                                'gap_mes': gap['mes'],
                                'gap_data_aniversario': gap['data_aniversario'],
                                'gap_valor_base': gap['valor_base'],
                                'gap_prioridade': gap['prioridade'],
                                # Campos de correção vazios para gaps
                                'correcao_ano_aniversario': '',
                                'correcao_mes_aniversario': '',
                                'correcao_ano_aplicado': '',
                                'correcao_mes_aplicado': '',
                                'data_limite_aplicacao': '',
                                'data_efetiva_aplicacao': '',
                                'dias_atraso': '',
                                'tipo_correcao': '',
                                'taxa_aplicada': '',
                                'taxa_esperada': '',
                                'diferenca_taxa': '',
                                'necessita_ajuste': '',
                                # Novos campos vazios para gaps
                                'teve_alteracoes_no_periodo': '',
                                'qtd_alteracoes_no_periodo': '',
                                'valor_base_original': '',
                                'valor_base_na_aplicacao': '',
                                'tipos_alteracoes_encontradas': '',
                                'datas_alteracoes': '',
                                'valores_impacto_alteracoes': '',
                                'impacto_total_alteracoes': '',
                                # Novos campos vazios para duplicatas
                                'duplicata_periodo' : '',
                                'duplicata_valor_removido': ''
                            })
                        except Exception as e:
                            logging.error(f"Erro ao exportar gap para CSV: {str(e)}")
                        
                for cco in resultado_analise.get('ccos_com_duplicatas', []):
                    for duplicata in cco['duplicatas']:
                        try:
                            writer.writerow({
                                'cco_id': cco['_id'],
                                'contrato': cco.get('contratoCpp', ''),
                                'campo': duplicata.get('campo', ''),
                                'remessa': duplicata.get('remessa', ''),
                                'fase': duplicata.get('faseRemessa', ''),
                                'data_reconhecimento': duplicata.get('dataReconhecimento', ''),
                                'valor_atual': cco.get('valorAtual', 0),
                                'tipo_problema': 'DUPLICATA',
                                # Campos de gap vazios para correções
                                'gap_ano': '',
                                'gap_mes': '',
                                'gap_data_aniversario': '',
                                'gap_valor_base': '',
                                'gap_prioridade': '',
                                # Campos de correção vazios para gaps
                                'correcao_ano_aniversario': '',
                                'correcao_mes_aniversario': '',
                                'correcao_ano_aplicado': '',
                                'correcao_mes_aplicado': '',
                                'data_limite_aplicacao': '',
                                'data_efetiva_aplicacao': '',
                                'dias_atraso': '',
                                'tipo_correcao': '',
                                'taxa_aplicada': '',
                                'taxa_esperada': '',
                                'diferenca_taxa': '',
                                'necessita_ajuste': '',
                                # Novos campos vazios para gaps
                                'teve_alteracoes_no_periodo': '',
                                'qtd_alteracoes_no_periodo': '',
                                'valor_base_original': '',
                                'valor_base_na_aplicacao': '',
                                'tipos_alteracoes_encontradas': '',
                                'datas_alteracoes': '',
                                'valores_impacto_alteracoes': '',
                                'impacto_total_alteracoes': '',
                                'duplicata_periodo': duplicata['periodo'],
                                'duplicata_valor_removido': duplicata['valor_duplicado']
                                
                            })
                        except Exception as e:
                            logging.error(f"Erro ao exportar duplicata para CSV: {str(e)}")
                            
                            
                # Exportar correções fora do período (ATUALIZADO COM NOVOS CAMPOS)
                for cco in resultado_analise.get('ccos_com_correcoes_fora_periodo', []):
                    for correcao in cco['correcoes_fora_periodo']:
                        # Processar alterações no período
                        alteracoes = correcao.get('alteracoes_no_periodo', [])
                        teve_alteracoes = correcao.get('teve_alteracoes_no_periodo', False)
                        
                        # Preparar strings para alterações
                        tipos_alteracoes = ';'.join([alt['tipo'] for alt in alteracoes]) if alteracoes else ''
                        datas_alteracoes = ';'.join([alt['data_aplicacao'] for alt in alteracoes]) if alteracoes else ''
                        valores_impacto = ';'.join([f"{alt['valor_impacto']:,.2f}" for alt in alteracoes]) if alteracoes else ''
                        impacto_total = sum([alt['valor_impacto'] for alt in alteracoes]) if alteracoes else 0
                        
                        try:
                            writer.writerow({
                                'cco_id': cco['_id'],
                                'contrato': cco.get('contratoCpp', ''),
                                'campo': cco.get('campo', ''),
                                'remessa': cco.get('remessa', ''),
                                'fase': cco.get('faseRemessa', ''),
                                'data_reconhecimento': cco.get('dataReconhecimento', ''),
                                'valor_atual': cco.get('valorAtual', 0),
                                'tipo_problema': 'CORRECAO_FORA_PERIODO_COM_ALTERACAO' if teve_alteracoes else 'CORRECAO_FORA_PERIODO',
                                # Campos de gap vazios para correções
                                'gap_ano': '',
                                'gap_mes': '',
                                'gap_data_aniversario': '',
                                'gap_valor_base': '',
                                'gap_prioridade': '',
                                # Campos de correção preenchidos
                                'correcao_ano_aniversario': correcao['ano_aniversario'],
                                'correcao_mes_aniversario': correcao['mes_aniversario'],
                                'correcao_ano_aplicado': correcao['ano_aplicado'],
                                'correcao_mes_aplicado': correcao['mes_aplicado'],
                                'data_limite_aplicacao': correcao['data_limite'],
                                'data_efetiva_aplicacao': correcao['data_aplicacao'],
                                'dias_atraso': correcao['dias_atraso'],
                                'tipo_correcao': correcao['tipo_correcao'],
                                'taxa_aplicada': correcao['taxa_aplicada'],
                                'taxa_esperada': correcao['taxa_esperada'],
                                'diferenca_taxa': correcao['diferenca_taxa'],
                                'necessita_ajuste': correcao['necessita_ajuste'],
                                # NOVOS CAMPOS: Informações sobre alterações
                                'teve_alteracoes_no_periodo': 'SIM' if teve_alteracoes else 'NÃO',
                                'qtd_alteracoes_no_periodo': len(alteracoes),
                                'valor_base_original': correcao.get('valor_base_antes_alteracoes', ''),
                                'valor_base_na_aplicacao': correcao.get('valor_base_na_aplicacao', ''),
                                'tipos_alteracoes_encontradas': tipos_alteracoes,
                                'datas_alteracoes': datas_alteracoes,
                                'valores_impacto_alteracoes': valores_impacto,
                                'impacto_total_alteracoes': f"{impacto_total:,.2f}" if impacto_total != 0 else '',
                                # Novos campos vazios para duplicatas
                                'duplicata_periodo' : '',
                                'duplicata_valor_removido': ''
                            })
                        except Exception as e:
                            logging.error(f"Erro ao exportar correção fora do período para CSV: {str(e)}")    
            
            logger.info(f"Relatório de gaps e correções exportado para: {arquivo_saida}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao exportar gaps para CSV: {e}")
            return False
    
    def exportar_gaps_json(self, resultado_analise: Dict[str, Any], 
                          arquivo_saida: str = "gaps_ipca_igpm.json") -> bool:
        """
        Exporta resultado da análise para JSON
        """
        try:
            with open(arquivo_saida, 'w', encoding='utf-8') as jsonfile:
                json.dump(resultado_analise, jsonfile, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Relatório de gaps exportado para: {arquivo_saida}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao exportar gaps para JSON: {e}")
            return False
    
    def gerar_relatorio_resumido(self, resultado_analise: Dict[str, Any]) -> str:
        """
        Gera relatório resumido em texto
        """
        stats = resultado_analise.get('estatisticas', {})
        
        relatorio = f"""
=== RELATÓRIO DE GAPS IPCA/IGPM ===

Data da Análise: {resultado_analise.get('data_analise', 'N/A')}

ESTATÍSTICAS GERAIS:
- Total de CCOs analisadas: {stats.get('total_ccos_analisadas', 0):,}
- CCOs com gaps: {stats.get('ccos_com_gaps', 0):,}
- Total de gaps identificados: {stats.get('total_gaps_identificados', 0):,}
- Valor total impactado: R$ {stats.get('valor_total_impactado', 0):,.2f}

GAPS POR ANO:"""
        
        gaps_por_ano = stats.get('gaps_por_ano', {})
        for ano in sorted(gaps_por_ano.keys()):
            relatorio += f"\n- {ano}: {gaps_por_ano[ano]} gaps"
        
        relatorio += "\n\nGAPS POR CONTRATO:"
        gaps_por_contrato = stats.get('gaps_por_contrato', {})
        for contrato in sorted(gaps_por_contrato.keys()):
            relatorio += f"\n- {contrato}: {gaps_por_contrato[contrato]} gaps"
        
        return relatorio
    
    def analisar_impacto_financeiro(self, resultado_analise: Dict[str, Any], 
                                   taxa_ipca_estimada: float = 0.045) -> Dict[str, Any]:
        """
        Analisa o impacto financeiro dos gaps identificados
        
        Args:
            resultado_analise: Resultado da análise de gaps
            taxa_ipca_estimada: Taxa IPCA estimada para cálculo (4.5% por padrão)
            
        Returns:
            Análise de impacto financeiro
        """
        impacto_total = 0.0
        impactos_por_contrato = {}
        impactos_por_ano = {}
        
        for cco in resultado_analise.get('ccos_com_gaps', []):
            contrato = cco.get('contratoCpp', 'N/A')
            
            if contrato not in impactos_por_contrato:
                impactos_por_contrato[contrato] = 0.0
            
            for gap in cco['gaps']:
                valor_base = gap['valor_base']
                impacto_gap = valor_base * taxa_ipca_estimada
                
                impacto_total += impacto_gap
                impactos_por_contrato[contrato] += impacto_gap
                
                ano = gap['ano']
                if ano not in impactos_por_ano:
                    impactos_por_ano[ano] = 0.0
                impactos_por_ano[ano] += impacto_gap
        
        return {
            'taxa_ipca_utilizada': taxa_ipca_estimada,
            'impacto_financeiro_total': impacto_total,
            'impactos_por_contrato': impactos_por_contrato,
            'impactos_por_ano': impactos_por_ano,
            'percentual_do_valor_total': (impacto_total / resultado_analise['estatisticas']['valor_total_impactado'] * 100) if resultado_analise['estatisticas']['valor_total_impactado'] > 0 else 0
        }

class IPCAGapReportGenerator:
    """
    Gerador de relatórios específicos para gaps IPCA/IGPM
    """
    
    def __init__(self, gap_analyzer: IPCAGapAnalyzer):
        self.analyzer = gap_analyzer
    
    def gerar_relatorio_executivo(self, filtros: Dict[str, Any] = None) -> str:
        """
        Gera relatório executivo para apresentação
        """
        resultado = self.analyzer.analisar_gaps_sistema(filtros)
        impacto = self.analyzer.analisar_impacto_financeiro(resultado)
        
        stats = resultado.get('estatisticas', {})
        
        relatorio = f"""
=== RELATÓRIO EXECUTIVO - GAPS IPCA/IGPM ===

RESUMO EXECUTIVO:
• {stats.get('ccos_com_gaps', 0)} CCOs identificadas com gaps de correção monetária
• {stats.get('total_gaps_identificados', 0)} correções IPCA/IGPM faltantes
• Valor total impactado: R$ {stats.get('valor_total_impactado', 0):,.2f}
• Impacto financeiro estimado: R$ {impacto['impacto_financeiro_total']:,.2f}

PRINCIPAIS CONTRATOS IMPACTADOS:"""
        
        # Top 5 contratos por gaps
        gaps_contratos = stats.get('gaps_por_contrato', {})
        top_contratos = sorted(gaps_contratos.items(), key=lambda x: x[1], reverse=True)[:5]
        
        for i, (contrato, gaps) in enumerate(top_contratos, 1):
            impacto_contrato = impacto['impactos_por_contrato'].get(contrato, 0)
            relatorio += f"\n{i}. {contrato}: {gaps} gaps (R$ {impacto_contrato:,.2f})"
        
        relatorio += f"""

ANOS COM MAIOR INCIDÊNCIA:"""
        
        gaps_anos = stats.get('gaps_por_ano', {})
        top_anos = sorted(gaps_anos.items(), key=lambda x: x[1], reverse=True)[:3]
        
        for ano, gaps in top_anos:
            impacto_ano = impacto['impactos_por_ano'].get(ano, 0)
            relatorio += f"\n• {ano}: {gaps} gaps (R$ {impacto_ano:,.2f})"
        
        return relatorio
    
    def gerar_relatorio_detalhado_contrato(self, contrato: str) -> str:
        """
        Gera relatório detalhado para um contrato específico
        """
        filtros = {'contratoCpp': contrato}
        resultado = self.analyzer.analisar_gaps_sistema(filtros)
        
        relatorio = f"""
=== RELATÓRIO DETALHADO - CONTRATO {contrato} ===

"""
        
        ccos_contrato = resultado.get('ccos_com_gaps', [])
        if not ccos_contrato:
            relatorio += "Nenhum gap identificado para este contrato."
            return relatorio
        
        for cco in ccos_contrato:
            relatorio += f"""
CCO: {cco['_id']}
Campo: {cco.get('campo', 'N/A')}
Remessa: {cco.get('remessa', 'N/A')} (Fase: {cco.get('faseRemessa', 'N/A')})
Valor Atual: R$ {cco.get('valorAtual', 0):,.2f}
Data Reconhecimento: {cco.get('dataReconhecimento', 'N/A')}

Gaps Identificados:"""
            
            for gap in cco['gaps']:
                relatorio += f"""
  • {gap['mes']:02d}/{gap['ano']} - Valor Base: R$ {gap['valor_base']:,.2f} (Prioridade: {gap['prioridade']})"""
            
            relatorio += "\n" + "-" * 80 + "\n"
        
        return relatorio