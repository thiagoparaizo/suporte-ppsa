"""
Analisador de Gaps IPCA/IGPM
Utilitário para identificar CCOs que precisam de correção monetária baseado na regra de aniversário.
"""

import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from bson import ObjectId
from typing import Dict, Any, List, Optional
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
            
            cursor = self.db.conta_custo_oleo_entity.find(query).sort(sort)
            
            for cco in cursor:
                estatisticas['total_ccos_analisadas'] += 1
                
                gaps, correcoes_fora = self._analisar_cco_individual(cco, data_atual)
                
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
            
            return {
                'data_analise': data_atual.isoformat(),
                'filtros_aplicados': filtros or {},
                'estatisticas': estatisticas,
                'ccos_com_gaps': ccos_com_gaps,
                'ccos_com_correcoes_fora_periodo': ccos_com_correcoes_fora
            }
            
        except Exception as e:
            logger.error(f"Erro ao analisar gaps do sistema: {e}")
            return {'error': str(e)}
    
    def _analisar_cco_individual(self, cco: Dict[str, Any], data_atual: datetime) -> List[Dict[str, Any]]:
        """
        Analisa uma CCO individual para identificar gaps
        """
        gaps = []
        correcoes_fora_do_periodo = []
        
        # Garantir que data_atual tenha timezone
        if data_atual.tzinfo is None:
            data_atual = data_atual.replace(tzinfo=timezone.utc)
        
        # Validar data de reconhecimento
        data_reconhecimento = self._extrair_data_reconhecimento(cco)
        if not data_reconhecimento:
            return gaps
        
        # Mapear correções IPCA/IGPM existentes
        correcoes_existentes = self._mapear_correcoes_ipca_igpm(cco)
        
        # Calcular primeiro aniversário: 1 mês após o reconhecimento
        mes_reconhecimento = data_reconhecimento.month
        ano_reconhecimento = data_reconhecimento.year
        
        # Primeiro aniversário: mês seguinte ao reconhecimento
        if mes_reconhecimento == 12:
            # Dezembro -> Janeiro do ano seguinte
            mes_aniversario = 1
            ano_aniversario = ano_reconhecimento + 2  # +2 porque vai para janeiro do segundo ano
        else:
            # Outros meses -> mês seguinte do ano seguinte
            mes_aniversario = mes_reconhecimento + 1
            ano_aniversario = ano_reconhecimento + 1
        
        logger.info(f"Analisando CCO {cco['_id']} - Reconhecimento: {mes_reconhecimento:02d}/{ano_reconhecimento}, Primeiro aniversário: {mes_aniversario:02d}/{ano_aniversario}")
        
        # Primeiro, mapear TODAS as correções por ano para melhor análise
        correcoes_por_ano = self._mapear_correcoes_por_ano(cco)
        
        while ano_aniversario <= data_atual.year:
            chave_periodo = (ano_aniversario, mes_aniversario)
            ano_taxa, mes_taxa = self._calcular_mes_taxa_aplicacao(ano_aniversario, mes_aniversario)
            logger.info(f"CCO {cco['_id']} - Aniversário: {mes_aniversario:02d}/{ano_aniversario}, Taxa período: {mes_taxa:02d}/{ano_taxa}")
            
            # Verificar data limite
            if ano_aniversario == data_atual.year and mes_aniversario >= data_atual.month:
                if mes_aniversario == data_atual.month and data_atual.day < 16:
                    break
                elif mes_aniversario > data_atual.month:
                    break
            
            # Verificar se já existe correção EXATA para este período
            if chave_periodo not in correcoes_existentes:
                logger.warning(f"Correção IPCA/IGPM para {mes_aniversario:02d}/{ano_aniversario} não encontrada para CCO {cco['_id']}")
                
                # Buscar correções em anos próximos (ano do aniversário e seguinte)
                correcao_encontrada = self._buscar_correcao_para_aniversario(
                    cco, ano_aniversario, mes_aniversario, correcoes_por_ano
                )
                
                if correcao_encontrada:
                    # Existe correção, mas fora do período
                    correcao_info = correcao_encontrada
                    correcao = correcao_info['correcao']
                    
                    # Calcular diferença de tempo entre aniversário e aplicação
                    data_aniversario_esperada = datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc)
                    data_aplicacao = correcao_info['data_correcao']
                    
                    # Obter taxas
                    taxa_aplicada = self._converter_decimal128_para_float(correcao.get('taxaCorrecao', 1.0))
                    taxa_esperada = self._obter_taxa_esperada_periodo(ano_taxa, mes_taxa, correcao.get('tipo', 'IPCA'))
                    
                    correcoes_fora_do_periodo.append({
                        'ano_aniversario': ano_aniversario,
                        'mes_aniversario': mes_aniversario,
                        'ano_taxa_esperada': ano_taxa,
                        'mes_taxa_esperada': mes_taxa,
                        'ano_aplicado': data_aplicacao.year,
                        'mes_aplicado': data_aplicacao.month,
                        'data_aniversario_esperada': data_aniversario_esperada.isoformat(),
                        'data_aplicacao_real': data_aplicacao.isoformat(),
                        'atraso_meses': self._calcular_atraso_meses(data_aniversario_esperada, data_aplicacao),
                        'taxa_aplicada': taxa_aplicada,
                        'taxa_esperada': taxa_esperada,
                        'diferenca_taxa': (taxa_esperada - taxa_aplicada) if taxa_esperada else 0,
                        'valor_atual': self._converter_decimal128_para_float(correcao.get('valorReconhecidoComOH', 0)),
                        'tipo_correcao': correcao.get('tipo', 'IPCA'),
                        'necessita_ajuste': abs(taxa_esperada - taxa_aplicada) > 0.001 if taxa_esperada else False
                    })
                else:
                    # Gap real - não existe correção
                    valor_na_data = self._calcular_valor_cco_na_data(cco, datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc))
                    
                    if valor_na_data > 0:
                        gaps.append({
                            'ano': ano_aniversario,
                            'mes': mes_aniversario,
                            'ano_taxa_esperada': ano_taxa,
                            'mes_taxa_esperada': mes_taxa,
                            'data_aniversario': datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc).isoformat(),
                            'valor_base': valor_na_data,
                            'tipo_sugerido': 'IPCA',
                            'prioridade': self._calcular_prioridade_gap(datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc), valor_na_data)
                        })
            
            ano_aniversario += 1
        
        return gaps, correcoes_fora_do_periodo
    
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
                        'data_correcao': data_correcao,
                        'mes': data_correcao.month
                    })
        
        return correcoes_por_ano

    def _buscar_correcao_para_aniversario(self, cco: Dict[str, Any], ano_aniversario: int, 
                                        mes_aniversario: int, correcoes_por_ano: Dict) -> Optional[Dict]:
        """
        Busca correção que pode estar relacionada a um aniversário específico
        Considera correções no ano do aniversário e no ano seguinte
        """
        data_aniversario = datetime(ano_aniversario, mes_aniversario, 16, tzinfo=timezone.utc)
        
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
                        
                        # Considerar apenas correções com até 18 meses de atraso
                        if diff_meses <= 18:
                            return correcao_info
        
        return None

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
    
    def _mapear_correcoes_ipca_igpm(self, cco: Dict[str, Any]) -> set:
        """
        Mapeia todas as correções IPCA/IGPM existentes na CCO
        """
        correcoes_mapeadas = set()
        correcoes = cco.get('correcoesMonetarias', [])
        
        for correcao in correcoes:
            tipo = correcao.get('tipo', '')
            if tipo in ['IPCA', 'IGPM']:
                data_correcao = self._extrair_data_correcao(correcao)
                if data_correcao:
                    correcoes_mapeadas.add((data_correcao.year, data_correcao.month))
        
        return correcoes_mapeadas
    
    def _extrair_data_correcao(self, correcao: Dict[str, Any]) -> Optional[datetime]:
        """
        Extrai data de correção de forma segura
        """
        data_correcao = correcao.get('dataCorrecao')
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
    
    def exportar_gaps_csv(self, resultado_analise: Dict[str, Any], 
                     arquivo_saida: str = "gaps_ipca_igpm.csv") -> bool:
        """
        Exporta resultado da análise para CSV (incluindo correções fora do período)
        """
        try:
            with open(arquivo_saida, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'cco_id', 'contrato', 'campo', 'remessa', 'fase', 
                    'data_reconhecimento', 'valor_atual', 'tipo_problema',
                    'gap_ano', 'gap_mes', 'gap_data_aniversario', 
                    'gap_valor_base', 'gap_prioridade',
                    'correcao_ano_aniversario', 'correcao_mes_aniversario',
                    'correcao_ano_aplicado', 'correcao_mes_aplicado',
                    'taxa_aplicada', 'taxa_esperada',
                    'diferenca_taxa', 'necessita_ajuste'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Exportar gaps (sem alteração)
                for cco in resultado_analise.get('ccos_com_gaps', []):
                    for gap in cco['gaps']:
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
                            'taxa_aplicada': '',
                            'taxa_esperada': '',
                            'diferenca_taxa': '',
                            'necessita_ajuste': ''
                        })
                
                # Exportar correções fora do período (com ano adicionado)
                for cco in resultado_analise.get('ccos_com_correcoes_fora_periodo', []):
                    for correcao in cco['correcoes_fora_periodo']:
                        writer.writerow({
                            'cco_id': cco['_id'],
                            'contrato': cco.get('contratoCpp', ''),
                            'campo': cco.get('campo', ''),
                            'remessa': cco.get('remessa', ''),
                            'fase': cco.get('faseRemessa', ''),
                            'data_reconhecimento': cco.get('dataReconhecimento', ''),
                            'valor_atual': cco.get('valorAtual', 0),
                            'tipo_problema': 'CORRECAO_FORA_PERIODO',
                            # Campos de gap vazios para correções
                            'gap_ano': '',
                            'gap_mes': '',
                            'gap_data_aniversario': '',
                            'gap_valor_base': '',
                            'gap_prioridade': '',
                            # Campos de correção preenchidos (agora com ano)
                            'correcao_ano_aniversario': correcao['ano_aniversario'],
                            'correcao_mes_aniversario': correcao['mes_aniversario'],
                            'correcao_ano_aplicado': correcao['ano_aplicado'],      # NOVO
                            'correcao_mes_aplicado': correcao['mes_aplicado'],
                            'taxa_aplicada': correcao['taxa_aplicada'],
                            'taxa_esperada': correcao['taxa_esperada'],
                            'diferenca_taxa': correcao['diferenca_taxa'],
                            'necessita_ajuste': correcao['necessita_ajuste']
                        })
            
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