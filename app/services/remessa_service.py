"""
Serviço para análise de remessas x CCOs
Implementa a lógica do script MongoDB em Python
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict, Counter
from pymongo import MongoClient
from pymongo.collection import Collection

from app.repositories.remessa_repository import RemessaRepository
from app.repositories.cco_repository import CCORepository
from app.utils.converters import (
    converter_decimal128_para_float, 
    formatar_data_brasileira,
    validar_e_converter_valor_monetario
)

logger = logging.getLogger(__name__)

class RemessaAnaliseService:
    """Serviço principal para análise de remessas x CCOs"""
    
    def __init__(self, mongo_uri: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client.sgppServices
        self.remessa_repo = RemessaRepository(self.db)
        self.cco_repo = CCORepository(self.db)

    def pesquisar_remessas_por_filtros(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pesquisa remessas aplicando filtros específicos
        """
        try:
            logger.info(f"Pesquisando remessas com filtros: {filtros}")
            
            filtro_mongo = self._construir_filtro_mongo(filtros)
            remessas = self.remessa_repo.buscar_por_filtros(filtro_mongo)
            
            # Processar resultados
            resultados = []
            for remessa in remessas:
                resultado = self._processar_remessa_resumida(remessa)
                resultados.append(resultado)
            
            return {
                'success': True,
                'resultados': resultados,
                'total': len(resultados),
                'filtro_aplicado': filtro_mongo
            }
            
        except Exception as e:
            logger.error(f"Erro ao pesquisar remessas: {e}")
            return {'success': False, 'error': str(e)}

    def pesquisar_remessa_por_id(self, remessa_id: str) -> Dict[str, Any]:
        """
        Pesquisa remessa específica por ID
        """
        try:
            logger.info(f"Pesquisando remessa por ID: {remessa_id}")
            
            remessa = self.remessa_repo.buscar_por_id(remessa_id)
            if not remessa:
                return {'success': False, 'error': 'Remessa não encontrada'}
            
            resultado = self._processar_remessa_resumida(remessa)
            
            return {
                'success': True,
                'resultado': resultado
            }
            
        except Exception as e:
            logger.error(f"Erro ao pesquisar remessa por ID: {e}")
            return {'success': False, 'error': str(e)}

    def analisar_remessas_vs_ccos(self, filtros: Dict[str, Any], analise_detalhada: bool = False) -> Dict[str, Any]:
        """
        Executa análise completa de remessas vs CCOs
        Equivalente ao script JavaScript
        """
        logger.info("=== INICIANDO ANÁLISE COMPLETA REMESSAS x CCOs ===")
        logger.info(f"Filtros aplicados: {filtros}")
        logger.info(f"Análise detalhada: {'SIM' if analise_detalhada else 'NÃO'}")
        
        resultado = {
            'parametros': filtros,
            'tipoAnalise': 'DETALHADA' if analise_detalhada else 'SIMPLIFICADA',
            'remessasAnalisadas': [],
            'estatisticas': self._inicializar_estatisticas()
        }
        
        try:
            # STEP 1: Buscar remessas com reconhecimento
            logger.info("=== STEP 1: Buscando remessas com reconhecimento ===")
            remessas_com_reconhecimento = self._executar_step1(filtros, analise_detalhada)
            resultado['estatisticas']['totalRemessas'] = len(remessas_com_reconhecimento)
            logger.info(f"Remessas encontradas com reconhecimento: {len(remessas_com_reconhecimento)}")
            
            # STEP 2 e 3: Analisar CCOs
            logger.info("=== STEP 2 e 3: Analisando CCOs e complementando dados ===")
            resultado['remessasAnalisadas'] = self._executar_steps2e3(
                remessas_com_reconhecimento, 
                resultado['estatisticas'], 
                analise_detalhada
            )
            
            # Gerar relatório final
            self._gerar_relatorio_estatisticas(resultado['estatisticas'])
            
        except Exception as error:
            logger.error(f"ERRO CRÍTICO: {error}")
            resultado['estatisticas']['erros'].append(f"ERRO CRÍTICO: {str(error)}")
        
        logger.info("=== ANÁLISE FINALIZADA ===")
        return resultado

    def _construir_filtro_mongo(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        """Constrói filtro MongoDB a partir dos parâmetros"""
        filtro_mongo = {}
        
        if filtros.get('id'):
            filtro_mongo['_id'] = filtros['id']
            return filtro_mongo
        
        # Filtros básicos
        if filtros.get('contratoCPP'):
            filtro_mongo['contratoCPP'] = filtros['contratoCPP']
        if filtros.get('campo'):
            filtro_mongo['campo'] = filtros['campo']
        if filtros.get('remessa'):
            filtro_mongo['remessa'] = int(filtros['remessa'])
        if filtros.get('exercicio'):
            filtro_mongo['exercicio'] = int(filtros['exercicio'])
        if filtros.get('periodo'):
            filtro_mongo['periodo'] = int(filtros['periodo'])
        if filtros.get('faseRemessa'):
            filtro_mongo['faseRemessa'] = filtros['faseRemessa']
        if filtros.get('origemDoGasto'):
            filtro_mongo['origemDoGasto'] = filtros['origemDoGasto']
        if filtros.get('etapa'):
            filtro_mongo['etapa'] = filtros['etapa']
        
        return filtro_mongo

    def _processar_remessa_resumida(self, remessa: Dict[str, Any]) -> Dict[str, Any]:
        """Processa remessa para exibição resumida"""
        gastos = remessa.get('gastos', [])
        
        # Calcular estatísticas básicas
        total_gastos = len(gastos)
        gastos_reconhecidos = len([g for g in gastos if g.get('reconhecido') == 'SIM'])
        valor_total = sum(converter_decimal128_para_float(g.get('valorMoedaOBJReal', 0)) for g in gastos)
        valor_reconhecido = sum(converter_decimal128_para_float(g.get('valorReconhecido', 0)) 
                               for g in gastos if g.get('reconhecido') == 'SIM')
        
        # Identificar fases com reconhecimento
        fases_reconhecimento = self._identificar_fases_reconhecimento_simples(gastos)
        
        return {
            'id': remessa['_id'],
            'contratoCPP': remessa.get('contratoCPP', ''),
            'campo': remessa.get('campo', ''),
            'remessa': remessa.get('remessa', 0),
            'remessaExposicao': remessa.get('remessaExposicao', 0),
            'exercicio': remessa.get('exercicio', 0),
            'periodo': remessa.get('periodo', 0),
            'mesAnoReferencia': remessa.get('mesAnoReferencia', ''),
            'faseRemessa': remessa.get('faseRemessa', ''),
            'etapa': remessa.get('etapa', ''),
            'origemDoGasto': remessa.get('origemDoGasto', ''),
            'gastosCompartilhados': remessa.get('gastosCompartilhados', False),
            'usuarioResponsavel': remessa.get('usuarioResponsavel', ''),
            'dataLancamento': formatar_data_brasileira(remessa.get('dataLancamento')),
            'version': remessa.get('version', 0),
            'totalGastos': total_gastos,
            'gastosReconhecidos': gastos_reconhecidos,
            'valorTotal': valor_total,
            'valorReconhecido': valor_reconhecido,
            'fasesComReconhecimento': len(fases_reconhecimento),
            'listaFasesReconhecimento': [f['fase'] for f in fases_reconhecimento]
        }

    def _inicializar_estatisticas(self) -> Dict[str, Any]:
        """Inicializa estrutura de estatísticas"""
        return {
            'totalRemessas': 0,
            'totalFasesEncontradas': 0,
            'totalCCOsEncontradas': 0,
            'totalCCOsNaoEncontradas': 0,
            'totalCCOsDuplicadas': 0,
            'fasesPorTipo': {},
            'consolidacaoGeral': {
                'totalGastos': 0,
                'valores': {
                    'lancamentoTotal': 0,
                    'reconhecido': 0,
                    'naoReconhecido': 0,
                    'recusado': 0,
                    'naoPassivelReconhecimento': 0,
                    'naoReconhecidoDecurso': 0
                },
                'contadores': {
                    'total': 0,
                    'reconhecido': 0,
                    'recAutomatico': 0,
                    'recDecurso': 0,
                    'reconhecidoParcial': 0,
                    'naoReconhecido': 0,
                    'naoReconhecidoPorDecurso': 0,
                    'recusados': 0,
                    'passivelRec': 0,
                    'naoPassivelRec': 0,
                    'outros': 0
                },
                'classificacoes': {},
                'moedas': {},
                'responsaveis': {}
            },
            'erros': []
        }

    def _executar_step1(self, filtros: Dict[str, Any], analise_detalhada: bool) -> List[Dict[str, Any]]:
        """Executa STEP 1: Identificar fases com reconhecimento"""
        logger.info("Executando consulta de remessas...")
        
        # Construir filtro incluindo gastos reconhecidos
        filtro_mongo = self._construir_filtro_mongo(filtros)
        filtro_mongo["gastos.reconhecido"] = "SIM"
        
        remessas = self.remessa_repo.buscar_remessas_com_reconhecimento(filtro_mongo)
        logger.info(f"Remessas encontradas: {len(remessas)}")
        
        remessas_com_fases = []
        total_processadas = 0
        
        for remessa in remessas:
            total_processadas += 1
            
            if total_processadas % 10 == 0:
                logger.info(f"Processadas: {total_processadas} remessas...")
            
            try:
                if analise_detalhada:
                    resultado_analise = self._analisar_gastos_detalhado(remessa.get('gastos', []))
                else:
                    resultado_analise = self._identificar_fases_reconhecimento(remessa.get('gastos', []))
                
                if resultado_analise['fases']:
                    remessa_completa = {
                        'id': remessa['_id'],
                        'fatorAlocacao': remessa.get('fatorAlocacao', 'INDEFINIDO'),
                        'exercicio': remessa.get('exercicio'),
                        'periodo': remessa.get('periodo'),
                        'contratoCPP': remessa.get('contratoCPP'),
                        'campo': remessa.get('campo'),
                        'faseRemessaAtual': remessa.get('faseRemessa'),
                        'remessa': remessa.get('remessa'),
                        'remessaExposicao': remessa.get('remessaExposicao'),
                        'mesAnoReferencia': remessa.get('mesAnoReferencia'),
                        'gastosCompartilhados': remessa.get('gastosCompartilhados'),
                        'origemDoGasto': remessa.get('origemDoGasto'),
                        'uep': remessa.get('uep', ''),
                        'version': remessa.get('version'),
                        'fasesComReconhecimento': resultado_analise['fases']
                    }
                    
                    if analise_detalhada and 'consolidacaoRemessa' in resultado_analise:
                        remessa_completa['consolidacaoRemessa'] = resultado_analise['consolidacaoRemessa']
                    
                    remessas_com_fases.append(remessa_completa)
                    
            except Exception as error:
                logger.error(f"Erro ao processar remessa {remessa['_id']}: {error}")
        
        return remessas_com_fases

    def _identificar_fases_reconhecimento(self, gastos: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Identifica fases com reconhecimento (análise simplificada)"""
        fases_validas = ['MEN', 'ROP', 'RAD', 'REC', 'REV']
        fases_encontradas = {}
        
        for gasto in gastos:
            if gasto.get('reconhecido') == 'SIM' and gasto.get('faseRespostaGestora'):
                fase_base = self._extrair_fase_base(gasto.get('faseRespostaGestora'))
                
                if fase_base in fases_validas or fase_base.startswith('REV'):
                    if fase_base not in fases_encontradas:
                        fases_encontradas[fase_base] = {
                            'fase': fase_base,
                            'faseOriginal': gasto.get('faseRespostaGestora'),
                            'dataReconhecimento': self._formatar_data_para_iso(gasto.get('dataReconhecimento')),
                            'dataLancamento': self._formatar_data_para_iso(gasto.get('dataLancamento')),
                            'valorReconhecido': converter_decimal128_para_float(gasto.get('valorReconhecido', 0)),
                            'primeiroResponsavel': gasto.get('responsavel', ''),
                            'reconhecimentoTipo': gasto.get('reconhecimentoTipo', '')
                        }
        
        # Converter para array ordenado
        fases_array = list(fases_encontradas.values())
        fases_array.sort(key=lambda x: self._ordem_fase(x['fase']))
        
        return {'fases': fases_array}

    def _identificar_fases_reconhecimento_simples(self, gastos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Versão simplificada para listagem de remessas"""
        fases_validas = ['MEN', 'ROP', 'RAD', 'REC', 'REV']
        fases_encontradas = set()
        
        for gasto in gastos:
            if gasto.get('reconhecido') == 'SIM' and gasto.get('faseRespostaGestora'):
                fase_base = self._extrair_fase_base(gasto.get('faseRespostaGestora'))
                if fase_base in fases_validas or fase_base.startswith('REV'):
                    fases_encontradas.add(fase_base)
        
        return [{'fase': fase} for fase in sorted(fases_encontradas, key=self._ordem_fase)]

    def _analisar_gastos_detalhado(self, gastos: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Análise detalhada dos gastos (equivalente ao script JS)"""
        logger.info("Executando análise detalhada dos gastos...")
        
        fases_validas = ['MEN', 'ROP', 'RAD', 'REC', 'REV']
        fases_dados = {}
        
        # Inicializar consolidação da remessa
        consolidacao_remessa = self._inicializar_consolidacao()
        
        # Processar todos os gastos
        for gasto in gastos:
            if gasto.get('reconhecido') == 'SIM' and gasto.get('faseRespostaGestora'):
                fase_base = self._extrair_fase_base(gasto.get('faseRespostaGestora'))
                
                if fase_base in fases_validas or fase_base.startswith('REV'):
                    if fase_base not in fases_dados:
                        fases_dados[fase_base] = {
                            'fase': fase_base,
                            'faseOriginal': gasto.get('faseRespostaGestora'),
                            'dataReconhecimento': self._formatar_data_para_iso(gasto.get('dataReconhecimento')),
                            'dataLancamento': self._formatar_data_para_iso(gasto.get('dataLancamento')),
                            'consolidacao': self._inicializar_consolidacao()
                        }
                    
                    self._atualizar_contadores(gasto, fases_dados[fase_base]['consolidacao'])
            
            # Sempre atualizar consolidação da remessa
            self._atualizar_contadores(gasto, consolidacao_remessa)
        
        # Converter para array ordenado
        fases_array = list(fases_dados.values())
        fases_array.sort(key=lambda x: self._ordem_fase(x['fase']))
        
        return {
            'fases': fases_array,
            'consolidacaoRemessa': consolidacao_remessa
        }

    def _executar_steps2e3(self, remessas: List[Dict[str, Any]], estatisticas: Dict[str, Any], analise_detalhada: bool) -> List[Dict[str, Any]]:
        """Executa STEPS 2 e 3: Analisar CCOs e complementar dados"""
        logger.info("Buscando CCOs correspondentes...")
        
        remessas_completas = []
        total_fases_analisadas = 0
        
        for index_remessa, remessa in enumerate(remessas):
            if (index_remessa + 1) % 5 == 0:
                logger.info(f"Processando remessa {index_remessa + 1} de {len(remessas)}")
            
            try:
                for fase_info in remessa['fasesComReconhecimento']:
                    total_fases_analisadas += 1
                    estatisticas['totalFasesEncontradas'] += 1
                    
                    # Contabilizar fases por tipo
                    fase_nome = fase_info['fase']
                    if fase_nome not in estatisticas['fasesPorTipo']:
                        estatisticas['fasesPorTipo'][fase_nome] = 0
                    estatisticas['fasesPorTipo'][fase_nome] += 1
                    
                    # Buscar CCO correspondente
                    ccos_encontradas = self._buscar_cco_correspondente(remessa, fase_nome)
                    
                    if ccos_encontradas and len(ccos_encontradas) > 0:
                        if len(ccos_encontradas) > 1:
                            # CCOs duplicadas
                            ids_ccos = " | ".join([cco['_id'] for cco in ccos_encontradas])
                            fase_info['cco'] = {
                                'id': ids_ccos,
                                'statusCCO': 'CCOS_DUPLICADAS',
                                'observacao': f'CCOs duplicadas para fase {fase_nome}'
                            }
                            estatisticas['totalCCOsDuplicadas'] += 1
                        else:
                            # CCO única encontrada
                            cco_encontrada = ccos_encontradas[0]
                            fase_info['cco'] = self._processar_cco_encontrada(cco_encontrada)
                            estatisticas['totalCCOsEncontradas'] += 1
                    else:
                        # CCO não encontrada
                        fase_info['cco'] = {
                            'id': '',
                            'statusCCO': 'NAO_ENCONTRADA',
                            'observacao': f'CCO não encontrada para fase {fase_nome}'
                        }
                        estatisticas['totalCCOsNaoEncontradas'] += 1
                
                remessas_completas.append(remessa)
                
                # Consolidar estatísticas gerais para análise detalhada
                if analise_detalhada and 'consolidacaoRemessa' in remessa:
                    self._consolidar_estatisticas_gerais(
                        remessa['consolidacaoRemessa'], 
                        estatisticas['consolidacaoGeral']
                    )
                    
            except Exception as error:
                logger.error(f"Erro ao processar CCOs da remessa {remessa['id']}: {error}")
                estatisticas['erros'].append(f"Erro na remessa {remessa['remessa']}: {str(error)}")
        
        logger.info(f"Total de fases analisadas: {total_fases_analisadas}")
        return remessas_completas

    def _buscar_cco_correspondente(self, remessa: Dict[str, Any], fase: str) -> Optional[List[Dict[str, Any]]]:
        """Busca CCO correspondente para a remessa e fase"""
        try:
            filtros = {
                'contratoCpp': remessa['contratoCPP'],
                'campo': remessa['campo'],
                'origemDosGastos': remessa['origemDoGasto'],
                'remessa': remessa['remessa'],
                'faseRemessa': fase
            }
            
            ccos_encontradas = self.cco_repo.buscar_por_filtros(filtros)
            
            if len(ccos_encontradas) > 1:
                logger.warning(f"Múltiplas CCOs encontradas para remessa {remessa['remessa']} fase {fase}: {len(ccos_encontradas)}")
            
            return ccos_encontradas if ccos_encontradas else None
            
        except Exception as error:
            logger.error(f"Erro ao buscar CCO para remessa {remessa['remessa']} fase {fase}: {error}")
            return None

    def _processar_cco_encontrada(self, cco: Dict[str, Any]) -> Dict[str, Any]:
        """Processa CCO encontrada extraindo informações relevantes"""
        from app.utils.converters import converter_decimal128_para_float
        
        # Verificar se tem correções monetárias
        correcoes = cco.get('correcoesMonetarias', [])
        if correcoes:
            # Usar última correção
            correcao = correcoes[-1]
            return {
                'id': str(cco['_id']),
                'tipo': correcao.get('tipo', ''),
                'subTipo': correcao.get('subTipo', ''),
                'contrato': correcao.get('contrato', ''),
                'campo': correcao.get('campo', ''),
                'dataCorrecao': self._formatar_data_para_iso(correcao.get('dataCorrecao')),
                'dataCriacaoCorrecao': self._formatar_data_para_iso(correcao.get('dataCriacaoCorrecao')),
                'valorReconhecido': converter_decimal128_para_float(correcao.get('valorReconhecido', 0)),
                'valorReconhecidoComOH': converter_decimal128_para_float(correcao.get('valorReconhecidoComOH', 0)),
                'overHeadTotal': converter_decimal128_para_float(correcao.get('overHeadTotal', 0)),
                'diferencaValor': converter_decimal128_para_float(correcao.get('diferencaValor', 0)),
                'taxaCorrecao': converter_decimal128_para_float(correcao.get('taxaCorrecao', 0)),
                'igpmAcumulado': converter_decimal128_para_float(correcao.get('igpmAcumulado', 0)),
                'igpmAcumuladoReais': converter_decimal128_para_float(correcao.get('igpmAcumuladoReais', 0)),
                'statusCCO': 'ENCONTRADA',
                'flgRecuperado': bool(cco.get('flgRecuperado', False))
            }
        else:
            # Usar valores da CCO raiz
            return {
                'id': str(cco['_id']),
                'tipo': cco.get('tipo', ''),
                'subTipo': cco.get('subTipo', ''),
                'valorReconhecido': converter_decimal128_para_float(cco.get('valorReconhecido', 0)),
                'valorReconhecidoComOH': converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0)),
                'overHeadTotal': converter_decimal128_para_float(cco.get('overHeadTotal', 0)),
                'statusCCO': 'ENCONTRADA',
                'flgRecuperado': bool(cco.get('flgRecuperado', False))
            }

    def _inicializar_consolidacao(self) -> Dict[str, Any]:
        """Inicializa estrutura de consolidação"""
        return {
            'valores': {
                'lancamentoTotal': 0,
                'reconhecido': 0,
                'naoReconhecido': 0,
                'recusado': 0,
                'naoPassivelReconhecimento': 0,
                'naoReconhecidoDecurso': 0
            },
            'contadores': {
                'total': 0,
                'reconhecido': 0,
                'recAutomatico': 0,
                'recDecurso': 0,
                'reconhecidoParcial': 0,
                'naoReconhecido': 0,
                'naoReconhecidoPorDecurso': 0,
                'recusados': 0,
                'passivelRec': 0,
                'naoPassivelRec': 0,
                'outros': 0
            },
            'classificacoes': {},
            'moedas': {},
            'responsaveis': {}
        }

    def _atualizar_contadores(self, gasto: Dict[str, Any], consolidacao: Dict[str, Any]):
        """Atualiza contadores e valores da consolidação"""
        consolidacao['contadores']['total'] += 1
        
        # Consolidações por categoria
        classificacao = gasto.get('classificacaoGastoTipo', 'INDEFINIDO')
        moeda = gasto.get('moedaTransacao', 'INDEFINIDO') 
        responsavel = gasto.get('responsavel', 'INDEFINIDO')
        
        consolidacao['classificacoes'][classificacao] = consolidacao['classificacoes'].get(classificacao, 0) + 1
        consolidacao['moedas'][moeda] = consolidacao['moedas'].get(moeda, 0) + 1
        consolidacao['responsaveis'][responsavel] = consolidacao['responsaveis'].get(responsavel, 0) + 1
        
        # Processar valores por status
        status = gasto.get('statusGastoTipo')
        reconhecimento_tipo = gasto.get('reconhecimentoTipo')
        
        valor_reconhecido = converter_decimal128_para_float(gasto.get('valorReconhecido', 0))
        valor_nao_reconhecido = converter_decimal128_para_float(gasto.get('valorNaoReconhecido', 0))
        valor_total = converter_decimal128_para_float(gasto.get('valorMoedaOBJReal', 0))
        
        if status == 'Reconhecido':
            consolidacao['valores']['reconhecido'] += valor_reconhecido
            if reconhecimento_tipo == 'TOTAL_AUTOMATICO':
                consolidacao['contadores']['recAutomatico'] += 1
            elif reconhecimento_tipo == 'TOTAL_POR_DECURSO_DE_PRAZO':
                consolidacao['contadores']['recDecurso'] += 1
            elif reconhecimento_tipo == 'PARCIAL':
                consolidacao['contadores']['reconhecidoParcial'] += 1
                consolidacao['valores']['naoReconhecido'] += valor_nao_reconhecido
            elif reconhecimento_tipo == 'TOTAL':
                consolidacao['contadores']['reconhecido'] += 1
        elif status == 'Nao_Reconhecido':
            consolidacao['valores']['naoReconhecido'] += valor_nao_reconhecido
            if reconhecimento_tipo == 'TOTAL_POR_DECURSO_DE_PRAZO':
                consolidacao['contadores']['naoReconhecidoPorDecurso'] += 1
                consolidacao['valores']['naoReconhecidoDecurso'] += valor_nao_reconhecido
            else:
                consolidacao['contadores']['naoReconhecido'] += 1
        elif status == 'Recusado':
            consolidacao['contadores']['recusados'] += 1
            consolidacao['valores']['recusado'] += valor_total
        elif status == 'Passivel_Reconhecimento':
            consolidacao['contadores']['passivelRec'] += 1
        elif status == 'Nao_Passivel_Reconhecimento':
            consolidacao['contadores']['naoPassivelRec'] += 1
            consolidacao['valores']['naoPassivelReconhecimento'] += valor_total
        else:
            consolidacao['contadores']['outros'] += 1
        
        # Valor de lançamento total (exceto recusados)
        if status != 'Recusado':
            consolidacao['valores']['lancamentoTotal'] += valor_total

    def _consolidar_estatisticas_gerais(self, consolidacao_remessa: Dict[str, Any], consolidacao_geral: Dict[str, Any]):
        """Consolida estatísticas gerais a partir da consolidação da remessa"""
        consolidacao_geral['totalGastos'] += consolidacao_remessa['contadores']['total']
        
        # Somar valores
        for chave, valor in consolidacao_remessa['valores'].items():
            consolidacao_geral['valores'][chave] = consolidacao_geral['valores'].get(chave, 0) + valor
        
        # Somar contadores
        for chave, valor in consolidacao_remessa['contadores'].items():
            consolidacao_geral['contadores'][chave] = consolidacao_geral['contadores'].get(chave, 0) + valor
        
        # Consolidar classificações, moedas e responsáveis
        for categoria in ['classificacoes', 'moedas', 'responsaveis']:
            for chave, valor in consolidacao_remessa[categoria].items():
                consolidacao_geral[categoria][chave] = consolidacao_geral[categoria].get(chave, 0) + valor

    def _extrair_fase_base(self, fase_resposta_gestora: str) -> str:
        """Extrai fase base da resposta gestora"""
        if not fase_resposta_gestora:
            return ''
        
        # Para REV com sufixo, manter apenas REV
        if fase_resposta_gestora.startswith('REV'):
            return 'REV'
        
        fases_validas = ['MEN', 'ROP', 'RAD', 'REC']
        if fase_resposta_gestora not in fases_validas:
            # Tentar extrair por match
            for fase in fases_validas:
                if fase in fase_resposta_gestora:
                    return fase
            logger.warning(f"Fase não correspondente: {fase_resposta_gestora}")
        
        return fase_resposta_gestora

    def _ordem_fase(self, fase: str) -> int:
        """Retorna ordem numérica para ordenação de fases"""
        ordem_fases = {'MEN': 1, 'ROP': 2, 'RAD': 3, 'REC': 4}
        
        if fase in ordem_fases:
            return ordem_fases[fase]
        elif fase.startswith('REV'):
            return 5
        else:
            return 6

    def _formatar_data_para_iso(self, data: Any) -> str:
        """Formata data para ISO string"""
        if not data:
            return ''
        
        try:
            if isinstance(data, datetime):
                return data.isoformat()
            elif isinstance(data, str):
                # Tentar converter string para datetime
                try:
                    dt = datetime.fromisoformat(data.replace('Z', '+00:00'))
                    return dt.isoformat()
                except:
                    return data
            elif hasattr(data, 'isoformat'):
                return data.isoformat()
            else:
                return str(data)
        except Exception as e:
            logger.warning(f"Erro ao formatar data {data}: {e}")
            return str(data) if data else ''

    def _gerar_relatorio_estatisticas(self, estatisticas: Dict[str, Any]):
        """Gera relatório de estatísticas"""
        logger.info("=== RELATÓRIO DE ESTATÍSTICAS ===")
        logger.info(f"Total de remessas analisadas: {estatisticas['totalRemessas']}")
        logger.info(f"Total de fases encontradas: {estatisticas['totalFasesEncontradas']}")
        logger.info(f"Total de CCOs encontradas: {estatisticas['totalCCOsEncontradas']}")
        logger.info(f"Total de CCOs NÃO encontradas: {estatisticas['totalCCOsNaoEncontradas']}")
        logger.info(f"Total de CCOs duplicadas: {estatisticas['totalCCOsDuplicadas']}")
        
        if estatisticas['totalFasesEncontradas'] > 0:
            percentual = (estatisticas['totalCCOsEncontradas'] / estatisticas['totalFasesEncontradas'] * 100)
            logger.info(f"Percentual de CCOs encontradas: {percentual:.2f}%")
        
        logger.info("--- Distribuição de Fases ---")
        for fase, count in estatisticas['fasesPorTipo'].items():
            logger.info(f"{fase}: {count}")
        
        if estatisticas['erros']:
            logger.info("--- Erros Encontrados ---")
            for i, erro in enumerate(estatisticas['erros'], 1):
                logger.info(f"{i}. {erro}")

    def obter_contratos_disponiveis(self) -> List[str]:
        """Obtém lista de contratos disponíveis"""
        try:
            contratos = self.remessa_repo.obter_valores_distintos('contratoCPP')
            return sorted([c for c in contratos if c])
        except Exception as e:
            logger.error(f"Erro ao obter contratos: {e}")
            return []

    def obter_campos_por_contrato(self, contrato: str) -> List[str]:
        """Obtém lista de campos por contrato"""
        try:
            campos = self.remessa_repo.obter_valores_distintos('campo', {'contratoCPP': contrato})
            return sorted([c for c in campos if c])
        except Exception as e:
            logger.error(f"Erro ao obter campos: {e}")
            return []

    def obter_etapas_disponiveis(self) -> List[str]:
        """Obtém lista de etapas disponíveis"""
        try:
            etapas = self.remessa_repo.obter_valores_distintos('etapa')
            return sorted([e for e in etapas if e])
        except Exception as e:
            logger.error(f"Erro ao obter etapas: {e}")
            return []