"""
Serviço para recálculo de CCOs
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from decimal import Decimal
from pymongo import MongoClient
from pymongo.collection import Collection
from bson import ObjectId
from bson.int64 import Int64
from bson.decimal128 import Decimal128

from app.repositories.cco_repository import CCORepository
from app.utils.converters import converter_decimal128_para_float, validar_e_converter_valor_monetario

logger = logging.getLogger(__name__)

class TipoRecalculo:
    """Enum para tipos de recálculo disponíveis"""
    TRACK_PARTICIPATION = "TRACK_PARTICIPATION"
    AJUSTE_IPCA = "AJUSTE_IPCA"
    AJUSTE_IGPM = "AJUSTE_IGPM"
    AJUSTE_MANUAL = "AJUSTE_MANUAL"

class ModoRecalculo:
    """Enum para modos de recálculo"""
    COMPLETO = "COMPLETO"
    CORRECAO_MONETARIA = "CORRECAO_MONETARIA"

class RecalculoService:
    """Serviço principal para recálculo de CCOs"""
    
    def __init__(self, mongo_uri_local: str, mongo_uri: str = None):
        self.client = MongoClient(mongo_uri)
        self.db_prd = self.client.sgppServices
        self.cco_repo = CCORepository(self.db_prd)
        
        # Conexão local para salvar resultados temporários
        self.mongo_uri_local = mongo_uri_local or "mongodb://localhost:27017/"
        self.client_local = MongoClient(self.mongo_uri_local)
        self.db_local = self.client_local.temp_recalculos

    def pesquisar_ccos_para_recalculo(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pesquisa CCOs disponíveis para recálculo baseado em filtros
        """
        try:
            logger.info(f"Pesquisando CCOs para recálculo com filtros: {filtros}")
            
            filtro_mongo = self._construir_filtro_pesquisa(filtros)
            
            # Buscar CCOs com projeção específica para listagem
            projecao = {
                '_id': 1,
                'contratoCpp': 1,
                'campo': 1,
                'remessa': 1,
                'faseRemessa': 1,
                'exercicio': 1,
                'periodo': 1,
                'mesAnoReferencia': 1,
                'valorReconhecido': 1,
                'valorReconhecidoComOH': 1,
                'overHeadTotal': 1,
                'flgRecuperado': 1,
                'dataReconhecimento': 1
            }
            
            ccos = list(self.cco_repo.buscar_por_filtros(filtro_mongo, projecao))
            
            # Processar resultados para listagem
            resultados = []
            for cco in ccos:
                resultado = self._processar_cco_para_listagem(cco)
                resultados.append(resultado)
            
            return {
                'success': True,
                'resultados': resultados,
                'total': len(resultados),
                'filtro_aplicado': filtro_mongo
            }
            
        except Exception as e:
            logger.error(f"Erro ao pesquisar CCOs: {e}")
            return {'success': False, 'error': str(e)}

    def executar_recalculo_tp(self, cco_id: str, tp_original: float, tp_correcao: float, 
                             modo: str = ModoRecalculo.COMPLETO, observacoes: str = "") -> Dict[str, Any]:
        """
        Executa recálculo de Track Participation para uma CCO específica
        """
        try:
            logger.info(f"Iniciando recálculo TP para CCO {cco_id}")
            logger.info(f"TP Original: {tp_original}, TP Correção: {tp_correcao}, Modo: {modo}")
            
            # Buscar CCO original
            cco_original = self.cco_repo.buscar_por_id(cco_id)
            if not cco_original:
                return {'success': False, 'error': 'CCO não encontrada'}
            
            # Validar TPs
            if tp_original <= 0 or tp_correcao <= 0:
                return {'success': False, 'error': 'TPs devem ser maiores que zero'}
            
            # Calcular fator de correção
            fator_correcao = tp_correcao / tp_original
            
            # Capturar estado original antes de qualquer alteração
            #cco_estado_original = self._capturar_estado_original(cco_original)
            cco_estado_original = cco_original.copy()

            # Preparar metadata
            metadata = {
                'tp_original': tp_original,
                'tp_correcao': tp_correcao
            }

            # Executar recálculo baseado no modo
            if modo == ModoRecalculo.COMPLETO:
                cco_recalculada, ultimo_valor_reconhecido_com_oh = self._executar_recalculo_completo(cco_estado_original, fator_correcao)
            elif modo == ModoRecalculo.CORRECAO_MONETARIA:
                cco_recalculada, ultimo_valor_reconhecido_com_oh = self._executar_correcao_monetaria(cco_estado_original, fator_correcao, metadata)
            else:
                return {'success': False, 'error': f'Modo de recálculo inválido: {modo}'}
            
            # Adicionar metadados do recálculo
            metadata_recalculo = {
                'tipo_recalculo': TipoRecalculo.TRACK_PARTICIPATION,
                'modo_recalculo': modo,
                'tp_original': tp_original,
                'tp_correcao': tp_correcao,
                'fator_correcao': fator_correcao,
                'observacoes': observacoes,
                'data_recalculo': datetime.now(),
                'usuario': 'system'  # TODO: pegar do contexto da sessão
            }
            
            cco_recalculada['metadata_recalculo'] = metadata_recalculo
            
            # Preparar resultado comparativo
            resultado = self._preparar_resultado_comparativo(cco_original, cco_recalculada, metadata_recalculo)
            resultado['ultimo_valor_reconhecido_com_oh'] = converter_decimal128_para_float(ultimo_valor_reconhecido_com_oh) if ultimo_valor_reconhecido_com_oh is not None else 0
            
            return {
                'success': True,
                'resultado': resultado
            }
            
        except Exception as e:
            logger.error(f"Erro ao executar recálculo TP: {e}")
            return {'success': False, 'error': str(e)}

    def salvar_resultado_temporario(self, resultado_recalculo: Dict[str, Any]) -> Dict[str, Any]:
        """
        Salva resultado do recálculo em coleção temporária
        """
        try:
            cco_id = resultado_recalculo['cco_original']['_id']
            
            # Preparar documento para salvar
            documento_temp = {
                '_id': ObjectId(),
                'cco_original_id': cco_id,
                'cco_original': resultado_recalculo['cco_original'],
                'cco_recalculada': resultado_recalculo['cco_recalculada'],
                'metadata_recalculo': resultado_recalculo['metadata_recalculo'],
                'comparativo': resultado_recalculo['comparativo'],
                'data_criacao': datetime.now(),
                'status': 'TEMPORARIO'
            }
            
            # Salvar na coleção temporária
            resultado_insert = self.db_local.ccos_recalculadas.insert_one(documento_temp)
            
            # Criar evento temporário
            evento_temp = {
                '_id': ObjectId(),
                'cco_recalculada_id': resultado_insert.inserted_id,
                'cco_original_id': cco_id,
                'tipo_evento': 'RECALCULO_TEMPORARIO',
                'metadata_recalculo': resultado_recalculo['metadata_recalculo'],
                'data_evento': datetime.now()
            }
            
            self.db_local.eventos_recalculo.insert_one(evento_temp)
            
            return {
                'success': True,
                'id_temporario': str(resultado_insert.inserted_id),
                'message': 'Resultado salvo em coleção temporária'
            }
            
        except Exception as e:
            logger.error(f"Erro ao salvar resultado temporário: {e}")
            return {'success': False, 'error': str(e)}

    def aplicar_recalculo_definitivo(self, id_temporario: str) -> Dict[str, Any]:
        """
        Aplica o recálculo definitivamente na base principal
        """
        try:
            # Buscar resultado temporário
            documento_temp = self.db_local.ccos_recalculadas.find_one({'_id': ObjectId(id_temporario)})
            if not documento_temp:
                return {'success': False, 'error': 'Resultado temporário não encontrado'}
            
            cco_recalculada = documento_temp['cco_recalculada']
            metadata_recalculo = documento_temp['metadata_recalculo']
            
            # Atualizar CCO na base principal
            resultado_update = self._atualizar_cco_e_criar_evento(
                cco_recalculada, 
                metadata_recalculo['observacoes']
            )
            
            if resultado_update['success']:
                # Marcar como aplicado na base temporária
                self.db_local.ccos_recalculadas.update_one(
                    {'_id': ObjectId(id_temporario)},
                    {'$set': {'status': 'APLICADO', 'data_aplicacao': datetime.now()}}
                )
                
                return {
                    'success': True,
                    'nova_versao': resultado_update['nova_versao'],
                    'message': 'Recálculo aplicado com sucesso'
                }
            else:
                return resultado_update
                
        except Exception as e:
            logger.error(f"Erro ao aplicar recálculo definitivo: {e}")
            return {'success': False, 'error': str(e)}

    def listar_recalculos_temporarios(self) -> List[Dict[str, Any]]:
        """
        Lista todos os recálculos temporários salvos
        """
        try:
            documentos = list(self.db_local.ccos_recalculadas.find(
                {'status': 'TEMPORARIO'},
                {'cco_original._id': 1, 'cco_original.contratoCpp': 1, 'cco_original.campo': 1, 'cco_original.remessa': 1,
                 'metadata_recalculo': 1, 'data_criacao': 1}
            ).sort('data_criacao', -1))
            
            resultados = []
            for doc in documentos:
                resultado = {
                    'id': str(doc['_id']),
                    'idCCO': doc['cco_original']['_id'],
                    'contrato': doc['cco_original'].get('contratoCpp', ''),
                    'campo': doc['cco_original'].get('campo', ''),
                    'remessa': doc['cco_original'].get('remessa', 0),
                    'tipo_recalculo': doc['metadata_recalculo']['tipo_recalculo'],
                    'modo_recalculo': doc['metadata_recalculo']['modo_recalculo'],
                    'data_criacao': doc['data_criacao'],
                    'observacoes': doc['metadata_recalculo'].get('observacoes', '')
                }
                resultados.append(resultado)
            
            return resultados
            
        except Exception as e:
            logger.error(f"Erro ao listar recálculos temporários: {e}")
            return []

    def _construir_filtro_pesquisa(self, filtros: Dict[str, Any]) -> Dict[str, Any]:
        """Constrói filtro MongoDB para pesquisa de CCOs"""
        filtro_mongo = {}
        
        if filtros.get('id'):
            filtro_mongo['_id'] = filtros['id']
            return filtro_mongo
        
        if filtros.get('contratoCpp'):
            filtro_mongo['contratoCpp'] = filtros['contratoCpp']
        if filtros.get('campo'):
            filtro_mongo['campo'] = filtros['campo']
        if filtros.get('remessa'):
            filtro_mongo['remessa'] = int(filtros['remessa'])
        if filtros.get('faseRemessa'):
            filtro_mongo['faseRemessa'] = filtros['faseRemessa']
        if filtros.get('exercicio'):
            filtro_mongo['exercicio'] = int(filtros['exercicio'])
        if filtros.get('periodo'):
            filtro_mongo['periodo'] = int(filtros['periodo'])
        if filtros.get('origemDosGastos'):
            filtro_mongo['origemDosGastos'] = filtros['origemDosGastos']
            
        return filtro_mongo

    def _processar_cco_para_listagem(self, cco: Dict[str, Any]) -> Dict[str, Any]:
        """Processa CCO para exibição em listagem"""
        return {
            'id': str(cco['_id']),
            'contratoCpp': cco.get('contratoCpp', ''),
            'campo': cco.get('campo', ''),
            'remessa': cco.get('remessa', 0),
            'faseRemessa': cco.get('faseRemessa', ''),
            'exercicio': cco.get('exercicio', 0),
            'periodo': cco.get('periodo', 0),
            'mesAnoReferencia': cco.get('mesAnoReferencia', ''),
            'valorReconhecido': converter_decimal128_para_float(cco.get('valorReconhecido', 0)),
            'valorReconhecidoComOH': converter_decimal128_para_float(cco.get('valorReconhecidoComOH', 0)),
            'overHeadTotal': converter_decimal128_para_float(cco.get('overHeadTotal', 0)),
            'flgRecuperado': cco.get('flgRecuperado', False),
            'dataReconhecimento': cco.get('dataReconhecimento', '')
        }

    def _executar_recalculo_completo(self, cco_original: Dict[str, Any], fator_correcao: float) -> Dict[str, Any]:
        """Executa recálculo completo aplicando fator de correção em todos os valores"""
        cco_recalculada = cco_original.copy()
        
        # Campos que serão recalculados
        campos_monetarios = [
            'valorReconhecido', 'valorReconhecidoComOH', 'overHeadTotal',
            'valorLancamento', 'quantidadeLancamento'
        ]
        
        for campo in campos_monetarios:
            if campo in cco_recalculada:
                valor_original = converter_decimal128_para_float(cco_recalculada[campo])
                valor_recalculado = valor_original * fator_correcao
                cco_recalculada[campo] = Decimal128(str(round(valor_recalculado, 2)))
        
        # TODO Aplicar quando for implementar recalcular gastos nas remessas
        # Recalcular gastos
        # if 'gastos' in cco_recalculada and isinstance(cco_recalculada['gastos'], list):
        #     for gasto in cco_recalculada['gastos']:
        #         if 'valorConvertido' in gasto:
        #             valor_original = converter_decimal128_para_float(gasto['valorConvertido'])
        #             gasto['valorConvertido'] = Decimal128(str(round(valor_original * fator_correcao, 2)))
        
        # Recalcular correções monetárias
        if 'correcoesMonetarias' in cco_recalculada and isinstance(cco_recalculada['correcoesMonetarias'], list):
            for correcao in cco_recalculada['correcoesMonetarias']:
                for campo in ['valorReconhecido', 'valorReconhecidoComOH', 'overHeadTotal']:
                    if campo in correcao:
                        valor_original = converter_decimal128_para_float(correcao[campo])
                        correcao[campo] = Decimal128(str(round(valor_original * fator_correcao, 15)))
        
        return cco_recalculada

    def _obter_valores_atuais(self, cco_doc: Dict[str, Any]) -> Tuple[Dict[str, Any], Any]:
        """
        Obtém valores atuais da CCO (da última correção ou da raiz)
        """
        valores = {}
        ultimo_valor_reconhecido_com_oh = None
        
        # Campos que podem estar nas correções monetárias
        campos_correcoes = [
            'valorReconhecido', 'valorReconhecidoComOH', 'overHeadTotal',
            'valorLancamentoTotal', 'quantidadeLancamento', 'valorNaoReconhecido',
            'valorReconhecivel', 'valorNaoPassivelRecuperacao', 'valorReconhecidoExploracao',
            'valorReconhecidoProducao', 'overHeadExploracao', 'overHeadProducao'
        ]
        
        # Se tem correções monetárias, usar a última
        if 'correcoesMonetarias' in cco_doc and cco_doc['correcoesMonetarias']:
            fonte = cco_doc['correcoesMonetarias'][-1]
            logger.info(f"Usando valores da última correção: {fonte.get('tipo', 'N/A')}")
        else:
            fonte = cco_doc
            logger.info("Usando valores da raiz da CCO")
        
        # Encontrar último valorReconhecidoComOH válido
        if 'correcoesMonetarias' in cco_doc and cco_doc['correcoesMonetarias']:
            for correcao in reversed(cco_doc['correcoesMonetarias']):
                if 'valorReconhecidoComOH' in correcao:
                    valor_com_oh = converter_decimal128_para_float(correcao['valorReconhecidoComOH'])
                    if valor_com_oh != 0:
                        if valor_com_oh < 0:
                            logger.warning("ATENÇÃO: Correção monetária encontrada com valorReconhecidoComOH negativo")
                        ultimo_valor_reconhecido_com_oh = correcao['valorReconhecidoComOH']
                        logger.info(f"Usando valorReconhecidoComOH da correção: {ultimo_valor_reconhecido_com_oh}")
                        break
        
        if ultimo_valor_reconhecido_com_oh is None:
            ultimo_valor_reconhecido_com_oh = cco_doc.get('valorReconhecidoComOH')
            logger.info(f"Usando valorReconhecidoComOH da raiz da CCO: {ultimo_valor_reconhecido_com_oh}")
        
        # Extrair campos relevantes
        for campo in campos_correcoes:
            if campo in fonte:
                valores[campo] = fonte[campo]
            elif campo in cco_doc:  # Fallback para raiz se não estiver na correção
                valores[campo] = cco_doc[campo]
            else:
                valores[campo] = Decimal128('0')
        
        return valores, ultimo_valor_reconhecido_com_oh
    def _executar_correcao_monetaria(self, cco_original: Dict[str, Any], fator_correcao: float, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Executa correção monetária seguindo padrão do script original"""
        import copy as copy
        cco_recalculada = copy.deepcopy(cco_original)
        
        # Obter valores atuais (da última correção ou da raiz)
        valores_atuais, ultimo_valor_reconhecido_com_oh = self._obter_valores_atuais(cco_recalculada)
        
        # Calcular novos valores baseado nos valores atuais
        novo_valor_reconhecido = converter_decimal128_para_float(valores_atuais['valorReconhecido']) * fator_correcao
        
        
        novo_overhead_total = converter_decimal128_para_float(valores_atuais['overHeadTotal']) * fator_correcao

        # Lógica específica para valorReconhecidoComOH baseada em flgRecuperado
        valor_original_com_oh = converter_decimal128_para_float(ultimo_valor_reconhecido_com_oh) if ultimo_valor_reconhecido_com_oh else 0
        valor_original_para_correcao = valor_original_com_oh # TODO validar
        
        valor_reconhecido_com_oh_recalculado = valor_original_com_oh * fator_correcao#TODO validar
        diferenca_valor_reconhecido_com_oh = valor_reconhecido_com_oh_recalculado - valor_original_com_oh#TODO validar
        valor_com_oh_correcao = converter_decimal128_para_float(valores_atuais['valorReconhecidoComOH']) * fator_correcao # TODO validar
        diferenca_valor = 0 # TODO validar   

        if ultimo_valor_reconhecido_com_oh is not None and cco_original.get('flgRecuperado', False):
            # CCO Recuperada: valorReconhecidoComOH na correção = diferença
            logger.info(f"### Usando valorReconhecidoComOH anterior para calcular diferença: {ultimo_valor_reconhecido_com_oh}")
            valor_reconhecido_com_oh_recalculado = valor_original_com_oh * fator_correcao
            logger.info(f"### Valor reconhecido com OH recalculado: {valor_reconhecido_com_oh_recalculado}")
            diferenca_valor_reconhecido_com_oh = valor_reconhecido_com_oh_recalculado - valor_original_com_oh
            logger.info(f"### Diferença valor reconhecido com OH: {diferenca_valor_reconhecido_com_oh}")
            
            # Para CCO recuperada, valorReconhecidoComOH na correção = diferença
            valor_com_oh_correcao = diferenca_valor_reconhecido_com_oh
            diferenca_valor = diferenca_valor_reconhecido_com_oh
            valor_original_para_correcao = valor_original_com_oh
            
            # Valor total da CCO será atualizado com a diferença aplicada
            novo_valor_com_oh_total = valor_original_com_oh + diferenca_valor_reconhecido_com_oh
            
        #else:
            
            # CCO Não Recuperada: cálculo padrão
            # novo_valor_com_oh_total = novo_valor_reconhecido + novo_overhead_total
            # diferenca_valor = novo_valor_com_oh_total - valor_original_com_oh
            # valor_com_oh_correcao = diferenca_valor
            # valor_original_para_correcao = valor_original_com_oh
        
        # Obter dados da última correção para herdar valores específicos
        ultima_correcao = None
        if 'correcoesMonetarias' in cco_original and cco_original['correcoesMonetarias']:
            ultima_correcao = cco_original['correcoesMonetarias'][-1]
        
        # Criar nova correção monetária
        nova_correcao = {
            "tipo": "RETIFICACAO",
            "subTipo": f"TP_CORRECTION_{metadata['tp_original']}_TO_{metadata['tp_correcao']}",
            "dataCorrecao": datetime.now().isoformat(),
            "dataCriacaoCorrecao": datetime.now(),
            "ativo": True,
            "observacao": f"Retificação automática TP de {metadata['tp_original']} para {metadata['tp_correcao']}",
            "taxaCorrecao": Decimal128("0"),
            "igpmAcumulado": ultima_correcao.get('igpmAcumulado', Decimal128("1.0")) if ultima_correcao else Decimal128("1.0"),
            "transferencia": False,
            "contrato": cco_original.get('contratoCpp', ''),
            "campo": cco_original.get('campo', ''),
            "remessa": cco_original.get('remessa', 0),
            "faseRemessa": cco_original.get('faseRemessa', ''),
            "exercicio": cco_original.get('exercicio', 0),
            "periodo": cco_original.get('periodo', 0),
            "quantidadeLancamento": valores_atuais.get('quantidadeLancamento', 0),
            "valorReconhecido": Decimal128(str(round(novo_valor_reconhecido, 16))),
            "valorReconhecidoComOH": Decimal128(str(round(valor_com_oh_correcao, 16))),
            "diferencaValor": Decimal128(str(round(diferenca_valor, 16))),
            "valorReconhecidoComOhOriginal": Decimal128(str(round(valor_original_para_correcao, 16))),
            "overHeadExploracao": Decimal128(str(round(converter_decimal128_para_float(valores_atuais.get('overHeadExploracao', 0)) * fator_correcao, 15))),
            "overHeadProducao": Decimal128(str(round(converter_decimal128_para_float(valores_atuais.get('overHeadProducao', 0)) * fator_correcao, 15))),
            "overHeadTotal": Decimal128(str(round(novo_overhead_total, 15))),
            "valorLancamentoTotal": Decimal128(str(round(
                converter_decimal128_para_float(valores_atuais.get('valorLancamentoTotal', 0)) * fator_correcao, 15
            ))),
            "valorNaoPassivelRecuperacao": Decimal128(str(round(converter_decimal128_para_float(valores_atuais.get('valorNaoPassivelRecuperacao', 0)) * fator_correcao, 15))),
            "valorReconhecivel": Decimal128(str(round(converter_decimal128_para_float(valores_atuais.get('valorReconhecivel', 0)) * fator_correcao, 15))),
            "valorNaoReconhecido": Decimal128(str(round(
                converter_decimal128_para_float(valores_atuais.get('valorNaoReconhecido', 0)) * fator_correcao, 15
            ))),
            "valorReconhecidoExploracao": Decimal128(str(round(converter_decimal128_para_float(valores_atuais.get('valorReconhecidoExploracao', 0)) * fator_correcao, 15))),
            "valorReconhecidoProducao": Decimal128(str(round(converter_decimal128_para_float(valores_atuais.get('valorReconhecidoProducao', 0)) * fator_correcao, 15))),
            "metadadosRetificacao": {
                "tpOriginal": Decimal128(str(metadata['tp_original'])),
                "tpCorrecao": Decimal128(str(metadata['tp_correcao'])),
                "fatorConversao": Decimal128(str(fator_correcao)),
                "versaoScript": "2.0",
                "tipoOperacao": "RETIFICACAO_TP"
            }
        }
        
        # Adicionar campos específicos para CCO recuperada
        if ultima_correcao:
            # Herdar e ajustar valores de recuperação
            if ultima_correcao and 'igpmAcumuladoReais' in ultima_correcao:
                nova_correcao["igpmAcumuladoReais"] = Decimal128(str(round(
                    converter_decimal128_para_float(ultima_correcao['igpmAcumuladoReais']) * fator_correcao, 15
                )))
            
            if ultima_correcao and 'valorRecuperado' in ultima_correcao:
                nova_correcao["valorRecuperado"] = ultima_correcao['valorRecuperado']
                nova_correcao["valorRecuperadoTotal"] = ultima_correcao['valorRecuperadoTotal']
        
        # Adicionar campos spécíficos para CCO de transferência
        if cco_original.get('flgRecuperado', False) == True and converter_decimal128_para_float(nova_correcao.get('valorReconhecidoComOH', 0)) > 0:
            # CCO com novo saldo positivo. Devemos voltar ela como 'recuperável', mudando a flag de 'flgRecuperado' para False
            cco_recalculada["flgRecuperado"] = False
            print(f"✓ Ajustando CCO para 'Recuperável' (flgRecuperado = False). Novo valor reconhecido com OH: {converter_decimal128_para_float(nova_correcao['valorReconhecidoComOH']):,.5f}")
        
        # Adicionar à lista de correções
        if 'correcoesMonetarias' not in cco_recalculada:
            cco_recalculada['correcoesMonetarias'] = []
        
        cco_recalculada['correcoesMonetarias'].append(nova_correcao)
        
        return cco_recalculada, ultimo_valor_reconhecido_com_oh


    def _capturar_estado_original(self, cco: Dict[str, Any]) -> Dict[str, Any]:
        """Captura estado verdadeiramente original da CCO"""
        cco_original = cco.copy()
        
        ## TODO regra inexistente!
        
        # # Se existe correção de RETIFICACAO prévia, removê-la para obter estado anterior
        # if 'correcoesMonetarias' in cco_original:
        #     correcoesMonetarias = cco_original['correcoesMonetarias'].copy()
        #     # Remover última retificação se existir
        #     if correcoesMonetarias and correcoesMonetarias[-1].get('tipo') == 'RETIFICACAO':
        #         correcoesMonetarias.pop()
        #         cco_original['correcoesMonetarias'] = correcoesMonetarias
                
        #         # Restaurar valores anteriores à retificação
        #         if correcoesMonetarias:
        #             ultima_correcao = correcoesMonetarias[-1]
        #             cco_original['valorReconhecidoComOH'] = ultima_correcao.get('valorReconhecidoComOH')
        #             # Recalcular outros valores baseado na última correção válida
        
        return cco_original

    def _preparar_resultado_comparativo(self, cco_original: Dict[str, Any], 
                                   cco_recalculada: Dict[str, Any], 
                                   metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara resultado comparativo entre CCO original e recalculada"""
        
        comparativo = {}
        modo_recalculo = metadata.get('modo_recalculo')
        
        if modo_recalculo == ModoRecalculo.CORRECAO_MONETARIA:
            # Comparar nova correção monetária com a anterior
            if 'correcoesMonetarias' in cco_original and len(cco_original.get('correcoesMonetarias', [])) > 0:
                correcoes_originais = cco_original.get('correcoesMonetarias', [])
                correcoes_recalculadas = cco_recalculada.get('correcoesMonetarias', [])
                # Nova correção adicionada
                nova_correcao = correcoes_recalculadas[-1] if correcoes_recalculadas else {}
                # Última correção original (antes do recálculo)
                correcao_anterior = correcoes_originais[-1] if correcoes_originais else {}
            
            else:
                correcoes_originais = cco_original
                correcao_anterior = cco_original
                nova_correcao = cco_recalculada.get('correcoesMonetarias', [])[0]
                
            
            
            # Campos específicos das correções monetárias
            campos_correcao = [
                'valorReconhecido', 'valorReconhecidoComOH', 'overHeadTotal',
                'valorLancamentoTotal', 'quantidadeLancamento', 'valorNaoReconhecido',
                'valorReconhecivel', 'valorNaoPassivelRecuperacao', 'overHeadProducao',
                'diferencaValor', 'valorRecuperado', 'valorRecuperadoTotal'
            ]
            
            for campo in campos_correcao:
                if campo == 'quantidadeLancamento':
                    valor_anterior = correcao_anterior.get(campo, 0) if correcao_anterior else 0
                    valor_novo = nova_correcao.get(campo, 0)    
                else:
                    valor_anterior = converter_decimal128_para_float(correcao_anterior.get(campo, 0)) if correcao_anterior else 0
                    valor_novo = converter_decimal128_para_float(nova_correcao.get(campo, 0))    
                
                diferenca = valor_novo - valor_anterior
                
                comparativo[f"correcao_{campo}"] = {
                    'valor_original': valor_anterior,
                    'valor_recalculado': valor_novo,
                    'diferenca': diferenca,
                    
                    'percentual_variacao': (diferenca / valor_anterior * 100) if (valor_anterior != 0 and campo not in ['flgRecuperado', 'diferencaValor']) else 0,
                    'fonte': 'Correção Monetária'
                }
                
                # CCO com novo saldo positivo. Voltamos ela como 'recuperável', mudando a flag de 'flgRecuperado' para False
            if cco_original.get('flgRecuperado', False) == True and nova_correcao.get('flgRecuperado', False) == False:
                comparativo["correcao_flgRecuperado"] = {
                    'valor_original': True,
                    'valor_recalculado': False,
                    'diferenca': True,
                    'percentual_variacao': 0,
                    'fonte': 'Correção Monetária'
                }
                
            
        
        else:  # RECALCULO_COMPLETO
            # Comparar toda a CCO (valores principais)
            campos_comparacao = [
                'valorReconhecido', 'valorReconhecidoComOH', 'overHeadTotal',
                'valorLancamentoTotal', 'quantidadeLancamento', 'valorNaoReconhecido',
                'valorReconhecivel', 'valorNaoPassivelRecuperacao', 'valorReconhecidoExploracao',
                'valorReconhecidoProducao', 'overHeadExploracao', 'overHeadProducao'
            ]
            
            for campo in campos_comparacao:
                if campo in cco_original or campo in cco_recalculada:
                    if campo == 'quantidadeLancamento':
                        valor_original = cco_original.get(campo, 0)
                        valor_recalculado = cco_recalculada.get(campo, 0)
                    else:
                        valor_original = converter_decimal128_para_float(cco_original.get(campo, 0))
                        valor_recalculado = converter_decimal128_para_float(cco_recalculada.get(campo, 0))
                        
                            
                    
                    diferenca = valor_recalculado - valor_original
                    
                    comparativo[campo] = {
                        'valor_original': valor_original,
                        'valor_recalculado': valor_recalculado,
                        'diferenca': diferenca,
                        'percentual_variacao': (diferenca / valor_original * 100) if (valor_original != 0 and campo not in ['correcao_flgRecuperado', 'correcao_diferencaValor']) else 0,
                        'fonte': 'CCO Completa'
                    }
        
        return {
            'cco_original': cco_original,
            'cco_recalculada': cco_recalculada,
            'metadata_recalculo': metadata,
            'comparativo': comparativo,
            'resumo': {
                'total_campos_alterados': len([c for c in comparativo.values() if c['diferenca'] != 0]),
                'maior_variacao_percentual': max([abs(c['percentual_variacao']) for c in comparativo.values()], default=0),
                'modo_comparacao': 'Correção Monetária' if modo_recalculo == ModoRecalculo.CORRECAO_MONETARIA else 'CCO Completa'
            }
        }

    def _atualizar_cco_e_criar_evento(self, cco_recalculada: Dict[str, Any], observacoes: str) -> Dict[str, Any]:
        """Atualiza CCO na base principal e cria evento de atualização"""
        try:
            cco_id = cco_recalculada['_id']
            
            # Incrementar versão
            nova_versao = (cco_recalculada.get('version', 0) + 1)
            
            # Preparar dados para atualização (remover _id e metadata_recalculo)
            dados_atualizacao = {k: v for k, v in cco_recalculada.items() 
                            if (k != '_id' and k != 'metadata_recalculo')}
            
            # Remover metadadosRetificacao das correções monetárias se existir
            if 'correcoesMonetarias' in dados_atualizacao and dados_atualizacao['correcoesMonetarias']:
                for correcao in dados_atualizacao['correcoesMonetarias']:
                    if 'metadadosRetificacao' in correcao:
                        del correcao['metadadosRetificacao']
                        
            # Atualizar versão
            dados_atualizacao['version'] = Int64(nova_versao)
            
            # Atualizar documento principal
            # O cco_id pode ser uma string customizada ou ObjectId, tratar adequadamente
            filter_id = self._preparar_filtro_id(cco_id)
            resultado_update = self.db.conta_custo_oleo_entity.update_one(
                {'_id': filter_id},
                {'$set': dados_atualizacao}
            )
            
            if resultado_update.modified_count != 1:
                return {'success': False, 'erro': 'Falha ao atualizar documento principal'}
            
            # Buscar evento base para criar novo evento
            evento_base = self.db.event.find_one({
                'aggregateId': cco_id,
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
                novo_evento['username'] = 'system_recalculo'
                novo_evento['createSystem'] = observacoes
                
                # Atualizar contaCustoOleoEntity com a nova versão da CCO, 
                # convertendo tipos conforme necessário
                if 'contaCustoOleoEntity' in novo_evento:
                    cco_para_evento = self._converter_cco_para_evento(cco_recalculada)
                    cco_para_evento['version'] = nova_versao - 1  # versão anterior no evento
                    novo_evento['contaCustoOleoEntity'] = cco_para_evento
                
                self.db.event.insert_one(novo_evento)
            
            return {'success': True, 'nova_versao': nova_versao}
            
        except Exception as e:
            logger.error(f"Erro ao atualizar CCO: {e}")
            return {'success': False, 'erro': str(e)}

    def _preparar_filtro_id(self, cco_id) -> Any:
        """
        Prepara o ID para usar como filtro no MongoDB, tratando diferentes tipos de ID
        """
        try:
            # Se já for ObjectId, retornar como está
            if isinstance(cco_id, ObjectId):
                return cco_id
            
            # Se for string, verificar se é um ObjectId válido (24 caracteres hex)
            cco_id_str = str(cco_id)
            if len(cco_id_str) == 24:
                try:
                    return ObjectId(cco_id_str)
                except:
                    # Se não conseguir converter, usar como string
                    return cco_id_str
            else:
                # IDs customizados (como 'DdxcWkTmTXOOvY89d-qOYAAAAAA') usar como string
                return cco_id_str
        except:
            # Em caso de qualquer erro, usar o valor original
            return cco_id

    def _converter_cco_para_evento(self, cco: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converte CCO da estrutura da coleção para a estrutura do evento,
        ajustando tipos de dados conforme necessário
        """
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
            if campo in ['_id', '_class', 'metadata_recalculo']:
                continue
                
            # ID da CCO vira 'id' no evento
            if campo == '_id':
                cco_evento['id'] = str(valor)
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
                            # Converter datetime para formato ISO string (sem timezone info)
                            correcao_evento[campo_corr] = valor_corr.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                        else:
                            correcao_evento[campo_corr] = valor_corr
                    cco_evento[campo].append(correcao_evento)
            elif isinstance(valor, datetime):
                # Manter datetimes como estão (serão serializados pelo MongoDB)
                cco_evento[campo] = valor
            elif isinstance(valor, ObjectId):
                # Converter ObjectId para string
                cco_evento[campo] = str(valor)
            else:
                # Manter outros tipos como estão
                cco_evento[campo] = valor
        
        # Adicionar campo id se não existir
        if 'id' not in cco_evento:
            cco_evento['id'] = str(cco.get('_id', ''))
        
        return cco_evento

    def fechar_conexoes(self):
        """Fecha conexões com MongoDB"""
        if self.client:
            self.client.close()
        if self.client_local:
            self.client_local.close()