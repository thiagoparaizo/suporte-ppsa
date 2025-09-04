"""
Repository para operações de remessas
"""

import logging
from typing import Dict, List, Optional, Any
from pymongo.database import Database
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

class RemessaRepository:
    """Repository para operações com remessas"""
    
    def __init__(self, database: Database):
        self.db = database
        self.collection: Collection = database.remessa_derivada_campo_entity

    def buscar_por_id(self, remessa_id: str) -> Optional[Dict[str, Any]]:
        """Busca remessa por ID"""
        try:
            return self.collection.find_one({"_id": remessa_id})
        except Exception as e:
            logger.error(f"Erro ao buscar remessa por ID {remessa_id}: {e}")
            return None

    def buscar_por_filtros(self, filtros: Dict[str, Any], limite: int = 500) -> List[Dict[str, Any]]:
        """Busca remessas por filtros com projeção otimizada"""
        try:
            # Projeção básica para listagem
            projecao = {
                '_id': 1,
                'contratoCPP': 1,
                'campo': 1,
                'remessa': 1,
                'remessaExposicao': 1,
                'exercicio': 1,
                'periodo': 1,
                'mesAnoReferencia': 1,
                'faseRemessa': 1,
                'etapa': 1,
                'origemDoGasto': 1,
                'gastosCompartilhados': 1,
                'usuarioResponsavel': 1,
                'dataLancamento': 1,
                'version': 1,
                'gastos': 1  # Incluir gastos para análise
            }
            
            cursor = self.collection.find(filtros, projecao).limit(limite)
            return list(cursor.sort([('remessa', -1), ('exercicio', -1), ('periodo', -1)]))
            
        except Exception as e:
            logger.error(f"Erro ao buscar remessas por filtros {filtros}: {e}")
            return []

    def buscar_remessas_com_reconhecimento(self, filtros: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Busca remessas que possuem gastos reconhecidos"""
        try:
            # Projeção expandida para análise completa
            projecao = {
                '_id': 1,
                'fatorAlocacao': 1,
                'exercicio': 1,
                'periodo': 1,
                'contratoCPP': 1,
                'campo': 1,
                'faseRemessa': 1,
                'remessa': 1,
                'remessaExposicao': 1,
                'mesAnoReferencia': 1,
                'gastosCompartilhados': 1,
                'origemDoGasto': 1,
                'uep': 1,
                'version': 1,
                'gastos.item': 1,
                'gastos.mesCompetencia': 1,
                'gastos.anoCompetencia': 1,
                'gastos.classificacaoGastoTipo': 1,
                'gastos.valorMoedaOBJReal': 1,
                'gastos.reconhecimentoTipo': 1,
                'gastos.statusGastoTipo': 1,
                'gastos.valorReconhecido': 1,
                'gastos.valorNaoReconhecido': 1,
                'gastos.responsavel': 1,
                'gastos.fase': 1,
                'gastos.reconhecido': 1,
                'gastos.faseRespostaGestora': 1,
                'gastos.dataLancamento': 1,
                'gastos.dataReconhecimento': 1,
                'gastos.valorMoedaOBJRealOriginal': 1,
                'gastos.moedaTransacao': 1,
                'gastos.valorMoedaACC': 1,
                'gastos.valorMoedaTrans': 1,
                'gastos.projeto': 1,
                'gastos.descricaoProjeto': 1,
                'gastos.elementoPEP': 1,
                'gastos.descricaoClasseCusto': 1
            }
            
            cursor = self.collection.find(filtros, projecao)
            return list(cursor.sort([('remessa', 1), ('exercicio', 1), ('periodo', 1)]))
            
        except Exception as e:
            logger.error(f"Erro ao buscar remessas com reconhecimento: {e}")
            return []

    def contar_por_filtros(self, filtros: Dict[str, Any]) -> int:
        """Conta remessas que atendem aos filtros"""
        try:
            return self.collection.count_documents(filtros)
        except Exception as e:
            logger.error(f"Erro ao contar remessas: {e}")
            return 0

    def obter_valores_distintos(self, campo: str, filtros: Dict[str, Any] = None) -> List[str]:
        """Obtém valores distintos de um campo"""
        try:
            if filtros:
                return self.collection.distinct(campo, filtros)
            else:
                return self.collection.distinct(campo)
        except Exception as e:
            logger.error(f"Erro ao obter valores distintos do campo {campo}: {e}")
            return []

    def buscar_remessa_completa(self, remessa_id: str) -> Optional[Dict[str, Any]]:
        """Busca remessa com todos os campos para análise detalhada"""
        try:
            return self.collection.find_one({"_id": remessa_id})
        except Exception as e:
            logger.error(f"Erro ao buscar remessa completa {remessa_id}: {e}")
            return None

    def buscar_estatisticas_basicas(self, filtros: Dict[str, Any] = None) -> Dict[str, Any]:
        """Busca estatísticas básicas das remessas"""
        try:
            pipeline = []
            
            if filtros:
                pipeline.append({"$match": filtros})
            
            pipeline.extend([
                {
                    "$group": {
                        "_id": None,
                        "totalRemessas": {"$sum": 1},
                        "contratos": {"$addToSet": "$contratoCPP"},
                        "campos": {"$addToSet": "$campo"},
                        "exercicios": {"$addToSet": "$exercicio"},
                        "fases": {"$addToSet": "$faseRemessa"},
                        "origens": {"$addToSet": "$origemDoGasto"}
                    }
                }
            ])
            
            resultado = list(self.collection.aggregate(pipeline))
            if resultado:
                stats = resultado[0]
                return {
                    'totalRemessas': stats.get('totalRemessas', 0),
                    'totalContratos': len(stats.get('contratos', [])),
                    'totalCampos': len(stats.get('campos', [])),
                    'exercicios': sorted(stats.get('exercicios', [])),
                    'fases': sorted(stats.get('fases', [])),
                    'origens': sorted(stats.get('origens', []))
                }
            else:
                return {
                    'totalRemessas': 0,
                    'totalContratos': 0,
                    'totalCampos': 0,
                    'exercicios': [],
                    'fases': [],
                    'origens': []
                }
                
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas básicas: {e}")
            return {}

    def buscar_ultimas_remessas(self, limite: int = 10) -> List[Dict[str, Any]]:
        """Busca as últimas remessas cadastradas"""
        try:
            projecao = {
                '_id': 1,
                'contratoCPP': 1,
                'campo': 1,
                'remessa': 1,
                'exercicio': 1,
                'periodo': 1,
                'faseRemessa': 1,
                'dataLancamento': 1,
                'version': 1
            }
            
            cursor = self.collection.find({}, projecao)
            return list(cursor.sort([('dataLancamento', -1)]).limit(limite))
            
        except Exception as e:
            logger.error(f"Erro ao buscar últimas remessas: {e}")
            return []