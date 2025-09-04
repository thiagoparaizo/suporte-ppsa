"""
Repository para operações de CCOs
"""

import logging
from typing import Dict, List, Optional, Any
from pymongo.database import Database
from pymongo.collection import Collection

logger = logging.getLogger(__name__)

class CCORepository:
    """Repository para operações com CCOs"""
    
    def __init__(self, database: Database):
        self.db = database
        self.collection: Collection = database.conta_custo_oleo_entity

    def buscar_por_id(self, cco_id: str) -> Optional[Dict[str, Any]]:
        """Busca CCO por ID"""
        try:
            return self.collection.find_one({"_id": cco_id})
        except Exception as e:
            logger.error(f"Erro ao buscar CCO por ID {cco_id}: {e}")
            return None

    def buscar_por_filtros(self, filtros, projecao=None):
        """Busca CCOs por filtros"""
        try:
            cursor = self.collection.find(filtros).limit(100)
            return list(cursor.sort([('remessa', -1), ('faseRemessa', 1)]))
        except Exception as e:
            logger.error(f"Erro ao buscar CCOs por filtros {filtros}: {e}")
            return []

    def buscar_cco_com_projecao_basica(self, filtros: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Busca CCOs com projeção básica para análise de remessas"""
        try:
            projecao = {
                '_id': 1,
                'contratoCpp': 1,
                'campo': 1,
                'remessa': 1,
                'faseRemessa': 1,
                'exercicio': 1,
                'periodo': 1,
                'origemDosGastos': 1,
                'dataReconhecimento': 1,
                'dataLancamento': 1,
                'valorReconhecido': 1,
                'valorReconhecidoComOH': 1,
                'overHeadTotal': 1,
                'overHeadExploracao': 1,
                'overHeadProducao': 1,
                'valorReconhecidoExploracao': 1,
                'valorReconhecidoProducao': 1,
                'valorLancamentoTotal': 1,
                'valorNaoReconhecido': 1,
                'valorReconhecivel': 1,
                'valorNaoPassivelRecuperacao': 1,
                'quantidadeLancamento': 1,
                'flgRecuperado': 1,
                'tipo': 1,
                'subTipo': 1,
                'dataCorrecao': 1,
                'dataCriacaoCorrecao': 1,
                'taxaCorrecao': 1,
                'igpmAcumulado': 1,
                'igpmAcumuladoReais': 1,
                'diferencaValor': 1,
                'ativo': 1,
                'transferencia': 1,
                'correcoesMonetarias': 1
            }
            
            cursor = self.collection.find(filtros, projecao)
            return list(cursor)
            
        except Exception as e:
            logger.error(f"Erro ao buscar CCOs com projeção básica: {e}")
            return []

    def buscar_cco_completa(self, cco_id: str) -> Optional[Dict[str, Any]]:
        """Busca CCO completa por ID"""
        try:
            return self.collection.find_one({"_id": cco_id})
        except Exception as e:
            logger.error(f"Erro ao buscar CCO completa {cco_id}: {e}")
            return None

    def contar_por_filtros(self, filtros: Dict[str, Any]) -> int:
        """Conta CCOs que atendem aos filtros"""
        try:
            return self.collection.count_documents(filtros)
        except Exception as e:
            logger.error(f"Erro ao contar CCOs: {e}")
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

    def buscar_estatisticas_basicas(self, filtros: Dict[str, Any] = None) -> Dict[str, Any]:
        """Busca estatísticas básicas das CCOs"""
        try:
            pipeline = []
            
            if filtros:
                pipeline.append({"$match": filtros})
            
            pipeline.extend([
                {
                    "$group": {
                        "_id": None,
                        "totalCCOs": {"$sum": 1},
                        "contratos": {"$addToSet": "$contratoCpp"},
                        "campos": {"$addToSet": "$campo"},
                        "fases": {"$addToSet": "$faseRemessa"},
                        "recuperadas": {
                            "$sum": {
                                "$cond": [{"$eq": ["$flgRecuperado", True]}, 1, 0]
                            }
                        },
                        "comCorrecoes": {
                            "$sum": {
                                "$cond": [
                                    {"$gt": [{"$size": {"$ifNull": ["$correcoesMonetarias", []]}}, 0]}, 
                                    1, 
                                    0
                                ]
                            }
                        }
                    }
                }
            ])
            
            resultado = list(self.collection.aggregate(pipeline))
            if resultado:
                stats = resultado[0]
                return {
                    'totalCCOs': stats.get('totalCCOs', 0),
                    'totalContratos': len(stats.get('contratos', [])),
                    'totalCampos': len(stats.get('campos', [])),
                    'totalRecuperadas': stats.get('recuperadas', 0),
                    'totalComCorrecoes': stats.get('comCorrecoes', 0),
                    'fases': sorted(stats.get('fases', []))
                }
            else:
                return {
                    'totalCCOs': 0,
                    'totalContratos': 0,
                    'totalCampos': 0,
                    'totalRecuperadas': 0,
                    'totalComCorrecoes': 0,
                    'fases': []
                }
                
        except Exception as e:
            logger.error(f"Erro ao buscar estatísticas básicas: {e}")
            return {}

    def buscar_ccos_por_remessa(self, contrato: str, campo: str, remessa: int) -> List[Dict[str, Any]]:
        """Busca todas as CCOs de uma remessa específica"""
        try:
            filtros = {
                'contratoCpp': contrato,
                'campo': campo,
                'remessa': remessa
            }
            
            projecao = {
                '_id': 1,
                'faseRemessa': 1,
                'valorReconhecidoComOH': 1,
                'overHeadTotal': 1,
                'flgRecuperado': 1,
                'dataReconhecimento': 1,
                'dataLancamento': 1,
                'correcoesMonetarias': {'$slice': -1}  # Apenas a última correção
            }
            
            cursor = self.collection.find(filtros, projecao)
            return list(cursor.sort([('faseRemessa', 1)]))
            
        except Exception as e:
            logger.error(f"Erro ao buscar CCOs da remessa {remessa}: {e}")
            return []

    def buscar_duplicatas(self, limite: int = 1000) -> List[Dict[str, Any]]:
        """Busca CCOs potencialmente duplicadas"""
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": {
                            "contratoCpp": "$contratoCpp",
                            "campo": "$campo", 
                            "remessa": "$remessa",
                            "faseRemessa": "$faseRemessa"
                        },
                        "count": {"$sum": 1},
                        "ids": {"$push": "$_id"},
                        "docs": {"$push": "$$ROOT"}
                    }
                },
                {
                    "$match": {
                        "count": {"$gt": 1}
                    }
                },
                {
                    "$limit": limite
                }
            ]
            
            return list(self.collection.aggregate(pipeline))
            
        except Exception as e:
            logger.error(f"Erro ao buscar duplicatas: {e}")
            return []