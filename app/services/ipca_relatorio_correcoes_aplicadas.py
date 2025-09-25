#!/usr/bin/env python3
"""
Gerador de relatório das correções IPCA/IGPM aplicadas
"""

import csv
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

def gerar_relatorio_correcoes_aplicadas():
    # Conectar MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.sgppServices
    
    # Pipeline corrigido
    pipeline = [
        {"$match": {"status": "APPLIED"}},
        {"$addFields": {
            "corrections_applied_only": {
                "$filter": {
                    "input": "$corrections_proposed",
                    "as": "correction",
                    "cond": {"$in": ["$$correction.correction_id", "$corrections_approved"]}
                }
            }
        }},
        {"$unwind": "$corrections_applied_only"},
        {"$project": {
            "session_id": 1,
            "cco_id": 1,
            "user_id": 1,
            "scenario_detected": 1,
            "applied_at": 1,
            "correction_id": "$corrections_applied_only.correction_id",
            "correction_type": "$corrections_applied_only.type",
            "target_period": "$corrections_applied_only.target_period",
            "current_value": "$corrections_applied_only.current_value",
            "proposed_value": "$corrections_applied_only.proposed_value",
            "impact": "$corrections_applied_only.impact",
            "taxa_aplicada": "$corrections_applied_only.taxa_aplicada",
            "description": "$corrections_applied_only.description"
        }}
    ]
    
    resultados = list(db.ipca_correction_sessions.aggregate(pipeline))
    
    # Gerar CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"relatorio_correcoes_aplicadas_{timestamp}.csv"
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        fieldnames = [
            'session_id', 'cco_id', 'user_id', 'scenario_detected', 
            'applied_at', 'correction_type', 'target_period',
            'current_value', 'proposed_value', 'impact', 
            'taxa_aplicada', 'description'
        ]
        
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in resultados:
            writer.writerow({
                'session_id': result['session_id'],
                'cco_id': result['cco_id'],
                'user_id': result['user_id'],
                'scenario_detected': result['scenario_detected'],
                'applied_at': result['applied_at'],
                'correction_type': result['correction_type'],
                'target_period': result['target_period'],
                'current_value': result['current_value'],
                'proposed_value': result['proposed_value'],
                'impact': result['impact'],
                'taxa_aplicada': result['taxa_aplicada'],
                'description': result['description']
            })
    
    # Gerar Excel com resumo
    excel_file = f"relatorio_correcoes_aplicadas_{timestamp}.xlsx"
    df = pd.DataFrame(resultados)
    
    with pd.ExcelWriter(excel_file) as writer:
        # Aba detalhada
        df.to_excel(writer, sheet_name='Detalhado', index=False)
        
        # Aba resumo por cenário
        resumo_cenario = df.groupby('scenario_detected').agg({
            'session_id': 'count',
            'impact': 'sum',
            'current_value': 'sum',
            'proposed_value': 'sum'
        }).rename(columns={'session_id': 'total_correcoes'})
        resumo_cenario.to_excel(writer, sheet_name='Resumo_Cenario')
        
        # Aba resumo por tipo
        resumo_tipo = df.groupby('correction_type').agg({
            'session_id': 'count',
            'impact': 'sum'
        }).rename(columns={'session_id': 'total_correcoes'})
        resumo_tipo.to_excel(writer, sheet_name='Resumo_Tipo')
    
    print(f"Relatórios gerados: {csv_file}, {excel_file}")

if __name__ == "__main__":
    gerar_relatorio_correcoes_aplicadas()