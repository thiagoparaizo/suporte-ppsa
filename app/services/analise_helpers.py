"""
Helpers para análise de remessas x CCOs, extraídos de app/routes/analise_ui.py.
"""

from datetime import datetime
from collections import Counter
from typing import Any, Dict, List

from app.utils.converters import converter_decimal128_para_float


def calcular_estatisticas_gastos(gastos: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_gastos = len(gastos)
    gastos_reconhecidos = len([g for g in gastos if g.get('reconhecido') == 'SIM'])

    valor_total = sum(converter_decimal128_para_float(g.get('valorMoedaOBJReal', 0)) for g in gastos)
    valor_reconhecido = sum(
        converter_decimal128_para_float(g.get('valorReconhecido', 0)) for g in gastos if g.get('reconhecido') == 'SIM'
    )
    valor_nao_reconhecido = sum(converter_decimal128_para_float(g.get('valorNaoReconhecido', 0)) for g in gastos)

    reconhecimento_tipos: Dict[str, int] = {}
    status_tipos: Dict[str, int] = {}
    for gasto in gastos:
        rec_tipo = gasto.get('reconhecimentoTipo', 'INDEFINIDO')
        reconhecimento_tipos[rec_tipo] = reconhecimento_tipos.get(rec_tipo, 0) + 1
        status = gasto.get('statusGastoTipo', 'INDEFINIDO')
        status_tipos[status] = status_tipos.get(status, 0) + 1

    return {
        'totalGastos': total_gastos,
        'gastosReconhecidos': gastos_reconhecidos,
        'gastosPendentes': total_gastos - gastos_reconhecidos,
        'taxaReconhecimento': (gastos_reconhecidos / total_gastos * 100) if total_gastos > 0 else 0,
        'valorTotal': valor_total,
        'valorReconhecido': valor_reconhecido,
        'valorNaoReconhecido': valor_nao_reconhecido,
        'percentualValorReconhecido': (valor_reconhecido / valor_total * 100) if valor_total > 0 else 0,
        'reconhecimentoTipos': reconhecimento_tipos,
        'statusTipos': status_tipos,
    }


def obter_top_classificacoes(gastos: List[Dict[str, Any]], top: int = 5) -> List[Dict[str, Any]]:
    classificacoes = [g.get('classificacaoGastoTipo', 'INDEFINIDO') for g in gastos if g.get('classificacaoGastoTipo')]
    counter = Counter(classificacoes)
    return [{'classificacao': k, 'quantidade': v} for k, v in counter.most_common(top)]


def obter_top_responsaveis(gastos: List[Dict[str, Any]], top: int = 5) -> List[Dict[str, Any]]:
    responsaveis = [g.get('responsavel', 'INDEFINIDO') for g in gastos if g.get('responsavel')]
    counter = Counter(responsaveis)
    return [{'responsavel': k, 'quantidade': v} for k, v in counter.most_common(top)]


def obter_distribuicao_status(gastos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    status = [g.get('statusGastoTipo', 'INDEFINIDO') for g in gastos]
    counter = Counter(status)
    total = len(gastos) or 1
    return [{'status': k, 'quantidade': v, 'percentual': v / total * 100} for k, v in counter.items()]


def obter_moedas_utilizadas(gastos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    moedas = [g.get('moedaTransacao', 'INDEFINIDO') for g in gastos if g.get('moedaTransacao')]
    counter = Counter(moedas)
    return [{'moeda': k, 'quantidade': v} for k, v in counter.items()]


def processar_dados_dashboard(resultado_analise: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'resumo': {
            'totalRemessas': resultado_analise['estatisticas']['totalRemessas'],
            'totalFases': resultado_analise['estatisticas']['totalFasesEncontradas'],
            'ccosEncontradas': resultado_analise['estatisticas']['totalCCOsEncontradas'],
            'taxaEncontro': (
                resultado_analise['estatisticas']['totalCCOsEncontradas']
                / (resultado_analise['estatisticas']['totalFasesEncontradas'] or 1)
                * 100
            ),
        },
        'distribuicaoFases': resultado_analise['estatisticas']['fasesPorTipo'],
        'timeline': extrair_timeline_dados(resultado_analise['remessasAnalisadas']),
        'consolidacao': resultado_analise['estatisticas'].get('consolidacaoGeral', {}),
    }


def extrair_timeline_dados(remessas_analisadas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    timeline_data: List[Dict[str, Any]] = []
    for remessa in remessas_analisadas:
        for fase in remessa['fasesComReconhecimento']:
            if fase.get('dataReconhecimento'):
                timeline_data.append(
                    {
                        'data': fase['dataReconhecimento'],
                        'remessa': remessa['remessa'],
                        'fase': fase['fase'],
                        'contrato': remessa['contratoCPP'],
                        'campo': remessa['campo'],
                        'cco_status': fase.get('cco', {}).get('statusCCO', 'SEM_DADOS'),
                    }
                )
    timeline_data.sort(key=lambda x: x['data'])
    return timeline_data


def gerar_csv_analise(resultado_analise: Dict[str, Any], formato: str = 'detalhado') -> str:
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')

    if formato == 'resumido':
        writer.writerow(['Métrica', 'Valor'])
        writer.writerow(['Total de Remessas', resultado_analise['estatisticas']['totalRemessas']])
        writer.writerow(['Total de Fases', resultado_analise['estatisticas']['totalFasesEncontradas']])
        writer.writerow(['CCOs Encontradas', resultado_analise['estatisticas']['totalCCOsEncontradas']])
        writer.writerow(['CCOs Não Encontradas', resultado_analise['estatisticas']['totalCCOsNaoEncontradas']])
        writer.writerow(['CCOs Duplicadas', resultado_analise['estatisticas']['totalCCOsDuplicadas']])
        writer.writerow([])
        writer.writerow(['Fase', 'Quantidade'])
        for fase, count in resultado_analise['estatisticas']['fasesPorTipo'].items():
            writer.writerow([fase, count])
    else:
        headers = [
            'Remessa ID', 'Contrato', 'Campo', 'Remessa', 'Exercicio', 'Periodo',
            'Mes Ano Ref', 'Fase', 'Data Reconhecimento', 'CCO Status', 'CCO ID',
            'Valor Reconhecido Fase', 'Valor CCO', 'Overhead Total', 'Observacao',
        ]
        writer.writerow(headers)
        for remessa in resultado_analise['remessasAnalisadas']:
            for fase in remessa['fasesComReconhecimento']:
                cco = fase.get('cco', {})
                writer.writerow(
                    [
                        remessa['id'],
                        remessa['contratoCPP'],
                        remessa['campo'],
                        remessa['remessa'],
                        remessa['exercicio'],
                        remessa['periodo'],
                        remessa['mesAnoReferencia'],
                        fase['fase'],
                        fase.get('dataReconhecimento', ''),
                        cco.get('statusCCO', 'SEM_DADOS'),
                        cco.get('id', ''),
                        fase.get('valorReconhecido', 0),
                        cco.get('valorReconhecidoComOH', 0),
                        cco.get('overHeadTotal', 0),
                        cco.get('observacao', ''),
                    ]
                )
    return output.getvalue()


def gerar_recomendacao_analise(total_remessas: int, remessas_com_reconhecimento: int) -> str:
    if remessas_com_reconhecimento == 0:
        return "AVISO: Nenhuma remessa com reconhecimento encontrada. Verifique os filtros aplicados."
    if remessas_com_reconhecimento > 1000:
        return "ATENÇÃO: Volume alto de dados. Considere refinar os filtros para melhor performance."
    if remessas_com_reconhecimento > 500:
        return "Volume moderado de dados. A análise pode levar alguns minutos."
    return "Volume adequado para análise rápida."

