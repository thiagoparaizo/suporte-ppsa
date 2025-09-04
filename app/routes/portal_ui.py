from flask import Flask, Blueprint, render_template, request, jsonify, redirect, session, url_for
import json
import os
from datetime import datetime
from collections import Counter, defaultdict
import pandas as pd
import re
from decimal import Decimal
import logging

from app.config import MONGO_URI
from app.utils.converters import processar_json_mongodb, validar_e_converter_valor_monetario, converter_decimal128_para_float, formatar_data_brasileira, formatar_data_simples
from app.utils.cache_utils import CacheManager

portal_bp = Blueprint('portal_ui', __name__)

logger = logging.getLogger(__name__)

dados_analise = None

@portal_bp.context_processor
def inject_today_date():
    return {'today_date': datetime.today().strftime('%Y-%m-%d')}

@portal_bp.route('/')
def index():
    """Página inicial com menu de funcionalidades"""
    return render_template('index.html', titulo="Portal de Análises PPSA")

@portal_bp.route('/verificacao-remessas-ccos')
def verificacao_remessas_ccos():
    """Página de verificação de remessas vs CCOs (antigo index)"""
    return render_template('verificacao_remessas_ccos.html', titulo="Verificação Remessas x CCOs")

@portal_bp.route('/upload', methods=['POST'])
def upload_file():
    """Endpoint para upload do arquivo JSON"""
    global dados_analise
    
    if 'file' not in request.files:
        return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'Nenhum arquivo selecionado'}), 400
    
    if file and file.filename.endswith('.json'):
        try:
            # Ler conteúdo do arquivo
            content = file.read().decode('utf-8')
            
            # Processar JSON com tipos BSON do MongoDB
            dados_analise = processar_json_mongodb(content)
            
            # Validar estrutura básica
            if 'remessasAnalisadas' not in dados_analise or 'estatisticas' not in dados_analise:
                return jsonify({'error': 'Arquivo JSON não possui estrutura esperada'}), 400
            
            return jsonify({'success': True, 'message': 'Arquivo carregado com sucesso'})
            
        except json.JSONDecodeError as e:
            return jsonify({'error': f'JSON inválido: {str(e)}'}), 400
        except ValueError as e:
            return jsonify({'error': f'Erro na conversão de tipos BSON: {str(e)}'}), 400
        except Exception as e:
            return jsonify({'error': f'Erro ao processar arquivo: {str(e)}'}), 500
    
    return jsonify({'error': 'Apenas arquivos JSON são aceitos'}), 400

# @portal_bp.route('/dashboard')
# def dashboard():
#     """Página principal do dashboard"""
#     global dados_analise
    
#     if dados_analise is None:
#         return redirect(url_for('portal_ui.index'))
    
#     # Processar dados para o dashboard
#     stats = processar_estatisticas(dados_analise)
    
#     return render_template('dashboard.html', 
#                          dados=dados_analise, 
#                          stats=stats,
#                          titulo="Dashboard - Análise Remessas x CCOs")

@portal_bp.route('/dashboard')
def dashboard():
    """Página principal do dashboard"""
    global dados_analise
    
    # Verificar se há dados da análise por filtros no cache
    cache_key = request.args.get('cache_key')
    if cache_key:
        try:
            cache_manager = CacheManager(scope='user')
            dados_cache = cache_manager.get_data(f"analise_temp_{cache_key}")
            if dados_cache:
                dados_analise = dados_cache
                # Remover do cache após uso
                cache_manager.delete_data(f"analise_temp_{cache_key}")
        except Exception as e:
            logger.error(f"Erro ao recuperar dados do cache: {e}")
    
    if dados_analise is None:
        return redirect(url_for('portal_ui.index'))
    
    # Processar dados para o dashboard
    stats = processar_estatisticas(dados_analise)
    
    return render_template('dashboard.html', 
                         dados=dados_analise, 
                         stats=stats,
                         titulo="Dashboard - Análise Remessas x CCOs")
    

@portal_bp.route('/api/dados-grafico/<tipo>')
def dados_grafico(tipo):
    """API para fornecer dados específicos para gráficos"""
    global dados_analise
    
    if dados_analise is None:
        return jsonify({'error': 'Nenhum dado carregado'}), 400
    
    if tipo == 'fases_tempo':
        return jsonify(gerar_dados_fases_tempo())
    elif tipo == 'valores_remessa':
        return jsonify(gerar_dados_valores_remessa())
    elif tipo == 'distribuicao_fases':
        return jsonify(gerar_dados_distribuicao_fases())
    elif tipo == 'timeline_reconhecimento':
        return jsonify(gerar_dados_timeline())
    
    return jsonify({'error': 'Tipo de gráfico não reconhecido'}), 400

def processar_estatisticas(dados):
    """Processa dados para gerar estatísticas do dashboard"""
    remessas = dados['remessasAnalisadas']
    is_detalhada = dados.get('tipoAnalise') == 'DETALHADA'
    
    stats = {
        'tipoAnalise': dados.get('tipoAnalise', 'SIMPLIFICADA'),
        'resumo_geral': {
            'total_remessas': len(remessas),
            'total_fases': sum(len(r['fasesComReconhecimento']) for r in remessas),
            'ccos_encontradas': int(dados['estatisticas']['totalCCOsEncontradas']) if 'totalCCOsEncontradas' in dados['estatisticas'] else 0,
            'ccos_nao_encontradas': int(dados['estatisticas']['totalCCOsNaoEncontradas']) if 'totalCCOsNaoEncontradas' in dados['estatisticas'] else 0,
            'fases_com_ccos_duplicadas': int(dados['estatisticas']['totalCCOsDuplicadas']) if 'totalCCOsDuplicadas' in dados['estatisticas'] else 0,
        },
        'por_exercicio': defaultdict(lambda: {'remessas': 0, 'fases': 0, 'valor_total': 0}),
        'por_fase': defaultdict(int),
        'valores': {
            'total_reconhecido_remessas': 0,
            'total_reconhecido_ccos': 0,
            'maior_valor_remessa': 0,
            'menor_valor_remessa': float('inf'),
            'total_overhead': 0,
            'total_correcoes_monetarias': 0
        },
        'datas': {
            'primeira_remessa': None,
            'ultima_remessa': None,
            'tempo_medio_reconhecimento': 0
        },
        'consolidacao_detalhada': {}
    }
    
    # Se é análise detalhada, incluir consolidação geral
    if is_detalhada and 'consolidacaoGeral' in dados['estatisticas']:
        stats['consolidacao_detalhada'] = dados['estatisticas']['consolidacaoGeral']
        
        stats['consolidacao_detalhada']['totalGastos'] = int(dados['estatisticas']['consolidacaoGeral']['contadores']['total']) or 0
    
    datas_reconhecimento = []
    valores_remessa = []
    
    for remessa in remessas:
        exercicio = int(remessa['exercicio'])
        stats['por_exercicio'][exercicio]['remessas'] += 1
        stats['por_exercicio'][exercicio]['fases'] += len(remessa['fasesComReconhecimento'])
        
        # Valores da remessa (consolidação ou por fase)
        valor_remessa = 0
        if is_detalhada and 'consolidacaoRemessa' in remessa:
            valor_remessa = remessa['consolidacaoRemessa']['valores']['reconhecido']
        
        for fase in remessa['fasesComReconhecimento']:
            stats['por_fase'][fase['fase']] += 1
            
            # Se não tem consolidação da remessa, somar por fase
            if not is_detalhada and 'valorReconhecido' in fase:
                valor_remessa += fase.get('valorReconhecido', 0)
            
            # Valores das fases (se detalhada)
            if is_detalhada and 'consolidacao' in fase:
                valor_fase = fase['consolidacao']['valores']['reconhecido']
                stats['valores']['total_reconhecido_remessas'] += valor_fase
            
            # Datas
            if fase.get('dataReconhecimento'):
                try:
                    data = datetime.fromisoformat(fase['dataReconhecimento'].replace('Z', '+00:00'))
                    datas_reconhecimento.append(data)
                except:
                    pass
            
            # Valores CCO
            if fase.get('cco') and fase['cco'].get('statusCCO') == 'ENCONTRADA':
                try:
                    # Verificar se tem valorReconhecido na estrutura da CCO
                    if 'valorReconhecido' in fase['cco']:
                        valor_cco = validar_e_converter_valor_monetario(fase['cco']['valorReconhecido'])
                        stats['valores']['total_reconhecido_ccos'] += valor_cco
                    
                    # Valores de overhead
                    if 'overHeadTotal' in fase['cco']:
                        overhead = validar_e_converter_valor_monetario(fase['cco']['overHeadTotal'])
                        stats['valores']['total_overhead'] += overhead
                    
                    # Correções monetárias
                    if 'igpmAcumuladoReais' in fase['cco']:
                        correcao = validar_e_converter_valor_monetario(fase['cco']['igpmAcumuladoReais'])
                        stats['valores']['total_correcoes_monetarias'] += correcao
                        
                except Exception as e:
                    logger.warning(f"Erro ao processar valores CCO: {e}")
        
        # Adicionar valor da remessa
        if valor_remessa > 0:
            valores_remessa.append(valor_remessa)
            stats['por_exercicio'][exercicio]['valor_total'] += valor_remessa
    
    # Processar valores
    if valores_remessa:
        stats['valores']['maior_valor_remessa'] = max(valores_remessa)
        stats['valores']['menor_valor_remessa'] = min(valores_remessa)
    else:
        stats['valores']['menor_valor_remessa'] = 0
    
    # Processar datas
    if datas_reconhecimento:
        # Normalizar todas as datas para remover timezone antes de ordenar
        datas_normalizadas = []
        for data in datas_reconhecimento:
            if hasattr(data, 'replace') and data.tzinfo:
                datas_normalizadas.append(data.replace(tzinfo=None))
            else:
                datas_normalizadas.append(data)
        
        datas_normalizadas.sort()
        stats['datas']['primeira_remessa'] = datas_normalizadas[0].strftime('%d/%m/%Y')
        stats['datas']['ultima_remessa'] = datas_normalizadas[-1].strftime('%d/%m/%Y')
        
        if len(datas_normalizadas) > 1:
            diferenca_total = (datas_normalizadas[-1] - datas_normalizadas[0]).days
            stats['datas']['tempo_medio_reconhecimento'] = diferenca_total / len(datas_normalizadas)
    
    # Converter defaultdicts para dicts normais
    stats['por_exercicio'] = dict(stats['por_exercicio'])
    stats['por_fase'] = dict(stats['por_fase'])
    
    return stats

def gerar_dados_fases_tempo():
    """Gera dados para gráfico de fases ao longo do tempo"""
    dados = []
    
    for remessa in dados_analise['remessasAnalisadas']:
        for fase in remessa['fasesComReconhecimento']:
            if fase.get('dataReconhecimento'):
                try:
                    data = datetime.fromisoformat(fase['dataReconhecimento'].replace('Z', '+00:00'))
                    dados.append({
                        'data': data.strftime('%Y-%m'),
                        'fase': fase['fase'],
                        'remessa': remessa['remessa'],
                        'valor': fase.get('valorReconhecido', 0)
                    })
                except:
                    pass
    
    return dados

def gerar_dados_valores_remessa():
    """Gera dados para gráfico de valores por remessa"""
    dados = []
    is_detalhada = dados_analise.get('tipoAnalise') == 'DETALHADA'
    
    for remessa in dados_analise['remessasAnalisadas']:
        if is_detalhada and 'consolidacaoRemessa' in remessa:
            # Análise detalhada - usar consolidação
            consolidacao = remessa['consolidacaoRemessa']['valores']
            dados.append({
                'remessa': remessa['remessa'],
                'exercicio': remessa['exercicio'],
                'periodo': remessa['periodo'],
                'valorReconhecido': consolidacao['reconhecido'],
                'valorLancamentoTotal': consolidacao['lancamentoTotal'],
                'valorNaoReconhecido': consolidacao['naoReconhecido'],
                'valorRecusado': consolidacao['recusado'],
                'valorNaoPassivelReconhecimento': consolidacao['naoPassivelReconhecimento'],
                'fases': len(remessa['fasesComReconhecimento'])
            })
        else:
            # Análise simplificada - usar soma das fases
            valor_total = sum(fase.get('valorReconhecido', 0) for fase in remessa['fasesComReconhecimento'])
            dados.append({
                'remessa': remessa['remessa'],
                'exercicio': remessa['exercicio'],
                'periodo': remessa['periodo'],
                'valorReconhecido': valor_total,
                'valorLancamentoTotal': valor_total,
                'valorNaoReconhecido': 0,
                'valorRecusado': 0,
                'valorNaoPassivelReconhecimento': 0,
                'fases': len(remessa['fasesComReconhecimento'])
            })
    
    return dados

def gerar_dados_distribuicao_fases():
    """Gera dados para gráfico de distribuição de fases"""
    contadores = Counter()
    
    for remessa in dados_analise['remessasAnalisadas']:
        for fase in remessa['fasesComReconhecimento']:
            contadores[fase['fase']] += 1
    
    return [{'fase': fase, 'count': count} for fase, count in contadores.items()]

# def gerar_dados_timeline():
#     """Gera dados para timeline de reconhecimentos"""
#     timeline = []
#     is_detalhada = dados_analise.get('tipoAnalise') == 'DETALHADA'
    
#     for remessa in dados_analise['remessasAnalisadas']:
#         for fase in remessa['fasesComReconhecimento']:
#             if fase.get('dataReconhecimento'):
#                 # Para análise detalhada, usar valores da consolidação da fase
#                 if is_detalhada and 'consolidacao' in fase:
#                     valor = fase['consolidacao']['valores']['reconhecido']
#                 else:
#                     # Para análise simplificada, usar valor da fase (se existir)
#                     valor = fase.get('valorReconhecido', 0)
                
#                 timeline.append({
#                     'data': fase['dataReconhecimento'],
#                     'remessa': remessa['remessa'],
#                     'fase': fase['fase'],
#                     'valor': valor,
#                     'exercicio': remessa['exercicio'],
#                     'periodo': remessa['periodo']
#                 })
    
#     # Ordenar por data
#     timeline.sort(key=lambda x: x['data'])
    
#     return timeline
def gerar_dados_timeline():
    """Gera dados para timeline de reconhecimentos"""
    timeline = []
    is_detalhada = dados_analise.get('tipoAnalise') == 'DETALHADA'
    
    for remessa in dados_analise['remessasAnalisadas']:
        for fase in remessa['fasesComReconhecimento']:
            if fase.get('dataReconhecimento'):
                if is_detalhada and 'consolidacao' in fase:
                    valor = fase['consolidacao']['valores']['reconhecido']
                    quantidade_itens = fase['consolidacao']['contadores']['total']
                else:
                    valor = fase.get('valorReconhecido', 0)
                    quantidade_itens = 1  # Para análise simplificada
                
                timeline.append({
                    'data': fase['dataReconhecimento'],
                    'remessa': remessa['remessa'],
                    'fase': fase['fase'],
                    'valor': valor,
                    'quantidadeItens': quantidade_itens,
                    'exercicio': remessa['exercicio'],
                    'periodo': remessa['periodo']
                })
    
    timeline.sort(key=lambda x: x['data'])
    return timeline

@portal_bp.route('/api/remessa-detalhada/<remessa_id>')
def remessa_detalhada(remessa_id):
    """API para obter detalhes completos de uma remessa"""
    global dados_analise
    
    if dados_analise is None:
        return jsonify({'error': 'Nenhum dado carregado'}), 400
    
    # Buscar remessa específica
    remessa_encontrada = None
    for remessa in dados_analise['remessasAnalisadas']:
        if remessa['id'] == remessa_id:
            remessa_encontrada = remessa
            break
    
    if not remessa_encontrada:
        return jsonify({'error': 'Remessa não encontrada'}), 404
    
    # Preparar dados detalhados
    detalhes = {
        'informacoes_basicas': {
            'id': remessa_encontrada['id'],
            'remessa': remessa_encontrada['remessa'],
            'contratoCPP': remessa_encontrada['contratoCPP'],
            'campo': remessa_encontrada['campo'],
            'exercicio': remessa_encontrada['exercicio'],
            'periodo': remessa_encontrada['periodo'],
            'mesAnoReferencia': remessa_encontrada['mesAnoReferencia'],
            'faseRemessaAtual': remessa_encontrada['faseRemessaAtual'],
            'origemDoGasto': remessa_encontrada['origemDoGasto'],
            'gastosCompartilhados': remessa_encontrada['gastosCompartilhados'],
            'fatorAlocacao':   remessa_encontrada.get('fatorAlocacao', 'INDEFINIDO'),
            'version': remessa_encontrada.get('version', 0)
        },
        'fases': [],
        'consolidacao_remessa': remessa_encontrada.get('consolidacaoRemessa'),
        'resumo_ccos': {
            'total_ccos': len(remessa_encontrada['fasesComReconhecimento']),
            'ccos_encontradas': 0,
            'valor_total_ccos': 0,
            'overhead_total': 0,
            'correcoes_monetarias_total': 0
        }
    }
    
    # Processar fases
    for fase in remessa_encontrada['fasesComReconhecimento']:
        fase_detalhada = {
            'fase': fase['fase'],
            'faseOriginal': fase['faseOriginal'],
            'dataReconhecimento': fase.get('dataReconhecimento'),
            'dataLancamento': fase.get('dataLancamento'),
            'consolidacao': fase.get('consolidacao'),
            'cco': None
        }
        
        # Processar CCO se existir
        if fase.get('cco') and fase['cco'].get('statusCCO') == 'ENCONTRADA':
            detalhes['resumo_ccos']['ccos_encontradas'] += 1
            
            cco_data = fase['cco']
            fase_detalhada['cco'] = {
                'id': cco_data.get('id'),
                'statusCCO': cco_data.get('statusCCO'),
                'informacoes_basicas': {
                    'contratoCpp': cco_data.get('contratoCpp'),
                    'campo': cco_data.get('campo'),
                    'remessa': cco_data.get('remessa'),
                    'faseRemessa': cco_data.get('faseRemessa'),
                    'exercicio': cco_data.get('exercicio'),
                    'periodo': cco_data.get('periodo')
                },
                'valores_atuais': extrair_valores_cco(cco_data),
                'correcao_monetaria': {
                    'tipo': cco_data.get('tipo'),
                    'subTipo': cco_data.get('subTipo'),
                    'dataCorrecao': cco_data.get('dataCorrecao'),
                    'taxaCorrecao': validar_e_converter_valor_monetario(cco_data.get('taxaCorrecao', 0)),
                    'igpmAcumulado': validar_e_converter_valor_monetario(cco_data.get('igpmAcumulado', 0)),
                    'diferencaValor': validar_e_converter_valor_monetario(cco_data.get('diferencaValor', 0))
                },
                'datas': {
                    'dataReconhecimento': cco_data.get('dataReconhecimento'),
                    'dataLancamento': cco_data.get('dataLancamento'),
                    'dataCorrecao': cco_data.get('dataCorrecao')
                }
            }
            
            # Atualizar resumo
            valores = fase_detalhada['cco']['valores_atuais']
            detalhes['resumo_ccos']['valor_total_ccos'] += valores.get('valorReconhecidoComOH', 0)
            detalhes['resumo_ccos']['overhead_total'] += valores.get('overHeadTotal', 0)
            detalhes['resumo_ccos']['correcoes_monetarias_total'] += validar_e_converter_valor_monetario(cco_data.get('diferencaValor', 0))
        
        detalhes['fases'].append(fase_detalhada)
    
    return jsonify(detalhes)

@portal_bp.route('/api/cco-detalhada/<cco_id>')
def cco_detalhada(cco_id):
    """API para obter detalhes completos de uma CCO"""
    global dados_analise
    
    if dados_analise is None:
        return jsonify({'error': 'Nenhum dado carregado'}), 400
    
    # Buscar CCO específica
    cco_encontrada = None
    remessa_origem = None
    fase_origem = None
    
    for remessa in dados_analise['remessasAnalisadas']:
        for fase in remessa['fasesComReconhecimento']:
            if fase.get('cco') and fase['cco'].get('id') == cco_id:
                cco_encontrada = fase['cco']
                remessa_origem = remessa
                fase_origem = fase
                break
        if cco_encontrada:
            break
    
    if not cco_encontrada:
        return jsonify({'error': 'CCO não encontrada'}), 404
    
    # Preparar dados detalhados da CCO
    detalhes = {
        'informacoes_basicas': {
            'id': cco_encontrada.get('id'),
            'contratoCpp': cco_encontrada.get('contratoCpp') or cco_encontrada.get('contrato') or remessa_origem['contratoCPP'],
            'campo': cco_encontrada.get('campo') or remessa_origem['campo'],
            'remessa': cco_encontrada.get('remessa') or remessa_origem['remessa'],
            'faseRemessa': cco_encontrada.get('faseRemessa'),
            'exercicio': cco_encontrada.get('exercicio') or remessa_origem['exercicio'],
            'periodo': cco_encontrada.get('periodo') or remessa_origem['periodo'],
            'statusCCO': cco_encontrada.get('statusCCO')
        },
        'valores_originais': extrair_valores_originais_cco(cco_encontrada),
        'valores_atuais': extrair_valores_cco(cco_encontrada),
        'correcao_monetaria': {
            'aplicada': bool(cco_encontrada.get('tipo')),
            'tipo': cco_encontrada.get('tipo'),
            'subTipo': cco_encontrada.get('subTipo'),
            'dataCorrecao': cco_encontrada.get('dataCorrecao'),
            'dataCriacaoCorrecao': cco_encontrada.get('dataCriacaoCorrecao'),
            'taxaCorrecao': validar_e_converter_valor_monetario(cco_encontrada.get('taxaCorrecao', 0)),
            'igpmAcumulado': validar_e_converter_valor_monetario(cco_encontrada.get('igpmAcumulado', 0)),
            'igpmAcumuladoReais': validar_e_converter_valor_monetario(cco_encontrada.get('igpmAcumuladoReais', 0)),
            'diferencaValor': validar_e_converter_valor_monetario(cco_encontrada.get('diferencaValor', 0)),
            'ativo': cco_encontrada.get('ativo', False)
        },
        'datas': {
            'dataReconhecimento': cco_encontrada.get('dataReconhecimento'),
            'dataLancamento': cco_encontrada.get('dataLancamento'),
            'dataCorrecao': cco_encontrada.get('dataCorrecao'),
            'dataCriacaoCorrecao': cco_encontrada.get('dataCriacaoCorrecao')
        },
        'contexto_remessa': {
            'remessaId': remessa_origem['id'],
            'remessaNumero': remessa_origem['remessa'],
            'exercicio': remessa_origem['exercicio'],
            'periodo': remessa_origem['periodo'],
            'mesAnoReferencia': remessa_origem['mesAnoReferencia'],
            'fase': fase_origem['fase'],
            'consolidacao_fase': fase_origem.get('consolidacao')
        },
        'transferencia': cco_encontrada.get('transferencia', ''),
        'observacoes': cco_encontrada.get('observacao', '')
    }
    
    return jsonify(detalhes)

def extrair_valores_cco(cco_data):
    """Extrai valores atuais da CCO"""
    return {
        'quantidadeLancamento': cco_data.get('quantidadeLancamento', 0),
        'valorLancamentoTotal': validar_e_converter_valor_monetario(cco_data.get('valorLancamentoTotal', 0)),
        'valorReconhecido': validar_e_converter_valor_monetario(cco_data.get('valorReconhecido', 0)),
        'valorReconhecidoComOH': validar_e_converter_valor_monetario(cco_data.get('valorReconhecidoComOH', 0)),
        'valorNaoReconhecido': validar_e_converter_valor_monetario(cco_data.get('valorNaoReconhecido', 0)),
        'valorReconhecivel': validar_e_converter_valor_monetario(cco_data.get('valorReconhecivel', 0)),
        'valorReconhecidoExploracao': validar_e_converter_valor_monetario(cco_data.get('valorReconhecidoExploracao', 0)),
        'valorReconhecidoProducao': validar_e_converter_valor_monetario(cco_data.get('valorReconhecidoProducao', 0)),
        'overHeadExploracao': validar_e_converter_valor_monetario(cco_data.get('overHeadExploracao', 0)),
        'overHeadProducao': validar_e_converter_valor_monetario(cco_data.get('overHeadProducao', 0)),
        'overHeadTotal': validar_e_converter_valor_monetario(cco_data.get('overHeadTotal', 0)),
        'valorNaoPassivelRecuperacao': validar_e_converter_valor_monetario(cco_data.get('valorNaoPassivelRecuperacao', 0)),
        'valorReconhecidoComOhOriginal': validar_e_converter_valor_monetario(cco_data.get('valorReconhecidoComOhOriginal', 0)),
        'flgRecuperado': cco_data.get('flgRecuperado', False),
        'transferencia': cco_data.get('transferencia', '')
    }
    
def extrair_valores_originais_cco(cco_data):
    """Extrai valores originais da CCO (antes das correções)"""
    # Para este exemplo, assumindo que os valores no JSON já são os corrigidos
    # Em um cenário real, teríamos que buscar os valores antes da correção
    return {
        'valorReconhecidoOriginal': validar_e_converter_valor_monetario(cco_data.get('valorReconhecido', 0)),
        'valorReconhecidoComOHOriginal': validar_e_converter_valor_monetario(cco_data.get('valorReconhecidoComOhOriginal', 0))
    }

@portal_bp.route('/api/remessas-detalhadas')
def remessas_detalhadas():
    """API para tabela detalhada de remessas"""
    global dados_analise
    
    if dados_analise is None:
        return jsonify({'error': 'Nenhum dado carregado'}), 400
    
    detalhes = []
    is_detalhada = dados_analise.get('tipoAnalise') == 'DETALHADA'
    
    for remessa in dados_analise['remessasAnalisadas']:
        for fase in remessa['fasesComReconhecimento']:
            detalhe = {
                'remessaId': remessa['id'],
                'remessa': remessa['remessa'],
                'exercicio': int(remessa['exercicio']),
                'periodo': int(remessa['periodo']),
                'mesAnoReferencia': remessa['mesAnoReferencia'],
                'contratoCPP': remessa['contratoCPP'],
                'campo': remessa['campo'],
                'fase': fase['fase'],
                'dataReconhecimento': fase.get('dataReconhecimento', ''),
                'dataLancamento': fase.get('dataLancamento', ''),
                'ccoEncontrada': 'Sim' if fase.get('cco') and fase['cco'].get('statusCCO') == 'ENCONTRADA' else 'Não',
                'ccoId': fase.get('cco', {}).get('id', ''),
                'statusCCO': fase.get('cco', {}).get('statusCCO', 'N/A'),
                'observacao': fase.get('cco', {}).get('observacao', '')
            }
            
            if fase.get('cco') and fase['cco'].get('statusCCO') == "CCOS_DUPLICADAS":
                detalhe["ccoEncontrada"] = "DUPLICADAS"
                # Para CSV, usar formato simples sem HTML
                itens = [item.strip() for item in fase.get('cco', {}).get('id', '').split("|") if item.strip()]
                detalhe['statusCCO'] = ' | '.join(itens)  # Formato CSV-friendly
                
            # Valores específicos para análise detalhada
            if is_detalhada and 'consolidacao' in fase:
                consolidacao = fase['consolidacao']
                detalhe.update({
                    'valorReconhecidoFase': consolidacao['valores']['reconhecido'],
                    'valorLancamentoTotalFase': consolidacao['valores']['lancamentoTotal'],
                    'quantidadeGastosFase': consolidacao['contadores']['total'],
                    'reconhecidosAutomaticosFase': int(consolidacao['contadores']['recAutomatico']) or 0,
                    'classificacoesTop': extrair_top_classificacoes(consolidacao['classificacoes']),
                    'responsaveisTop': extrair_top_responsaveis(consolidacao['responsaveis']),
                    'moedasUtilizadas': ' | '.join(consolidacao['moedas'].keys()) if consolidacao['moedas'] else ''
                })
            else:
                # Análise simplificada
                detalhe.update({
                    'valorReconhecidoFase': fase.get('valorReconhecido', 0),
                    'valorLancamentoTotalFase': 0,
                    'quantidadeGastosFase': 0,
                    'reconhecidosAutomaticosFase': 0,
                    'classificacoesTop': '',
                    'responsaveisTop': '',
                    'moedasUtilizadas': ''
                })
            
            # Processar valores CCO
            if fase.get('cco') and fase['cco'].get('statusCCO') == 'ENCONTRADA':
                cco = fase['cco']
                detalhe.update({
                    'valorCCO': validar_e_converter_valor_monetario(cco.get('valorReconhecido', 0)),
                    'valorCCOComOH': validar_e_converter_valor_monetario(cco.get('valorReconhecidoComOH', 0)),
                    'overheadTotal': validar_e_converter_valor_monetario(cco.get('overHeadTotal', 0)),
                    'correcaoMonetaria': validar_e_converter_valor_monetario(cco.get('diferencaValor', 0)),
                    'taxaCorrecao': validar_e_converter_valor_monetario(cco.get('taxaCorrecao', 0)),
                    'tipoCorrecao': cco.get('tipo', ''),
                    'dataCorrecao': cco.get('dataCorrecao', '')
                })
            else:
                detalhe.update({
                    'valorCCO': 0,
                    'valorCCOComOH': 0,
                    'overheadTotal': 0,
                    'correcaoMonetaria': 0,
                    'taxaCorrecao': 0,
                    'tipoCorrecao': '',
                    'dataCorrecao': ''
                })
            
            detalhes.append(detalhe)
    
    return jsonify(detalhes)

# def extrair_top_classificacoes(classificacoes, top=3):
#     """Extrai as top classificações para exibição"""
#     if not classificacoes:
#         return ''
    
#     sorted_class = sorted(classificacoes.items(), key=lambda x: x[1], reverse=True)[:top]
#     return ', '.join([f"{k}({v})" for k, v in sorted_class])

# def extrair_top_responsaveis(responsaveis, top=3):
#     """Extrai os top responsáveis para exibição"""
#     if not responsaveis:
#         return ''
    
#     sorted_resp = sorted(responsaveis.items(), key=lambda x: x[1], reverse=True)[:top]
#     return ', '.join([f"{k}({v})" for k, v in sorted_resp])

def extrair_top_classificacoes(classificacoes, top=3):
    """Extrai as top classificações para exibição em formato CSV-friendly"""
    if not classificacoes:
        return ''
    
    sorted_class = sorted(classificacoes.items(), key=lambda x: x[1], reverse=True)[:top]
    # Usar formato mais simples sem parênteses problemáticos
    return ' | '.join([f"{k}:{v}" for k, v in sorted_class])

def extrair_top_responsaveis(responsaveis, top=3):
    """Extrai os top responsáveis para exibição em formato CSV-friendly"""
    if not responsaveis:
        return ''
    
    sorted_resp = sorted(responsaveis.items(), key=lambda x: x[1], reverse=True)[:top]
    # Usar formato mais simples sem parênteses problemáticos
    return ' | '.join([f"{k}:{v}" for k, v in sorted_resp])

from bson import ObjectId
from pymongo import MongoClient
import os

# Configuração do MongoDB
client = MongoClient(MONGO_URI)
db = client.sgppServices

@portal_bp.route('/cco-timeline/<cco_id>')
def cco_timeline(cco_id):
    """Página de timeline completa da CCO"""
    try:
        # Buscar CCO completa no MongoDB
        cco_completa = db.conta_custo_oleo_entity.find_one({"_id": cco_id})
        
        if not cco_completa:
            return render_template('erro.html', 
                                 erro="CCO não encontrada", 
                                 mensagem=f"CCO com ID {cco_id} não foi encontrada no banco de dados.")
        
        # Processar timeline
        timeline_data = processar_timeline_cco(cco_completa)
        
        # Extrair valores atuais (última correção ou valores da raiz)
        valores_atuais = extrair_valores_atuais_cco(cco_completa)
        
        return render_template('cco_timeline.html', 
                             cco=cco_completa,
                             timeline=timeline_data,
                             valores_atuais=valores_atuais,
                             cco_json=json.dumps(cco_completa, indent=2, default=str),
                             titulo=f"Timeline CCO - {cco_id}")
                             
    except Exception as e:
        logger.error(f"Erro ao carregar timeline da CCO {cco_id}: {e}")
        return render_template('erro.html', 
                             erro="Erro interno", 
                             mensagem="Erro ao carregar dados da CCO.")

def extrair_valores_atuais_cco(cco_data):
    """Extrai valores atuais da CCO (última correção ou valores da raiz)"""
    valores_atuais = {}
    
    dataLancamento = formatar_data_brasileira(cco_data.get('dataLancamento'))
    dataReconhecimento = formatar_data_brasileira(cco_data.get('dataReconhecimento'))
    valores_atuais['data_lancamento'] = formatar_data_brasileira(dataLancamento)
    valores_atuais['data_reconhecimento'] = formatar_data_brasileira(dataReconhecimento)
    
    # Se tem correções monetárias, usar a última
    if 'correcoesMonetarias' in cco_data and cco_data['correcoesMonetarias']:
        ultima_correcao = cco_data['correcoesMonetarias'][-1]
        fonte = ultima_correcao
        dataAtualizacao =ultima_correcao.get('dataCorrecao') or ultima_correcao.get('dataCriacaoCorrecao')
        dataCorrecao = ultima_correcao.get('dataCorrecao')
        dataCriacaoCorrecao = ultima_correcao.get('dataCriacaoCorrecao')
        valores_atuais['fonte'] = f"Última correção ({ultima_correcao.get('tipo', 'N/A')})"
        valores_atuais['data_atualizacao'] = formatar_data_brasileira(dataAtualizacao)
        valores_atuais['data_correcao'] = formatar_data_brasileira(dataCorrecao)
        valores_atuais['data_criacao_correcao'] = formatar_data_brasileira(dataCriacaoCorrecao)
        
        
    else:
        # Usar valores da raiz da CCO
        fonte = cco_data
        valores_atuais['fonte'] = "Valores originais da CCO"
        #valores_atuais['data_atualizacao'] = cco_data.get('dataLancamento')
    
    # Extrair valores principais
    valores_atuais.update({
        'valorReconhecido': converter_decimal128_para_float(fonte.get('valorReconhecido', 0)),
        'valorReconhecidoComOH': converter_decimal128_para_float(fonte.get('valorReconhecidoComOH', 0)),
        'overHeadTotal': converter_decimal128_para_float(fonte.get('overHeadTotal', 0)),
        'valorLancamentoTotal': converter_decimal128_para_float(fonte.get('valorLancamentoTotal', 0)),
        'valorReconhecivel': converter_decimal128_para_float(fonte.get('valorReconhecivel', 0)),
        'valorNaoReconhecido': converter_decimal128_para_float(fonte.get('valorNaoReconhecido', 0)),
        'valorNaoPassivelRecuperacao': converter_decimal128_para_float(fonte.get('valorNaoPassivelRecuperacao', 0)),
        'quantidadeLancamento': fonte.get('quantidadeLancamento', 0),
        'flgRecuperado': fonte.get('flgRecuperado', False),
        
        # Valores extras (podem não existir)
        'valorRecuperado': converter_decimal128_para_float(fonte.get('valorRecuperado', 0)),
        'valorRecuperadoTotal': converter_decimal128_para_float(fonte.get('valorRecuperadoTotal', 0)),
        'taxaCorrecao': converter_decimal128_para_float(fonte.get('taxaCorrecao', 0)),
        'igpmAcumulado': converter_decimal128_para_float(fonte.get('igpmAcumulado', 0)),
        'igpmAcumuladoReais': converter_decimal128_para_float(fonte.get('igpmAcumuladoReais', 0)),
        'diferencaValor': converter_decimal128_para_float(fonte.get('diferencaValor', 0)),
        'overHeadExploracao': converter_decimal128_para_float(fonte.get('overHeadExploracao', 0)),
        'overHeadProducao': converter_decimal128_para_float(fonte.get('overHeadProducao', 0)),
        'valorReconhecidoExploracao': converter_decimal128_para_float(fonte.get('valorReconhecidoExploracao', 0)),
        'valorReconhecidoProducao': converter_decimal128_para_float(fonte.get('valorReconhecidoProducao', 0)),
        
        # Informações adicionais
        'ativo': fonte.get('ativo', True),
        'subTipo': fonte.get('subTipo', ''),
        'transferencia': fonte.get('transferencia', False)
    })
    
    return valores_atuais

def processar_timeline_cco(cco_data):
    """Processa dados da CCO para gerar timeline"""
    timeline = []
    
    # Evento inicial - Criação da CCO
    timeline.append({
        'tipo': 'CRIACAO',
        'titulo': 'CCO Criada',
        #'dataCorrecao': cco_data.get('dataLancamento'),
        'descricao': f"CCO criada para remessa {cco_data.get('remessa')} - Fase {cco_data.get('faseRemessa')}",
        'valores': {
            'valorReconhecido': converter_decimal128_para_float(cco_data.get('valorReconhecido', 0)),
            'valorReconhecidoComOH': converter_decimal128_para_float(cco_data.get('valorReconhecidoComOH', 0)),
            'overHeadTotal': converter_decimal128_para_float(cco_data.get('overHeadTotal', 0)),
            'valorLancamentoTotal': converter_decimal128_para_float(cco_data.get('valorLancamentoTotal', 0)),
            'valorNaoReconhecido': converter_decimal128_para_float(cco_data.get('valorNaoReconhecido', 0)),
            'valorReconhecidoExploracao': converter_decimal128_para_float(cco_data.get('valorReconhecidoExploracao', 0)),
			'valorReconhecidoProducao': converter_decimal128_para_float(cco_data.get('valorReconhecidoProducao', 0)),
			'overHeadExploracao': converter_decimal128_para_float(cco_data.get('overHeadExploracao', 0)),
			'overHeadProducao': converter_decimal128_para_float(cco_data.get('overHeadProducao', 0)),
			'valorReconhecivel': converter_decimal128_para_float(cco_data.get('valorReconhecivel', 0)),
            'valorNaoPassivelRecuperacao': converter_decimal128_para_float(cco_data.get('valorNaoPassivelRecuperacao', 0)),
			'valorRecusado': converter_decimal128_para_float(cco_data.get('valorRecusado', 0)),
			'diferencaValor': converter_decimal128_para_float(cco_data.get('diferencaValor', 0))
        },
        'detalhes': {
            'dataLancamento': formatar_data_brasileira(cco_data.get('dataLancamento')),
            'dataReconhecimento': formatar_data_brasileira(cco_data.get('dataReconhecimento')),
            'contrato': cco_data.get('contratoCpp'),
            'campo': cco_data.get('campo', ''),
            'remessa': cco_data.get('remessa', ''),
            'remessaExposicao': cco_data.get('remessaExposicao', ''),
            'quantidadeLancamento': cco_data.get('quantidadeLancamento', 0),
            'anoReconhecimento': cco_data.get('anoReconhecimento', 0),
            'mesReconhecimento': cco_data.get('mesReconhecimento', 0),
            'mesAnoReferencia': cco_data.get('mesAnoReferencia', ''),
            'faseRemessa': cco_data.get('faseRemessa', ''),
            'faseRespostaGestora': cco_data.get('faseRespostaGestora', ''),
            'periodo': cco_data.get('periodo', 0),
            'version': cco_data.get('version', 0)
        },
        'icone': 'fas fa-plus-circle',
        'cor': 'success'
    })
    
    # Processar correções monetárias
    if 'correcoesMonetarias' in cco_data and cco_data['correcoesMonetarias']:
        for i, correcao in enumerate(cco_data['correcoesMonetarias']):
            evento = processar_evento_correcao(correcao, i + 1)
            timeline.append(evento)
    
    # Ordenar por data
    #timeline.sort(key=lambda x: x['data'] if x['data'] else '1900-01-01')
    # Ordenar por data em ordem DECRESCENTE (mais recentes primeiro)
    timeline.sort(key=lambda x: x['dataCorrecao'] if 'dataCorrecao' in x else '1900-01-01', reverse=True)
    
    return timeline

def processar_evento_correcao(correcao, sequencia):
    """Processa um evento de correção monetária"""
    tipo = correcao.get('tipo', 'DESCONHECIDO')
    
    # Mapear tipos para apresentação
    tipo_map = {
        'IPCA': {'titulo': 'Correção Monetária IPCA', 'icone': 'fas fa-chart-line', 'cor': 'info'},
        'IGPM': {'titulo': 'Correção Monetária IGPM', 'icone': 'fas fa-chart-line', 'cor': 'info'},
        'RECUPERACAO': {'titulo': 'Recuperação de Valor', 'icone': 'fas fa-download', 'cor': 'warning'},
        'INVALIDACAO_RECONHECIMENTO_PARCIAL': {'titulo': 'Invalidação Parcial', 'icone': 'fas fa-repeat', 'cor': 'dark'},
        'RETIFICACAO': {'titulo': 'Retificação Manual', 'icone': 'fas fa-edit', 'cor': 'secondary'}
    }
    
    config = tipo_map.get(tipo, {'titulo': tipo, 'icone': 'fas fa-question-circle', 'cor': 'secondary'})
    
    # Descrição baseada no tipo
    descricao = gerar_descricao_evento(tipo, correcao)
    
    # Extrair valores com tratamento seguro
    valores = {
        'valorReconhecido': converter_decimal128_para_float(correcao.get('valorReconhecido', 0)),
        'valorReconhecidoComOH': converter_decimal128_para_float(correcao.get('valorReconhecidoComOH', 0)),
        'overHeadTotal': converter_decimal128_para_float(correcao.get('overHeadTotal', 0)),
        'diferencaValor': converter_decimal128_para_float(correcao.get('diferencaValor', 0)),
        'valorRecuperado': converter_decimal128_para_float(correcao.get('valorRecuperado', 0)),
        'valorRecuperadoTotal': converter_decimal128_para_float(correcao.get('valorRecuperadoTotal', 0)),
        'taxaCorrecao': converter_decimal128_para_float(correcao.get('taxaCorrecao', 0)),
        'igpmAcumulado': converter_decimal128_para_float(correcao.get('igpmAcumulado', 0)),
        'igpmAcumuladoReais': converter_decimal128_para_float(correcao.get('igpmAcumuladoReais', 0)),
        'valorLancamentoTotal': converter_decimal128_para_float(correcao.get('valorLancamentoTotal', 0)),
        'valorNaoReconhecido': converter_decimal128_para_float(correcao.get('valorNaoReconhecido', 0)),
        'valorReconhecivel': converter_decimal128_para_float(correcao.get('valorReconhecivel', 0)),
        'valorNaoPassivelRecuperacao': converter_decimal128_para_float(correcao.get('valorNaoPassivelRecuperacao', 0))
    }
    
    # Extrair detalhes com tratamento seguro
    detalhes = {
        'subTipo': correcao.get('subTipo', ''),
        'ativo': correcao.get('ativo', True),
        'observacao': correcao.get('observacao', ''),
        'transferencia': correcao.get('transferencia', False),
        'quantidadeLancamento': correcao.get('quantidadeLancamento', 0),
        'contrato': correcao.get('contrato', ''),
        'campo': correcao.get('campo', ''),
        'faseRemessa': correcao.get('faseRemessa', '')
    }
    
    return {
        'tipo': tipo,
        'sequencia': sequencia,
        'dataCorrecao': correcao.get('dataCorrecao'),
        'dataCorrecaoFormatada': formatar_data_brasileira(correcao.get('dataCorrecao')),
        'dataCriacaoCorrecao': formatar_data_brasileira(correcao.get('dataCriacaoCorrecao')),
        'titulo': config['titulo'],
        'descricao': descricao,
        'valores': valores,
        'detalhes': detalhes,
        'icone': config['icone'],
        'cor': config['cor']
    }

def gerar_descricao_evento(tipo, correcao):
    """Gera descrição do evento baseada no tipo"""
    try:
        if tipo in ['IPCA', 'IGPM']:
            taxa = converter_decimal128_para_float(correcao.get('taxaCorrecao', 0))
            if taxa > 0:
                return f"Aplicação de correção monetária {tipo} - Taxa: {taxa:.4f}%"
            else:
                return f"Correção monetária {tipo} aplicada"
        elif tipo == 'RECUPERACAO':
            valor_recuperado = converter_decimal128_para_float(correcao.get('valorRecuperado', 0))
            valor_recuperado_total = converter_decimal128_para_float(correcao.get('valorRecuperadoTotal', 0))
            if valor_recuperado > 0:
                return f"Recuperação de valor: R$ {valor_recuperado:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            elif valor_recuperado_total > 0:
                return f"Recuperação total: R$ {valor_recuperado_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            else:
                return "Recuperação de valor realizada"
        elif tipo == 'INVALIDACAO_RECONHECIMENTO_PARCIAL':
            return "Invalidação parcial do reconhecimento com transferência de valores"
        elif tipo == 'RETIFICACAO':
            return "Ajuste manual realizado nos valores da CCO"
        else:
            return f"Evento do tipo {tipo} aplicado à CCO"
    except Exception as e:
        return f"Evento do tipo {tipo}"
    
@portal_bp.route('/pesquisa-ccos')
def pesquisa_ccos():
    """Página de pesquisa de CCOs"""
    return render_template('pesquisa_ccos.html', titulo="Pesquisa de CCOs")

@portal_bp.route('/api/pesquisar-ccos', methods=['POST'])
def api_pesquisar_ccos():
    """API para pesquisar CCOs por filtros"""
    try:
        dados = request.get_json()
        
        # Validar se contratoCPP está presente (obrigatório)
        if not dados.get('contratoCpp') and not dados.get('id'):
            return jsonify({'error': 'Filtros de pesquisa inválidos'}), 400
        
        # Construir filtro MongoDB
        filtro_mongo = {}
        
        # Se pesquisa por ID
        if dados.get('id'):
            filtro_mongo['_id'] = dados['id']
        else:
            # Pesquisa por filtros
            if dados.get('contratoCpp'):
                filtro_mongo['contratoCpp'] = dados['contratoCpp']
            if dados.get('campo'):
                filtro_mongo['campo'] = dados['campo']
            if dados.get('remessa'):
                filtro_mongo['remessa'] = int(dados['remessa'])
            if dados.get('faseRemessa'):
                filtro_mongo['faseRemessa'] = dados['faseRemessa']
            if dados.get('origemDosGastos'):
                filtro_mongo['origemDosGastos'] = dados['origemDosGastos']
            if dados.get('exercicio'):
                filtro_mongo['exercicio'] = int(dados['exercicio'])
            if dados.get('periodo'):
                filtro_mongo['periodo'] = int(dados['periodo'])
            if dados.get('flgRecuperado') is not None:
                filtro_mongo['flgRecuperado'] = dados['flgRecuperado']
        
        # Projeção para trazer apenas dados resumidos
        projecao = {
            '_id': 1,
            'contratoCpp': 1,
            'campo': 1,
            'remessa': 1,
            'remessaExposicao': 1,
            'faseRemessa': 1,
            'exercicio': 1,
            'periodo': 1,
            'mesAnoReferencia': 1,
            'mesReconhecimento': 1,
            'anoReconhecimento': 1,
            'origemDosGastos': 1,
            'flgRecuperado': 1,
            'dataLancamento': 1,
            'dataReconhecimento': 1,
            'quantidadeLancamento': 1,
            'valorReconhecido': 1,
            'valorReconhecidoComOH': 1,
            'overHeadTotal': 1,
            'correcoesMonetarias': {'$slice': -1}  # Apenas a última correção
        }
        
        # Executar consulta
        ccos = list(db.conta_custo_oleo_entity.find(filtro_mongo, projecao).limit(500))
        
        # Processar resultados
        resultados = []
        for cco in ccos:
            # Extrair valores atuais (da última correção ou raiz)
            valores_atuais = extrair_valores_resumidos_cco(cco)
            
            resultado = {
                'id': cco['_id'],
                'contratoCpp': cco.get('contratoCpp', ''),
                'campo': cco.get('campo', ''),
                'remessa': cco.get('remessa', 0),
                'remessaExposicao': cco.get('remessaExposicao', 0),
                'faseRemessa': cco.get('faseRemessa', ''),
                'exercicio': cco.get('exercicio', 0),
                'periodo': cco.get('periodo', 0),
                'mesAnoReferencia': cco.get('mesAnoReferencia', ''),
                'anoReconhecimento': cco.get('anoReconhecimento', 0),
                'mesReconhecimento': cco.get('mesReconhecimento', 0),
                'origemDosGastos': cco.get('origemDosGastos', ''),
                'flgRecuperado': cco.get('flgRecuperado', False),
                'dataLancamento': formatar_data_brasileira(cco.get('dataLancamento')),
                'dataReconhecimento': formatar_data_brasileira(cco.get('dataReconhecimento')),
                'quantidadeLancamento': cco.get('quantidadeLancamento', 0),
                'valorReconhecido': valores_atuais['valorReconhecido'],
                'valorReconhecidoComOH': valores_atuais['valorReconhecidoComOH'],
                'overHeadTotal': valores_atuais['overHeadTotal'],
                'temCorrecoes': len(cco.get('correcoesMonetarias', [])) > 0,
                'ultimaAtualizacao': valores_atuais['ultimaAtualizacao']
            }
            resultados.append(resultado)
        
        return jsonify({
            'success': True,
            'resultados': resultados,
            'total': len(resultados),
            'filtro_aplicado': filtro_mongo
        })
        
    except Exception as e:
        logger.error(f"Erro ao pesquisar CCOs: {e}")
        return jsonify({'error': f'Erro interno: {str(e)}'}), 500

def extrair_valores_resumidos_cco(cco_data):
    """Extrai valores resumidos da CCO para listagem"""
    valores = {
        'valorReconhecido': 0,
        'valorReconhecidoComOH': 0,
        'overHeadTotal': 0,
        'ultimaAtualizacao': ''
    }
    
    # Se tem correções, usar a última
    if 'correcoesMonetarias' in cco_data and cco_data['correcoesMonetarias']:
        ultima_correcao = cco_data['correcoesMonetarias'][-1]
        valores['valorReconhecido'] = converter_decimal128_para_float(ultima_correcao.get('valorReconhecido', 0))
        valores['valorReconhecidoComOH'] = converter_decimal128_para_float(ultima_correcao.get('valorReconhecidoComOH', 0))
        valores['overHeadTotal'] = converter_decimal128_para_float(ultima_correcao.get('overHeadTotal', 0))
        valores['ultimaAtualizacao'] = formatar_data_simples(ultima_correcao.get('dataCorrecao') or ultima_correcao.get('dataCriacaoCorrecao'))
    else:
        # Usar valores da raiz
        valores['valorReconhecido'] = converter_decimal128_para_float(cco_data.get('valorReconhecido', 0))
        valores['valorReconhecidoComOH'] = converter_decimal128_para_float(cco_data.get('valorReconhecidoComOH', 0))
        valores['overHeadTotal'] = converter_decimal128_para_float(cco_data.get('overHeadTotal', 0))
        valores['ultimaAtualizacao'] = formatar_data_simples(cco_data.get('dataLancamento'))
    
    return valores

@portal_bp.route('/api/contratos-disponiveis')
def api_contratos_disponiveis():
    """API para listar contratos disponíveis"""
    try:
        contratos = db.conta_custo_oleo_entity.distinct('contratoCpp')
        return jsonify({'contratos': sorted([c for c in contratos if c])})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@portal_bp.route('/api/campos-por-contrato/<contrato>')
def api_campos_por_contrato(contrato):
    """API para listar campos por contrato"""
    try:
        campos = db.conta_custo_oleo_entity.distinct('campo', {'contratoCpp': contrato})
        return jsonify({'campos': sorted([c for c in campos if c])})
    except Exception as e:
        return jsonify({'error': str(e)}), 500