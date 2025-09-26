from flask import Flask, Blueprint, render_template, request, jsonify, redirect, session, url_for
import json
import os
from datetime import datetime
from collections import Counter, defaultdict
import pandas as pd
import re
from decimal import Decimal
import logging

from app.config import MONGO_URI, MONGO_URI_PRD
from app.utils.converters import processar_json_mongodb, validar_e_converter_valor_monetario, converter_decimal128_para_float, formatar_data_brasileira, formatar_data_simples
from app.utils.cache_utils import CacheManager
from app.services.portal_service import PortalService

portal_bp = Blueprint('portal_ui', __name__)

logger = logging.getLogger(__name__)

dados_analise = None
portal_service = PortalService(MONGO_URI, MONGO_URI_PRD)

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
    stats = portal_service.processar_estatisticas(dados_analise)
    
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
        return jsonify(portal_service.gerar_dados_fases_tempo(dados_analise))
    elif tipo == 'valores_remessa':
        return jsonify(portal_service.gerar_dados_valores_remessa(dados_analise))
    elif tipo == 'distribuicao_fases':
        return jsonify(portal_service.gerar_dados_distribuicao_fases(dados_analise))
    elif tipo == 'timeline_reconhecimento':
        return jsonify(gerar_dados_timeline())
    
    return jsonify({'error': 'Tipo de gráfico não reconhecido'}), 400

def processar_estatisticas(dados):
    """Compat: delega para PortalService"""
    return portal_service.processar_estatisticas(dados)

def gerar_dados_fases_tempo():
    return portal_service.gerar_dados_fases_tempo(dados_analise)

def gerar_dados_valores_remessa():
    return portal_service.gerar_dados_valores_remessa(dados_analise)

def gerar_dados_distribuicao_fases():
    return portal_service.gerar_dados_distribuicao_fases(dados_analise)

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
        'valores_originais': portal_service.extrair_valores_originais_cco(cco_encontrada),
        'valores_atuais': portal_service.extrair_valores_cco(cco_encontrada),
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
    # Compatibilidade: delega para o serviço
    return portal_service.extrair_valores_cco(cco_data)

def extrair_valores_originais_cco(cco_data):
    # Compatibilidade: delega para o serviço
    return portal_service.extrair_valores_originais_cco(cco_data)

@portal_bp.route('/api/remessas-detalhadas')
def remessas_detalhadas():
    """API para tabela detalhada de remessas"""
    global dados_analise
    if dados_analise is None:
        return jsonify({'error': 'Nenhum dado carregado'}), 400
    detalhes = portal_service.remessas_detalhadas_list(dados_analise)
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

@portal_bp.route('/cco-timeline/<cco_id>')
def cco_timeline(cco_id):
    """Página de timeline completa da CCO"""
    # try:
    # Buscar CCO completa no MongoDB via serviço
    cco_completa = portal_service._get_db_prd().conta_custo_oleo_entity.find_one({"_id": cco_id})
    
    if not cco_completa:
        return render_template('erro.html', 
                                erro="CCO não encontrada", 
                                mensagem=f"CCO com ID {cco_id} não foi encontrada no banco de dados.")
    
    # Processar timeline
    timeline_data = portal_service.processar_timeline_cco(cco_completa)
    
    # Extrair valores atuais (última correção ou valores da raiz)
    valores_atuais = portal_service.extrair_valores_atuais_cco(cco_completa)
    
    return render_template('cco_timeline.html', 
                            cco=cco_completa,
                            timeline=timeline_data,
                            valores_atuais=valores_atuais,
                            cco_json=json.dumps(cco_completa, indent=2, default=str),
                            titulo=f"Timeline CCO - {cco_id}")
                             
    # except Exception as e:
    #     logger.error(f"Erro ao carregar timeline da CCO {cco_id}: {e}")
    #     return render_template('erro.html', 
    #                          erro="Erro interno", 
    #                          mensagem="Erro ao carregar dados da CCO.")

def extrair_valores_atuais_cco(cco_data):
    # Compatibilidade: delega para o serviço
    return portal_service.extrair_valores_atuais_cco(cco_data)

def processar_timeline_cco(cco_data):
    # Compatibilidade: delega para o serviço
    return portal_service.processar_timeline_cco(cco_data)

def processar_evento_correcao(correcao, sequencia):
    # Compatibilidade: delega para o serviço
    return portal_service.processar_evento_correcao(correcao, sequencia)

def gerar_descricao_evento(tipo, correcao):
    # Compatibilidade: delega para o serviço
    return portal_service.gerar_descricao_evento(tipo, correcao)
    
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
        
        resultados = portal_service.pesquisar_ccos(filtro_mongo)
        
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
    # Compatibilidade: delega para o serviço
    return portal_service.extrair_valores_resumidos_cco(cco_data)

@portal_bp.route('/api/contratos-disponiveis')
def api_contratos_disponiveis():
    """API para listar contratos disponíveis"""
    try:
        contratos = portal_service.listar_contratos()
        return jsonify({'contratos': contratos})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@portal_bp.route('/api/campos-por-contrato/<contrato>')
def api_campos_por_contrato(contrato):
    """API para listar campos por contrato"""
    try:
        campos = portal_service.listar_campos_por_contrato(contrato)
        return jsonify({'campos': campos})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
