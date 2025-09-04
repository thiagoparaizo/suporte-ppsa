import re, json
import os
import logging
from decimal import Decimal
from flask import current_app
from bson.decimal128 import Decimal128

logger = logging.getLogger(__name__)

def processar_json_mongodb(content):
    """
    Processa JSON que contém tipos BSON do MongoDB convertendo para tipos Python padrão
    """
    
    #logger.info("Iniciando processamento de JSON com tipos BSON")
    
    # Dicionário de substituições para tipos BSON
    substituicoes_bson = [
        # NumberDecimal("123.45") -> 123.45
        (r'NumberDecimal\("([^"]+)"\)', lambda m: str(float(m.group(1)))),
        
        # NumberLong(123) -> 123
        (r'NumberLong\((\d+)\)', lambda m: m.group(1)),
        
        # ObjectId("...") -> "..."
        (r'ObjectId\("([^"]+)"\)', lambda m: f'"{m.group(1)}"'),
        
        # ISODate("...") -> "..."
        (r'ISODate\("([^"]+)"\)', lambda m: f'"{m.group(1)}"'),
        
        # Outros tipos BSON comuns
        (r'BinData\([^)]+\)', '"BINARY_DATA"'),
        (r'UUID\("([^"]+)"\)', lambda m: f'"{m.group(1)}"'),
        (r'Timestamp\([^)]+\)', '"TIMESTAMP"'),
        
        # Tratar casos especiais de NumberDecimal com notação científica
        (r'NumberDecimal\("([^"]*[Ee][+-]?\d+[^"]*)"\)', lambda m: str(float(m.group(1)))),
        
        # Tratar NumberDecimal("0E-15") -> 0
        (r'NumberDecimal\("0E[^"]*"\)', '0'),
        
        # Tratar NumberDecimal vazio ou inválido
        (r'NumberDecimal\(""\)', '0'),
        (r'NumberDecimal\(\)', '0'),
    ]
    
    # Aplicar substituições
    content_processado = content
    
    for i, (padrao, substituicao) in enumerate(substituicoes_bson):
        try:
            if callable(substituicao):
                matches = len(re.findall(padrao, content_processado))
                content_processado = re.sub(padrao, substituicao, content_processado)
                if matches > 0:
                    logger.info(f"Substituição {i+1}: {matches} ocorrências processadas")
            else:
                matches = len(re.findall(padrao, content_processado))
                content_processado = re.sub(padrao, substituicao, content_processado)
                if matches > 0:
                    logger.info(f"Substituição {i+1}: {matches} ocorrências processadas")
        except Exception as e:
            logger.warning(f"Erro na substituição {i+1}: {e}")
            continue
    
    # Tentar fazer parse do JSON
    try:
        logger.info("Tentando parse do JSON processado")
        dados = json.loads(content_processado)
        
        # Pós-processamento para garantir tipos corretos
        logger.info("Aplicando limpeza recursiva dos tipos")
        dados_limpos = limpar_tipos_recursivo(dados)
        
        logger.info("JSON processado com sucesso")
        return dados_limpos
        
    except json.JSONDecodeError as e:
        # Se ainda houver erro, tentar uma abordagem mais agressiva
        logger.warning(f"Erro inicial no JSON: {e}")
        logger.info("Tentando correção mais agressiva...")
        
        # Salvar versão processada para debug (apenas em desenvolvimento)
        if current_app.debug:
            try:
                debug_file = os.path.join(current_app.config['UPLOAD_FOLDER'], 'debug_json_processado.txt')
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(content_processado[:10000])  # Primeiros 10000 caracteres
                logger.info(f"Arquivo de debug salvo em: {debug_file}")
            except Exception as debug_e:
                print(f"Erro ao salvar arquivo de debug: {debug_e}")
                logger.warning(f"Não foi possível salvar arquivo de debug: {debug_e}")
        
        content_corrigido = corrigir_json_agressivo(content_processado)
        dados = json.loads(content_corrigido)
        dados_limpos = limpar_tipos_recursivo(dados)
        
        logger.info("JSON corrigido e processado com sucesso")
        return dados_limpos

def limpar_tipos_recursivo(obj):
    """
    Percorre recursivamente o objeto convertendo strings numéricas em números
    e limpando outros tipos problemáticos
    """
    if isinstance(obj, dict):
        return {key: limpar_tipos_recursivo(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [limpar_tipos_recursivo(item) for item in obj]
    elif isinstance(obj, str):
        # Tentar converter strings que parecem números
        if obj.replace('.', '').replace('-', '').replace('+', '').replace('e', '').replace('E', '').isdigit():
            try:
                # Se tem ponto ou notação científica, é float
                if '.' in obj or 'e' in obj.lower():
                    return float(obj)
                else:
                    return int(obj)
            except ValueError:
                return obj
        return obj
    else:
        return obj

def corrigir_json_agressivo(content):
    """
    Correções mais agressivas para JSON problemático
    """
    
    # Correções adicionais para casos complexos
    correcoes_extras = [
        # Remover vírgulas duplas
        (r',,+', ','),
        
        # Corrigir arrays vazios mal formados
        (r'\[\s*,', '['),
        (r',\s*\]', ']'),
        
        # Corrigir objetos vazios mal formados
        (r'{\s*,', '{'),
        (r',\s*}', '}'),
        
        # Remover trailing commas
        (r',(\s*[}\]])', r'\1'),
        
        # Corrigir aspas não fechadas (caso básico)
        (r':\s*"([^"]*)"([^,}\]]*)"', r': "\1\2"'),
        
        # Tratar valores undefined ou null mal formatados
        (r':\s*undefined', ': null'),
        (r':\s*None', ': null'),
        
        # Corrigir números mal formatados
        (r':\s*(\d+\.?\d*)\s*([,}\]])', r': \1\2'),
    ]
    
    content_corrigido = content
    for padrao, substituicao in correcoes_extras:
        content_corrigido = re.sub(padrao, substituicao, content_corrigido)
    
    return content_corrigido

def validar_e_converter_valor_monetario(valor_str):
    """
    Converte valores monetários que podem estar em formato NumberDecimal
    """
    if not valor_str:
        return 0.0
    
    # Se já é um número
    if isinstance(valor_str, (int, float)):
        return float(valor_str)
    
    # Se é string, tentar extrair número
    if isinstance(valor_str, str):
        # Remover qualquer wrapper NumberDecimal se ainda existir
        if 'NumberDecimal' in valor_str:
            # Extrair valor entre aspas
            match = re.search(r'"([^"]+)"', valor_str)
            if match:
                valor_str = match.group(1)
        
        # Converter para float
        try:
            return float(valor_str)
        except ValueError:
            return 0.0
    
    return 0.0

def converter_decimal128_para_float(valor):
    """
    Converte especificamente Decimal128 do MongoDB para float
    """
    if valor is None:
        return 0.0
    
    if isinstance(valor, Decimal128):
        return float(valor.to_decimal())
    elif isinstance(valor, (int, float)):
        return float(valor)
    elif isinstance(valor, Decimal):
        return float(valor)
    elif isinstance(valor, str):
        try:
            return float(valor)
        except ValueError:
            return 0.0
    else:
        return 0.0
    
from datetime import datetime

def formatar_data_brasileira(data):
    """
    Formata datas para o padrão brasileiro: DD/MM/AAAA HH:MM:SS
    Trata diferentes formatos de entrada do MongoDB
    """
    if not data:
        return ''
    
    try:
        # Se já é um objeto datetime
        if isinstance(data, datetime):
            return data.strftime('%d/%m/%Y %H:%M:%S')
        
        # Se é string, tratar diferentes formatos
        if isinstance(data, str):
            # Evitar recursão - se já está formatado, retornar
            if re.match(r'\d{2}/\d{2}/\d{4}', data):
                return data
            
            # Remover timezone info se presente (-0300, +0000, etc)
            data_limpa = re.sub(r'[+-]\d{4}$', '', data)
            
            # Formato: 2021-02-22T23:54:30-0300 -> 2021-02-22T23:54:30
            # Formato: 2023-01-05 12:56:56.786Z -> 2023-01-05 12:56:56.786
            data_limpa = data_limpa.replace('Z', '')
            
            # Tentar diferentes formatos
            formatos = [
                '%Y-%m-%dT%H:%M:%S',           # 2021-02-22T23:54:30
                '%Y-%m-%d %H:%M:%S.%f',       # 2023-01-05 12:56:56.786
                '%Y-%m-%d %H:%M:%S',          # 2023-01-05 12:56:56
                '%Y-%m-%dT%H:%M:%S.%f',       # 2021-02-22T23:54:30.123
                '%Y-%m-%d',                   # 2021-02-22
                '%d/%m/%Y %H:%M:%S',          # Já formatado brasileiro
                '%d/%m/%Y'                    # Só data brasileira
            ]
            
            for formato in formatos:
                try:
                    dt = datetime.strptime(data_limpa, formato)
                    return dt.strftime('%d/%m/%Y %H:%M:%S')
                except ValueError:
                    continue
            
            # Se não conseguiu converter com nenhum formato, retornar original
            logger.warning(f"Formato de data não reconhecido: {data}")
            return str(data)
        
        # Se tem método de conversão (ISODate do MongoDB)
        if hasattr(data, 'strftime'):
            return data.strftime('%d/%m/%Y %H:%M:%S')
        
        # Se chegou até aqui, retornar como string
        logger.warning(f"Tipo de data não suportado: {type(data)} - {data}")
        return str(data) if data else ''
        
    except Exception as e:
        logger.warning(f"Erro ao formatar data {data}: {e}")
        return str(data) if data else ''

def formatar_data_simples(data):
    """
    Formata data apenas como DD/MM/AAAA (sem horário)
    """
    if not data:
        return ''
    
    data_completa = formatar_data_brasileira(data)
    if data_completa and ' ' in data_completa:
        return data_completa.split(' ')[0]
    return data_completa