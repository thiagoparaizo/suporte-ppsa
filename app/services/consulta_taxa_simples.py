#!/usr/bin/env python3
"""
Script simples para consultar taxas IPCA/IGPM
Funciona independentemente dos outros módulos
"""

import sys
import os
import argparse
from pymongo import MongoClient
from bson import Decimal128

# Configuração padrão
DEFAULT_MONGO_URI = "mongodb://localhost:27017/"

def conectar_mongodb(mongo_uri=None):
    """Estabelece conexão com MongoDB"""
    try:
        uri = mongo_uri or DEFAULT_MONGO_URI
        
        # Substituir certificado se configurado
        ca_cert_path = os.getenv('CA_CERTIFICATE_PATH_DEFAULT', '')
        if ca_cert_path and 'tlsCAFile=PATH_CERT' in uri:
            uri = uri.replace('tlsCAFile=PATH_CERT', f'tlsCAFile={ca_cert_path}')
        
        client = MongoClient(uri)
        db = client.sgppServices
        
        # Testar conexão
        client.admin.command('ping')
        return client, db
        
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def consultar_taxa(db, ano, mes, tipo='IPCA'):
    """Consulta taxa específica na base"""
    try:
        if tipo.upper() == 'IPCA':
            colecao = db.ipca_entity
        elif tipo.upper() == 'IGPM':
            colecao = db.igpm_entity
        else:
            print(f"❌ Tipo '{tipo}' não reconhecido. Use IPCA ou IGPM.")
            return None
        
        documento = colecao.find_one({
            'anoReferencia': ano,
            'mesReferencia': mes
        })
        
        if documento:
            valor_percentual = float(documento['valor'].to_decimal()) if isinstance(documento['valor'], Decimal128) else float(documento['valor'])
            taxa_fator = 1 + (valor_percentual / 100)
            
            return {
                'encontrada': True,
                'id': str(documento['_id']),
                'periodo': f"{mes:02d}/{ano}",
                'valor_percentual': valor_percentual,
                'taxa_fator': taxa_fator,
                'version': documento.get('version', 'N/A'),
                'formatado': f"{valor_percentual:.4f}%"
            }
        else:
            return {
                'encontrada': False,
                'periodo': f"{mes:02d}/{ano}",
                'tipo': tipo
            }
            
    except Exception as e:
        print(f"❌ Erro ao consultar {tipo}: {e}")
        return None

def listar_taxas(db, tipo='IPCA', ano_inicio=None, ano_fim=None, limite=20):
    """Lista taxas disponíveis"""
    try:
        if tipo.upper() == 'IPCA':
            colecao = db.ipca_entity
        elif tipo.upper() == 'IGPM':
            colecao = db.igpm_entity
        else:
            print(f"❌ Tipo '{tipo}' não reconhecido. Use IPCA ou IGPM.")
            return []
        
        # Construir filtro
        filtro = {}
        if ano_inicio and ano_fim:
            filtro['anoReferencia'] = {'$gte': ano_inicio, '$lte': ano_fim}
        elif ano_inicio:
            filtro['anoReferencia'] = {'$gte': ano_inicio}
        elif ano_fim:
            filtro['anoReferencia'] = {'$lte': ano_fim}
        
        cursor = colecao.find(filtro).sort([('anoReferencia', -1), ('mesReferencia', -1)]).limit(limite)
        
        taxas = []
        for doc in cursor:
            valor_percentual = float(doc['valor'].to_decimal()) if isinstance(doc['valor'], Decimal128) else float(doc['valor'])
            taxa_fator = 1 + (valor_percentual / 100)
            
            taxas.append({
                'id': str(doc['_id']),
                'ano': doc['anoReferencia'],
                'mes': doc['mesReferencia'],
                'periodo': f"{doc['mesReferencia']:02d}/{doc['anoReferencia']}",
                'valor_percentual': valor_percentual,
                'taxa_fator': taxa_fator,
                'formatado': f"{valor_percentual:.4f}%"
            })
        
        return taxas
        
    except Exception as e:
        print(f"❌ Erro ao listar {tipo}: {e}")
        return []

def comando_consultar(args):
    """Executa comando de consulta"""
    client, db = conectar_mongodb()
    if not client:
        return
    
    try:
        resultado = consultar_taxa(db, args.ano, args.mes, args.tipo)
        
        if resultado:
            print(f"\n=== CONSULTA DE TAXA {args.tipo.upper()} ===")
            print(f"Período: {resultado['periodo']}")
            
            if resultado['encontrada']:
                print(f"Taxa: {resultado['taxa_fator']:.6f} ({resultado['formatado']})")
                print(f"ID Documento: {resultado['id']}")
                print(f"Versão: {resultado['version']}")
                print("✓ Taxa encontrada na base de dados")
            else:
                print("⚠️  Taxa não encontrada na base de dados")
                print("Usando taxa padrão: 1.0400 (4.0000%)")
        
    finally:
        client.close()

def comando_listar(args):
    """Executa comando de listagem"""
    client, db = conectar_mongodb()
    if not client:
        return
    
    try:
        taxas = listar_taxas(db, args.tipo, args.ano_inicio, args.ano_fim, args.limite)
        
        print(f"\n=== TAXAS {args.tipo.upper()} DISPONÍVEIS ===")
        print(f"Total encontrado: {len(taxas)}")
        
        if taxas:
            print(f"Período: {taxas[-1]['periodo']} a {taxas[0]['periodo']}")
            print("\nDetalhes:")
            
            for taxa in taxas:
                print(f"  {taxa['periodo']}: {taxa['formatado']} (fator: {taxa['taxa_fator']:.6f})")
        else:
            print("Nenhuma taxa encontrada para os critérios especificados")
        
    finally:
        client.close()

def comando_testar(args):
    """Executa testes básicos"""
    client, db = conectar_mongodb()
    if not client:
        return
    
    try:
        print("\n=== TESTES BÁSICOS ===")
        
        # Teste IPCA
        print("\n1. Testando IPCA...")
        resultado_ipca = consultar_taxa(db, 2022, 10, 'IPCA')
        if resultado_ipca and resultado_ipca['encontrada']:
            print(f"✓ IPCA 10/2022: {resultado_ipca['formatado']}")
        else:
            print("⚠️  IPCA 10/2022 não encontrado")
        
        # Teste IGPM
        print("\n2. Testando IGPM...")
        resultado_igpm = consultar_taxa(db, 2017, 3, 'IGPM')
        if resultado_igpm and resultado_igpm['encontrada']:
            print(f"✓ IGPM 03/2017: {resultado_igpm['formatado']}")
        else:
            print("⚠️  IGPM 03/2017 não encontrado")
        
        # Teste listagem
        print("\n3. Testando listagem...")
        amostras_ipca = listar_taxas(db, 'IPCA', limite=3)
        print(f"✓ Encontradas {len(amostras_ipca)} amostras IPCA recentes")
        
        print("\n=== TESTES CONCLUÍDOS ===")
        
    finally:
        client.close()

def main():
    """Função principal"""
    parser = argparse.ArgumentParser(description='Consulta taxas IPCA/IGPM')
    subparsers = parser.add_subparsers(dest='comando', help='Comandos disponíveis')
    
    # Comando: consultar
    consultar_parser = subparsers.add_parser('taxa', help='Consultar taxa específica')
    consultar_parser.add_argument('ano', type=int, help='Ano')
    consultar_parser.add_argument('mes', type=int, help='Mês')
    consultar_parser.add_argument('--tipo', default='IPCA', choices=['IPCA', 'IGPM'], help='Tipo de índice')
    
    # Comando: listar
    listar_parser = subparsers.add_parser('listar', help='Listar taxas disponíveis')
    listar_parser.add_argument('tipo', choices=['IPCA', 'IGPM'], help='Tipo de índice')
    listar_parser.add_argument('--ano-inicio', type=int, help='Ano inicial')
    listar_parser.add_argument('--ano-fim', type=int, help='Ano final')
    listar_parser.add_argument('--limite', type=int, default=20, help='Limite de resultados')
    
    # Comando: testar
    testar_parser = subparsers.add_parser('testar', help='Executar testes básicos')
    
    args = parser.parse_args()
    
    if not args.comando:
        parser.print_help()
        return
    
    # Executar comando
    if args.comando == 'taxa':
        comando_consultar(args)
    elif args.comando == 'listar':
        comando_listar(args)
    elif args.comando == 'testar':
        comando_testar(args)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Consulta de Taxas IPCA/IGPM")
        print("Use --help para ver opções disponíveis")
        print("")
        print("Exemplos:")
        print("  python consulta_taxa_simples.py taxa 2022 10 --tipo IPCA")
        print("  python consulta_taxa_simples.py listar IPCA --ano-inicio 2024")
        print("  python consulta_taxa_simples.py testar")
    else:
        main()