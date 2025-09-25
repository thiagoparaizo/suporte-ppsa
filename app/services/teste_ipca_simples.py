#!/usr/bin/env python3
"""
Teste simples para validar conexão e consulta de taxas IPCA/IGPM
"""

import sys
import os
from pymongo import MongoClient
from bson import Decimal128

# Configuração de conexão (ajustar conforme necessário)
MONGO_URI = "mongodb://localhost:27017/"

def testar_conexao():
    """Testa conexão básica com MongoDB"""
    try:
        # Preparar URI (substituir certificado se necessário)
        uri = MONGO_URI
        ca_cert_path = os.getenv('CA_CERTIFICATE_PATH_DEFAULT', '')
        if ca_cert_path and 'tlsCAFile=PATH_CERT' in uri:
            uri = uri.replace('tlsCAFile=PATH_CERT', f'tlsCAFile={ca_cert_path}')
        
        print("🔌 Testando conexão com MongoDB...")
        client = MongoClient(uri)
        db = client.sgppServices
        
        # Testar conexão
        client.admin.command('ping')
        print("✓ Conexão estabelecida com sucesso")
        
        return client, db
        
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return None, None

def testar_consulta_ipca(db, ano=2022, mes=10):
    """Testa consulta na coleção ipca_entity"""
    try:
        print(f"\n📊 Testando consulta IPCA para {mes:02d}/{ano}...")
        
        documento = db.ipca_entity.find_one({
            'anoReferencia': ano,
            'mesReferencia': mes
        })
        
        if documento:
            valor_percentual = float(documento['valor'].to_decimal())
            taxa_fator = 1 + (valor_percentual / 100)
            
            print("✓ Taxa IPCA encontrada:")
            print(f"  ID: {documento['_id']}")
            print(f"  Período: {mes:02d}/{ano}")
            print(f"  Valor: {valor_percentual:.4f}%")
            print(f"  Fator: {taxa_fator:.6f}")
            print(f"  Versão: {documento.get('version', 'N/A')}")
            return True
        else:
            print(f"⚠️  Taxa IPCA não encontrada para {mes:02d}/{ano}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao consultar IPCA: {e}")
        return False

def testar_consulta_igpm(db, ano=2017, mes=3):
    """Testa consulta na coleção igpm_entity"""
    try:
        print(f"\n📊 Testando consulta IGPM para {mes:02d}/{ano}...")
        
        documento = db.igpm_entity.find_one({
            'anoReferencia': ano,
            'mesReferencia': mes
        })
        
        if documento:
            valor_percentual = float(documento['valor'].to_decimal())
            taxa_fator = 1 + (valor_percentual / 100)
            
            print("✓ Taxa IGPM encontrada:")
            print(f"  ID: {documento['_id']}")
            print(f"  Período: {mes:02d}/{ano}")
            print(f"  Valor: {valor_percentual:.4f}%")
            print(f"  Fator: {taxa_fator:.6f}")
            print(f"  Versão: {documento.get('version', 'N/A')}")
            return True
        else:
            print(f"⚠️  Taxa IGPM não encontrada para {mes:02d}/{ano}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao consultar IGPM: {e}")
        return False

def listar_amostras_ipca(db, limite=5):
    """Lista algumas amostras de taxas IPCA"""
    try:
        print(f"\n📋 Listando {limite} amostras de taxas IPCA...")
        
        cursor = db.ipca_entity.find().sort([('anoReferencia', -1), ('mesReferencia', -1)]).limit(limite)
        
        amostras = list(cursor)
        
        if amostras:
            print("✓ Amostras encontradas:")
            for doc in amostras:
                try:
                    valor = float(doc['valor'].to_decimal())
                    print(f"  {doc['mesReferencia']}/{doc['anoReferencia']}: {valor:.4f}%")
                except Exception as e:
                    print(f"  {doc['mesReferencia']}/{doc['anoReferencia']}: ERRO ({e})")
            return True
        else:
            print("⚠️  Nenhuma amostra IPCA encontrada")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao listar amostras IPCA: {e}")
        return False

def listar_amostras_igpm(db, limite=5):
    """Lista algumas amostras de taxas IGPM"""
    try:
        print(f"\n📋 Listando {limite} amostras de taxas IGPM...")
        
        cursor = db.igpm_entity.find().sort([('anoReferencia', -1), ('mesReferencia', -1)]).limit(limite)
        
        amostras = list(cursor)
        
        if amostras:
            print("✓ Amostras encontradas:")
            for doc in amostras:
                valor = float(doc['valor'].to_decimal())
                print(f"  {doc['mesReferencia']}/{doc['anoReferencia']}: {valor:.4f}%")
            return True
        else:
            print("⚠️  Nenhuma amostra IGPM encontrada")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao listar amostras IGPM: {e}")
        return False

def main():
    """Função principal do teste"""
    print("=== TESTE SIMPLES IPCA/IGPM ===\n")
    
    # Testar conexão
    client, db = testar_conexao()
    if not client:
        return
    
    # Executar testes
    testes = [
        lambda: testar_consulta_ipca(db),
        lambda: testar_consulta_igpm(db),
        lambda: listar_amostras_ipca(db),
        lambda: listar_amostras_igpm(db)
    ]
    
    sucessos = 0
    for teste in testes:
        if teste():
            sucessos += 1
    
    # Resumo
    print(f"\n=== RESUMO DOS TESTES ===")
    print(f"Sucessos: {sucessos}/{len(testes)}")
    print(f"Taxa de sucesso: {sucessos/len(testes)*100:.1f}%")
    
    if sucessos == len(testes):
        print("🎉 Todos os testes passaram! Sistema pronto para uso.")
    else:
        print("⚠️  Alguns testes falharam. Verifique a configuração.")
    
    # Fechar conexão
    client.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print("""
Teste simples para validar acesso às coleções IPCA/IGPM

Uso:
    python teste_ipca_simples.py                    # Executar todos os testes
    python teste_ipca_simples.py --help             # Mostrar esta ajuda

Configuração:
    export CA_CERTIFICATE_PATH_DEFAULT="/path/to/cert.pem"   # Certificado SSL (se necessário)

O script testa:
1. Conexão com MongoDB
2. Consulta de taxa IPCA específica
3. Consulta de taxa IGPM específica  
4. Listagem de amostras de ambas as coleções
""")
        else:
            print(f"Argumento '{sys.argv[1]}' não reconhecido. Use --help para ajuda.")
    else:
        main()