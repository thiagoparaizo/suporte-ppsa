#!/usr/bin/env python3
"""
Debug do script recalculo_ipca.py
Vamos identificar onde está o problema
"""

print("🔍 INICIANDO DEBUG...")

# Teste 1: Verificar se o arquivo está sendo executado
print("✓ Arquivo sendo executado")

# Teste 2: Verificar imports básicos
try:
    import sys
    print("✓ sys importado")
except Exception as e:
    print(f"❌ Erro no import sys: {e}")
    exit(1)

try:
    import os
    print("✓ os importado")
except Exception as e:
    print(f"❌ Erro no import os: {e}")
    exit(1)

try:
    import argparse
    print("✓ argparse importado")
except Exception as e:
    print(f"❌ Erro no import argparse: {e}")
    exit(1)

# Teste 3: Verificar path
print(f"📁 Diretório atual: {os.getcwd()}")
print(f"📁 Arquivo script: {__file__}")
print(f"📁 sys.path: {sys.path[:3]}...")  # Primeiros 3 itens

# Teste 4: Verificar argumentos
print(f"📋 Argumentos recebidos: {sys.argv}")

# Teste 5: Testar imports problemáticos
print("\n🧪 TESTANDO IMPORTS PROBLEMÁTICOS...")

try:
    from ipca_igpm_recalculo_service import IPCAIGPMRecalculoService, ModoRecalculo
    print("✓ ipca_igpm_recalculo_service importado com sucesso")
except ImportError as e:
    print(f"❌ ImportError em ipca_igpm_recalculo_service: {e}")
except Exception as e:
    print(f"❌ Outro erro em ipca_igpm_recalculo_service: {e}")

try:
    from ipca_gap_analyzer import IPCAGapAnalyzer, IPCAGapReportGenerator
    print("✓ ipca_gap_analyzer importado com sucesso")
except ImportError as e:
    print(f"❌ ImportError em ipca_gap_analyzer: {e}")
except Exception as e:
    print(f"❌ Outro erro em ipca_gap_analyzer: {e}")

# Teste 6: Testar argparse básico
print("\n🧪 TESTANDO ARGPARSE...")

try:
    parser = argparse.ArgumentParser(description='Teste')
    subparsers = parser.add_subparsers(dest='comando', help='Comandos')
    
    # Comando taxa
    taxa_parser = subparsers.add_parser('taxa', help='Consultar taxa')
    taxa_parser.add_argument('ano', type=int, help='Ano')
    taxa_parser.add_argument('mes', type=int, help='Mês')
    taxa_parser.add_argument('--tipo', default='IPCA', help='Tipo')
    
    print("✓ Parser criado com sucesso")
    
    # Testar parsing
    if len(sys.argv) > 1:
        args = parser.parse_args()
        print(f"✓ Argumentos parseados: comando={getattr(args, 'comando', 'None')}")
        print(f"   args completos: {args}")
    else:
        print("⚠️  Nenhum argumento fornecido para parsing")
        
except Exception as e:
    print(f"❌ Erro no argparse: {e}")

# Teste 7: Verificar se chegou ao final
print("\n✓ DEBUG CONCLUÍDO")

# Teste 8: Simular a lógica principal
if len(sys.argv) > 1:
    print(f"\n🎯 SIMULANDO LÓGICA PRINCIPAL com args: {sys.argv[1:]}")
    
    if sys.argv[1] == 'taxa' and len(sys.argv) >= 4:
        try:
            ano = int(sys.argv[2])
            mes = int(sys.argv[3])
            tipo = 'IPCA'
            
            if '--tipo' in sys.argv:
                idx = sys.argv.index('--tipo')
                if idx + 1 < len(sys.argv):
                    tipo = sys.argv[idx + 1]
            
            print(f"   Comando: taxa")
            print(f"   Ano: {ano}")
            print(f"   Mês: {mes}")
            print(f"   Tipo: {tipo}")
            print("✓ Parâmetros extraídos com sucesso")
            
        except ValueError as e:
            print(f"❌ Erro ao converter parâmetros: {e}")
        except Exception as e:
            print(f"❌ Erro na extração de parâmetros: {e}")
    else:
        print("⚠️  Comando não reconhecido ou parâmetros insuficientes")
else:
    print("⚠️  Nenhum argumento fornecido")

print("\n🏁 FIM DO DEBUG")