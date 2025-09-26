#!/usr/bin/env python3
"""
Script para execução de recálculos IPCA/IGPM
Suporte para identificação de gaps e aplicação de correções monetárias
"""

import sys
import os
import argparse
import json
import csv
from datetime import datetime
from pymongo import MongoClient
from decimal import Decimal

# Adicionar o diretório do projeto ao path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Imports locais - ajustar conforme estrutura do projeto
try:
    from ipca_igpm_recalculo_service import IPCAIGPMRecalculoService, ModoRecalculo
    from ipca_gap_analyzer import IPCAGapAnalyzer, IPCAGapReportGenerator
except ImportError:
    # Fallback se não conseguir importar
    print("⚠️  Aviso: Não foi possível importar alguns módulos.")
    print("   Verifique se os arquivos estão no diretório correto:")
    print("   - ipca_igpm_recalculo_service.py")
    print("   - ipca_gap_analyzer.py")
    print("   Execute a partir do diretório services/")
    sys.exit(1)

# Configurações padrão
DEFAULT_MONGO_URI = "mongodb://localhost:27017/"
DEFAULT_LOCAL_URI = "mongodb://localhost:27017/"

class IPCARecalculoManager:
    """
    Gerenciador principal para recálculos IPCA/IGPM
    """
    
    def __init__(self, mongo_uri: str, mongo_local_uri: str = None):
        """
        Inicializa o gerenciador
        """
        self.mongo_uri = mongo_uri
        self.mongo_local_uri = mongo_local_uri
        
        # Conectar ao MongoDB
        self.client = MongoClient(self.mongo_uri)
        self.db_prd = self.client.sgppServices
        
        self.client_local = None
        self.db_local = None
        
        if mongo_local_uri:
            self.client_local = MongoClient(mongo_local_uri)
            self.db_local = self.client_local.temp_recalculos
        
        # Inicializar serviços
        self.recalculo_service = IPCAIGPMRecalculoService(self.db_local, self.db_prd)
        self.gap_analyzer = IPCAGapAnalyzer(self.db_local, self.db_prd)
        self.report_generator = IPCAGapReportGenerator(self.gap_analyzer)
        
        print("✓ Conexões estabelecidas e serviços inicializados")
    
    def identificar_gaps(self, filtros: dict = None, exportar: bool = False) -> dict:
        """
        Identifica gaps de correção IPCA/IGPM
        """
        print("\n=== IDENTIFICANDO GAPS IPCA/IGPM ===")
        
        resultado = self.gap_analyzer.analisar_gaps_sistema(filtros)
        
        if 'error' in resultado:
            print(f"❌ Erro na análise: {resultado['error']}")
            return resultado
        
        stats = resultado['estatisticas']
        print(f"✓ Análise concluída:")
        print(f"  - CCOs analisadas: {stats['total_ccos_analisadas']:,}")
        print(f"  - CCOs com gaps: {stats['ccos_com_gaps']:,}")
        print(f"  - Total de gaps: {stats['total_gaps_identificados']:,}")
        print(f"  - Valor impactado: R$ {stats['valor_total_impactado']:,.2f}")
        
        if exportar and (stats['ccos_com_gaps'] > 0 or stats['ccos_com_duplicatas'] > 0 or stats['ccos_com_correcoes_fora_periodo'] > 0):
            identificacao = filtros['_id'] if '_id' in filtros else filtros.get('contratoCpp', 'Todos')
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Exportar CSV
            csv_file = f"{identificacao}_gaps_ipca_igpm_{timestamp}.csv"
            if self.gap_analyzer.exportar_gaps_csv(resultado, csv_file):
                print(f"✓ Relatório CSV exportado: {csv_file}")
            
            # Exportar JSON
            json_file = f"{identificacao}_gaps_ipca_igpm_{timestamp}.json"
            if self.gap_analyzer.exportar_gaps_json(resultado, json_file):
                print(f"✓ Relatório JSON exportado: {json_file}")
            
            # Gerar relatório executivo
            relatorio_executivo = self.report_generator.gerar_relatorio_executivo(filtros)
            relatorio_file = f"{identificacao}_relatorio_executivo_{timestamp}.txt"
            
            with open(relatorio_file, 'w', encoding='utf-8') as f:
                f.write(relatorio_executivo)
            print(f"✓ Relatório executivo exportado: {relatorio_file}")
        
        return resultado
    
    def executar_recalculo_cco(self, cco_id: str, ano: int, mes: int, 
                              taxa_correcao: float, tipo: str = 'IPCA',
                              modo: str = 'CORRECAO_SIMPLES', observacoes: str = "") -> dict:
        """
        Executa recálculo IPCA/IGPM para uma CCO específica
        """
        print(f"\n=== EXECUTANDO RECÁLCULO {tipo} ===")
        print(f"CCO: {cco_id}")
        print(f"Período: {mes:02d}/{ano}")
        print(f"Taxa: {taxa_correcao}")
        print(f"Modo: {modo}")
        
        try:
            modo_enum = ModoRecalculo.CORRECAO_SIMPLES if modo == 'CORRECAO_SIMPLES' else ModoRecalculo.RECALCULO_COMPLETO
            
            resultado = self.recalculo_service.executar_recalculo_ipca_igpm(
                cco_id=cco_id,
                ano=ano,
                mes=mes,
                taxa_correcao=taxa_correcao,
                tipo=tipo,
                modo=modo_enum,
                observacoes=observacoes
            )
            
            if resultado['success']:
                print("✓ Recálculo executado com sucesso!")
                
                # Exibir resumo
                resumo = resultado['resultado']['resumo']
                print(f"  - Valor da correção: R$ {resumo['valor_correcao_aplicada']:,.2f}")
                print(f"  - Taxa aplicada: {resumo['taxa_aplicada']}")
                print(f"  - Período: {resumo['ano_mes_correcao']}")
                
                # Salvar resultado se houver conexão local
                if self.db_local:
                    self._salvar_resultado_temporario(resultado['resultado'])
                
            else:
                print(f"❌ Erro no recálculo: {resultado.get('error', 'Erro desconhecido')}")
            
            return resultado
            
        except Exception as e:
            print(f"❌ Erro na execução: {e}")
            return {'success': False, 'error': str(e)}
    
    def processar_lote_gaps(self, filtros: dict = None, taxa_padrao: float = None, 
                           tipo: str = 'IPCA', limite: int = 10) -> dict:
        """
        Processa um lote de gaps identificados
        """
        print(f"\n=== PROCESSANDO LOTE DE GAPS ===")
        
        # Identificar gaps
        resultado_gaps = self.identificar_gaps(filtros)
        
        if 'error' in resultado_gaps:
            return resultado_gaps
        
        ccos_com_gaps = resultado_gaps['ccos_com_gaps']
        
        if not ccos_com_gaps:
            print("✓ Nenhum gap identificado para processamento")
            return {'success': True, 'processados': 0}
        
        print(f"✓ {len(ccos_com_gaps)} CCOs com gaps identificadas")
        print(f"✓ Processando até {limite} gaps...")
        
        processados = 0
        sucessos = 0
        erros = []
        
        for cco in ccos_com_gaps[:limite]:
            cco_id = cco['_id']
            
            # Processar o primeiro gap (mais antigo)
            gaps_ordenados = sorted(cco['gaps'], key=lambda x: (x['ano'], x['mes']))
            gap = gaps_ordenados[0]
            
            print(f"\nProcessando CCO {cco_id} - Gap {gap['mes']:02d}/{gap['ano']}")
            
            resultado = self.executar_recalculo_cco(
                cco_id=cco_id,
                ano=gap['ano'],
                mes=gap['mes'],
                taxa_correcao=taxa_padrao,
                tipo=tipo,
                observacoes=f"Correção automática lote - Gap {gap['mes']:02d}/{gap['ano']}"
            )
            
            processados += 1
            
            if resultado['success']:
                sucessos += 1
                print(f"✓ Gap corrigido com sucesso")
            else:
                erros.append({
                    'cco_id': cco_id,
                    'gap': f"{gap['mes']:02d}/{gap['ano']}",
                    'erro': resultado.get('error', 'Erro desconhecido')
                })
                print(f"❌ Erro ao corrigir gap: {resultado.get('error')}")
        
        print(f"\n=== RESUMO DO LOTE ===")
        print(f"Processados: {processados}")
        print(f"Sucessos: {sucessos}")
        print(f"Erros: {len(erros)}")
        
        if erros:
            print("\nErros encontrados:")
            for erro in erros:
                print(f"  - {erro['cco_id']} ({erro['gap']}): {erro['erro']}")
        
        return {
            'success': True,
            'processados': processados,
            'sucessos': sucessos,
            'erros': erros
        }
    
    def _salvar_resultado_temporario(self, resultado: dict) -> str:
        """
        Salva resultado em coleção temporária
        """
        if not self.db_local:
            return None
        
        documento = {
            'tipo_recalculo': 'IPCA_IGPM',
            'data_processamento': datetime.now(),
            'resultado': resultado,
            'status': 'TEMPORARIO'
        }
        
        result = self.db_local.recalculos_ipca.insert_one(documento)
        return str(result.inserted_id)
    
    def fechar_conexoes(self):
        """
        Fecha conexões
        """
        if self.client:
            self.client.close()
        if self.client_local:
            self.client_local.close()

def main():
    """
    Função principal do script
    """
    parser = argparse.ArgumentParser(description='Recálculo IPCA/IGPM para CCOs')
    
    # Comandos principais
    subparsers = parser.add_subparsers(dest='comando', help='Comandos disponíveis')
    
    # Comando: identificar gaps
    gaps_parser = subparsers.add_parser('gaps', help='Identificar gaps de correção')
    gaps_parser.add_argument('--cco_id', help='ID da CCO')
    gaps_parser.add_argument('--contrato', help='Filtrar por contrato')
    gaps_parser.add_argument('--campo', help='Filtrar por campo')
    gaps_parser.add_argument('--ano', type=int, help='Filtrar por ano de reconhecimento')
    gaps_parser.add_argument('--exportar', action='store_true', help='Exportar relatórios')
    
    # Comando: recalcular CCO
    recalc_parser = subparsers.add_parser('recalcular', help='Recalcular CCO específica')
    recalc_parser.add_argument('cco_id', help='ID da CCO')
    recalc_parser.add_argument('ano', type=int, help='Ano da correção')
    recalc_parser.add_argument('mes', type=int, help='Mês da correção')
    recalc_parser.add_argument('taxa', type=float, help='Taxa de correção (ex: 1.045)')
    recalc_parser.add_argument('--tipo', default='IPCA', choices=['IPCA', 'IGPM'], help='Tipo de correção')
    recalc_parser.add_argument('--modo', default='CORRECAO_SIMPLES', 
                              choices=['CORRECAO_SIMPLES', 'RECALCULO_COMPLETO'], help='Modo de recálculo')
    recalc_parser.add_argument('--obs', default='', help='Observações')
    
    # Comando: processar lote
    lote_parser = subparsers.add_parser('lote', help='Processar lote de gaps')
    lote_parser.add_argument('--cco_id', help='ID da CCO')
    lote_parser.add_argument('--contrato', help='Filtrar por contrato')
    lote_parser.add_argument('--campo', help='Filtrar por campo')
    lote_parser.add_argument('--taxa', type=float, default=None, help='Taxa padrão (ex: 1.045)')
    lote_parser.add_argument('--tipo', default='IPCA', choices=['IPCA', 'IGPM'], help='Tipo de correção')
    lote_parser.add_argument('--limite', type=int, default=10, help='Limite de processamento')
    
    # Comando: consultar taxa
    taxa_parser = subparsers.add_parser('taxa', help='Consultar taxa histórica')
    taxa_parser.add_argument('ano', type=int, help='Ano')
    taxa_parser.add_argument('mes', type=int, help='Mês')
    taxa_parser.add_argument('--tipo', default='IPCA', choices=['IPCA', 'IGPM'], help='Tipo de índice')

    # Comando: listar taxas
    listar_parser = subparsers.add_parser('listar-taxas', help='Listar taxas disponíveis')
    listar_parser.add_argument('tipo', choices=['IPCA', 'IGPM'], help='Tipo de índice')
    listar_parser.add_argument('--ano-inicio', type=int, help='Ano inicial (opcional)')
    listar_parser.add_argument('--ano-fim', type=int, help='Ano final (opcional)')
    listar_parser.add_argument('--exportar', action='store_true', help='Exportar para CSV')
    
    # Argumentos globais
    parser.add_argument('--mongo-uri', default=DEFAULT_MONGO_URI, help='URI MongoDB principal')
    parser.add_argument('--mongo-local', default=DEFAULT_LOCAL_URI, help='URI MongoDB local')
    
    args = parser.parse_args()
    
    if not args.comando:
        parser.print_help()
        return
    
    # Inicializar gerenciador
    try:
        manager = IPCARecalculoManager(args.mongo_uri, args.mongo_local)
        
        # Executar comando
        if args.comando == 'gaps':
            # Preparar filtros
            filtros = {}
            if args.cco_id:
                filtros['_id'] = args.cco_id
            if args.contrato:
                filtros['contratoCpp'] = args.contrato
            if args.campo:
                filtros['campo'] = args.campo
            if args.ano:
                filtros['anoReconhecimento'] = args.ano
            
            resultado = manager.identificar_gaps(filtros, args.exportar)
            
            if 'error' not in resultado:
                # Exibir resumo
                print(manager.gap_analyzer.gerar_relatorio_resumido(resultado))
        
        elif args.comando == 'recalcular':
            resultado = manager.executar_recalculo_cco(
                cco_id=args.cco_id,
                ano=args.ano,
                mes=args.mes,
                taxa_correcao=args.taxa,
                tipo=args.tipo,
                modo=args.modo,
                observacoes=args.obs
            )
        
        elif args.comando == 'lote':
            # Preparar filtros
            filtros = {}
            if args.contrato:
                filtros['contratoCpp'] = args.contrato
            if args.campo:
                filtros['campo'] = args.campo
            
            # Determinar configuração de taxas
            usar_historicas = not args.nao_usar_historicas
            taxa_padrao = args.taxa
            
            if not usar_historicas and not taxa_padrao:
                print("❌ Erro: Deve especificar --taxa quando usar --nao-usar-historicas")
                return
            
            resultado = manager.processar_lote_gaps(
                filtros=filtros,
                tipo=args.tipo,
                usar_taxas_historicas=usar_historicas,
                taxa_padrao=taxa_padrao,
                limite=args.limite
            )
        
        elif args.comando == 'taxa':
            # Consultar taxa histórica da base de dados
            resultado = manager.recalculo_service.consultar_taxa_disponivel(
                args.ano, args.mes, args.tipo
            )
            
            if resultado['success']:
                print(f"\n=== CONSULTA DE TAXA {args.tipo} ===")
                print(f"Período: {args.mes:02d}/{args.ano}")
                
                if resultado['encontrada']:
                    print(f"Taxa: {resultado['taxa_fator']} ({resultado['formatado']})")
                    print(f"ID Documento: {resultado['documento_id']}")
                    print(f"Versão: {resultado['version']}")
                    print("✓ Taxa encontrada na base de dados")
                else:
                    print(f"Taxa: {resultado['taxa_fator']} ({resultado['formatado']})")
                    print("⚠️  Taxa não encontrada na base - usando padrão")
                    print(f"Mensagem: {resultado['mensagem']}")
            else:
                print(f"❌ Erro ao consultar taxa: {resultado['error']}")
            
            return
        
        elif args.comando == 'listar-taxas':
            # Listar taxas disponíveis
            resultado = manager.recalculo_service.listar_taxas_disponiveis(
                args.tipo, args.ano_inicio, args.ano_fim
            )
            
            if resultado['success']:
                print(f"\n=== TAXAS {args.tipo} DISPONÍVEIS ===")
                print(f"Total de registros: {resultado['total_registros']}")
                
                if resultado['total_registros'] > 0:
                    print(f"Período: {resultado['periodo_inicio']} a {resultado['periodo_fim']}")
                    print("\nDetalhes:")
                    
                    for taxa in resultado['taxas']:
                        print(f"  {taxa['periodo']}: {taxa['formatado']} (fator: {taxa['taxa_fator']:.6f})")
                    
                    # Exportar se solicitado
                    if args.exportar:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        arquivo_csv = f"taxas_{args.tipo.lower()}_{timestamp}.csv"
                        
                        with open(arquivo_csv, 'w', newline='', encoding='utf-8') as csvfile:
                            fieldnames = ['periodo', 'ano', 'mes', 'valor_percentual', 'taxa_fator', 'documento_id']
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            writer.writeheader()
                            
                            for taxa in resultado['taxas']:
                                writer.writerow({
                                    'periodo': taxa['periodo'],
                                    'ano': taxa['ano'],
                                    'mes': taxa['mes'],
                                    'valor_percentual': taxa['valor_percentual'],
                                    'taxa_fator': taxa['taxa_fator'],
                                    'documento_id': taxa['documento_id']
                                })
                        
                        print(f"\n✓ Taxas exportadas para: {arquivo_csv}")
                else:
                    print("Nenhuma taxa encontrada para os critérios especificados")
            else:
                print(f"❌ Erro ao listar taxas: {resultado['error']}")
            
            return
        
        # Fechar conexões
        # Fechar conexões
        manager.fechar_conexoes()
        
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
        sys.exit(1)

def exibir_exemplos():
    """
    Exibe exemplos de uso do script
    """
    print("""
=== EXEMPLOS DE USO ===

1. Identificar gaps em todo o sistema:
   python recalculo_ipca.py gaps --exportar

2. Identificar gaps por contrato:
   python recalculo_ipca.py gaps --contrato "Búzios" --exportar

3. Recalcular CCO com taxa automática (recomendado):
   python recalculo_ipca.py recalcular "-itmZW19TD2HDBBDfpimxAAAAAA" 2024 9 auto --tipo IPCA

4. Recalcular CCO com taxa específica:
   python recalculo_ipca.py recalcular "-itmZW19TD2HDBBDfpimxAAAAAA" 2024 9 1.0044 --tipo IPCA

5. Processar lote com taxas da base (recomendado):
   python recalculo_ipca.py lote --contrato "Búzios" --usar-historicas --limite 5

6. Processar lote com taxa fixa:
   python recalculo_ipca.py lote --contrato "Búzios" --nao-usar-historicas --taxa 1.045 --limite 5

7. Consultar taxa na base de dados:
   python recalculo_ipca.py taxa 2024 9 --tipo IPCA

8. Listar todas as taxas IPCA disponíveis:
   python recalculo_ipca.py listar-taxas IPCA --exportar

9. Listar taxas IGPM para período específico:
   python recalculo_ipca.py listar-taxas IGPM --ano-inicio 2023 --ano-fim 2024

10. Identificar gaps por ano de reconhecimento:
    python recalculo_ipca.py gaps --ano 2023 --exportar

=== CONFIGURAÇÃO ===

Para usar certificado SSL personalizado:
   export CA_CERTIFICATE_PATH_DEFAULT="/path/to/certificate.pem"

Para MongoDB local diferente:
   python recalculo_ipca.py gaps --mongo-local "mongodb://192.168.1.100:27017/"

=== TAXAS DA BASE DE DADOS ===

O sistema busca taxas automaticamente nas coleções:
- IPCA: coleção "ipca_entity" 
- IGPM: coleção "igpm_entity"

Estrutura dos documentos:
{
  "mesReferencia": 10,
  "anoReferencia": 2022, 
  "valor": NumberDecimal("6.470000000000000")  // Percentual
}

=== EXEMPLO DE CONSULTA DE TAXA ===

python recalculo_ipca.py taxa 2022 10 --tipo IPCA

Saída esperada:
=== CONSULTA DE TAXA IPCA ===
Período: 10/2022
Taxa: 1.0647 (6.4700%)
ID Documento: PYuTdNfKTROmQu2vB85lcQAAAAA
Versão: 1
✓ Taxa encontrada na base de dados

=== ARQUIVOS GERADOS ===

- gaps_ipca_igpm_YYYYMMDD_HHMMSS.csv    : Relatório detalhado em CSV
- gaps_ipca_igpm_YYYYMMDD_HHMMSS.json   : Dados completos em JSON
- relatorio_executivo_YYYYMMDD_HHMMSS.txt : Resumo executivo
- taxas_ipca_YYYYMMDD_HHMMSS.csv        : Exportação de taxas IPCA
- taxas_igpm_YYYYMMDD_HHMMSS.csv        : Exportação de taxas IGPM

=== EXEMPLO COMPLETO - CASO BÚZIOS ===

1. Verificar taxas disponíveis:
   python recalculo_ipca.py listar-taxas IPCA --ano-inicio 2024

2. Identificar gaps:
   python recalculo_ipca.py gaps --contrato "Búzios" --exportar

3. Consultar taxa específica:
   python recalculo_ipca.py taxa 2024 9 --tipo IPCA

4. Aplicar correção automática:
   python recalculo_ipca.py recalcular "-itmZW19TD2HDBBDfpimxAAAAAA" 2024 9 auto --obs "Gap setembro/2024"

5. Processar em lote:
   python recalculo_ipca.py lote --contrato "Búzios" --usar-historicas --limite 10

=== VERIFICAÇÃO DE AMBIENTE ===

Para validar se as coleções de taxa estão disponíveis:
python recalculo_ipca.py listar-taxas IPCA --ano-inicio 2020 --ano-fim 2025
""")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Script de Recálculo IPCA/IGPM para CCOs")
        print("Use --help para ver opções disponíveis")
        print("Use 'python recalculo_ipca.py exemplos' para ver exemplos de uso")
    elif len(sys.argv) == 2 and sys.argv[1] == 'exemplos':
        exibir_exemplos()
    else:
        main()