#!/usr/bin/env python3
"""
Script para recálculo de valores CCO com novo TP (Taxa de Participação)
Suporta entrada via arquivo JSON ou busca direta no MongoDB por ID
Salva resultado em coleção MongoDB local preservando tipos de dados originais
"""

import json
import csv
import sys
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from copy import deepcopy
from datetime import datetime, timezone
from pymongo import MongoClient
from bson import ObjectId
from bson.decimal128 import Decimal128


MONGO_URI = "mongodb+srv://myDatabaseUser:D1fficultP%40ssw0rd@mongodb0.example.com/?authSource=admin&replicaSet=myRepl"


class MongoConnector:
    """Conector para MongoDB"""
    
    def __init__(self, mongo_uri=None):
        """
        Inicializa conexão com MongoDB
        
        Args:
            mongo_uri (str): URI de conexão MongoDB
        """
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI', MONGO_URI)
        
        # Substituir placeholders se necessário
        ca_cert_path = os.getenv('CA_CERTIFICATE_PATH_DEFAULT', '')
        if 'tlsCAFile=PATH_CERT' in self.mongo_uri and ca_cert_path:
            self.mongo_uri = self.mongo_uri.replace('tlsCAFile=PATH_CERT', f'tlsCAFile={ca_cert_path}')
        
        self.client = None
        self.db = None
        
    def conectar(self):
        """Estabelece conexão com MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client.sgppServices
            
            # Testar conexão
            self.client.admin.command('ping')
            print(f"✓ Conectado ao MongoDB: {self.db.name}")
            return True
            
        except Exception as e:
            print(f"✗ Erro ao conectar MongoDB: {e}")
            return False
    
    def buscar_cco_por_id(self, cco_id):
        """
        Busca CCO por ID na coleção
        
        Args:
            cco_id (str): ID da CCO
            
        Returns:
            dict: Documento da CCO ou None se não encontrado
        """
        if self.db is None:
            print("✗ Conexão MongoDB não estabelecida")
            raise Exception("Conexão MongoDB não estabelecida")
        
        try:
            # Tentar buscar diretamente como string
            cco = self.db.conta_custo_oleo_entity.find_one({"_id": cco_id})
            
            if not cco:
                # Tentar como ObjectId se for um formato válido
                try:
                    if len(cco_id) == 24:  # Formato ObjectId padrão
                        cco = self.db.conta_custo_oleo_entity.find_one({"_id": ObjectId(cco_id)})
                except:
                    pass
            
            if cco:
                print(f"✓ CCO encontrada: {cco['_id']}")
                print(f"  Contrato: {cco.get('contratoCpp', 'N/A')}")
                print(f"  Campo: {cco.get('campo', 'N/A')}")
                print(f"  Remessa: {cco.get('remessa', 'N/A')}")
                print(f"  Fase: {cco.get('faseRemessa', 'N/A')}")
                return cco
            else:
                print(f"✗ CCO não encontrada: {cco_id}")
                return None
                
        except Exception as e:
            print(f"✗ Erro ao buscar CCO: {e}")
            return None
    
    def fechar_conexao(self):
        """Fecha conexão com MongoDB"""
        if self.client:
            self.client.close()
            print("✓ Conexão MongoDB fechada")


class MongoConnectorLocal:
    """Conector específico para MongoDB local onde salvar os resultados"""
    
    def __init__(self, mongo_uri_local=None, database_name="temp_recalculos"):
        """
        Inicializa conexão com MongoDB local
        
        Args:
            mongo_uri_local (str): URI de conexão MongoDB local
            database_name (str): Nome da base de dados onde salvar
        """
        self.mongo_uri_local = mongo_uri_local or "mongodb://localhost:27017/"
        self.database_name = database_name
        self.client_local = None
        self.db_local = None
        
    def conectar(self):
        """Estabelece conexão com MongoDB local"""
        try:
            self.client_local = MongoClient(self.mongo_uri_local)
            self.db_local = self.client_local[self.database_name]
            
            # Testar conexão
            self.client_local.admin.command('ping')
            print(f"✓ Conectado ao MongoDB Local: {self.db_local.name}")
            return True
            
        except Exception as e:
            print(f"✗ Erro ao conectar MongoDB Local: {e}")
            return False
    
    def salvar_cco_recalculada(self, cco_recalculada, colecao="temp_correcao_conta_custo_oleo_entity", 
                              tp_original=None, tp_correcao=None, observacoes=None):
        """
        Salva CCO recalculada na coleção local preservando tipos MongoDB
        
        Args:
            cco_recalculada (dict): Documento CCO recalculado
            colecao (str): Nome da coleção onde salvar
            tp_original (float): TP original usado
            tp_correcao (float): Novo TP aplicado
            observacoes (str): Observações adicionais
            
        Returns:
            str: ID do documento inserido ou None se erro
        """
        if self.db_local is None:
            print("✗ Conexão MongoDB Local não estabelecida")
            raise Exception("Conexão MongoDB Local não estabelecida")
        
        try:
            # Preparar documento para inserção
            doc_para_salvar = deepcopy(cco_recalculada)
            
            # Gerar novo ID preservando formato original
            id_original = doc_para_salvar.get('_id')
            if isinstance(id_original, str):
                # Se ID original é string, manter como string com sufixo
                novo_id = f"{id_original}_TP_{str(tp_correcao).replace('.', '_')}"
            else:
                # Se é ObjectId, criar novo ObjectId
                novo_id = ObjectId()
            
            doc_para_salvar['_id'] = novo_id
            
            # Adicionar metadados do recálculo
            timestamp_agora = datetime.now(timezone.utc)
            
            doc_para_salvar['metadadosRecalculo'] = {
                'idOriginal': id_original,
                'tpOriginal': Decimal128(str(tp_original)) if tp_original else None,
                'tpCorrecao': Decimal128(str(tp_correcao)) if tp_correcao else None,
                'fatorConversao': Decimal128(str(tp_correcao / tp_original)) if tp_original and tp_correcao else None,
                'dataRecalculo': timestamp_agora,
                'versaoScript': "2.0",
                'observacoes': observacoes or f"Recálculo automático TP de {tp_original} para {tp_correcao}",
                'tipoOperacao': "RECALCULO_TP"
            }
            
            # Inserir documento
            resultado = self.db_local[colecao].insert_one(doc_para_salvar)
            
            print(f"✓ CCO recalculada salva na coleção: {colecao}")
            print(f"  ID Original: {id_original}")
            print(f"  Novo ID: {novo_id}")
            print(f"  TP: {tp_original} → {tp_correcao}")
            
            return str(resultado.inserted_id)
            
        except Exception as e:
            print(f"✗ Erro ao salvar CCO recalculada: {e}")
            return None
    
    def verificar_se_existe(self, id_original, tp_correcao, colecao="temp_correcao_conta_custo_oleo_entity"):
        """
        Verifica se já existe um recálculo para essa CCO com esse TP
        
        Args:
            id_original: ID da CCO original
            tp_correcao: TP de correção
            colecao: Nome da coleção
            
        Returns:
            dict: Documento existente ou None
        """
        if self.db_local is None:
            print("✗ Conexão MongoDB Local não estabelecida")
            return None
        
        try:
            documento_existente = self.db_local[colecao].find_one({
                "metadadosRecalculo.idOriginal": id_original,
                "metadadosRecalculo.tpCorrecao": Decimal128(str(tp_correcao))
            })
            
            if documento_existente:
                print(f"⚠️  Já existe recálculo para CCO {id_original} com TP {tp_correcao}")
                print(f"   ID existente: {documento_existente['_id']}")
                return documento_existente
            
            return None
            
        except Exception as e:
            print(f"✗ Erro ao verificar existência: {e}")
            return None
    
    def listar_recalculos(self, colecao="temp_correcao_conta_custo_oleo_entity", limite=10):
        """
        Lista recálculos salvos na coleção
        
        Args:
            colecao: Nome da coleção
            limite: Número máximo de registros a retornar
        """
        if self.db_local is None:
            print("✗ Conexão não estabelecida")
            return
        
        try:
            recalculos = list(self.db_local[colecao].find({}, {
                "_id": 1,
                "metadadosRecalculo": 1,
                "contratoCpp": 1,
                "campo": 1,
                "remessa": 1,
                "faseRemessa": 1
            }).sort("metadadosRecalculo.dataRecalculo", -1).limit(limite))
            
            if not recalculos:
                print("📭 Nenhum recálculo encontrado")
                return
            
            print(f"📋 Últimos {len(recalculos)} recálculos:")
            for rec in recalculos:
                metadata = rec.get('metadadosRecalculo', {})
                print(f"  {rec['_id']}")
                print(f"    Original: {metadata.get('idOriginal', 'N/A')}")
                print(f"    TP: {metadata.get('tpOriginal', 'N/A')} → {metadata.get('tpCorrecao', 'N/A')}")
                print(f"    Data: {metadata.get('dataRecalculo', 'N/A')}")
                print(f"    CCO: {rec.get('contratoCpp', '')}/{rec.get('campo', '')}/R{rec.get('remessa', '')}/{rec.get('faseRemessa', '')}")
                print()
                
        except Exception as e:
            print(f"✗ Erro ao listar recálculos: {e}")
    
    def fechar_conexao(self):
        """Fecha conexão com MongoDB local"""
        if self.client_local:
            self.client_local.close()
            print("✓ Conexão MongoDB Local fechada")


class TPRecalculatorMongoDB:
    """Classe para recálculo de valores CCO com novo TP - Versão MongoDB Enhanced"""
    
    def __init__(self, tp_original, tp_correcao, mongo_uri=None, mongo_uri_local=None, 
                 database_local="temp_recalculos", salvar_resultado=True, sobrescrever=False,
                 modo_correcao="COMPLETO"):
        """
        Inicializa o recalculador
        
        Args:
            tp_original (float): TP original usado nos valores atuais
            tp_correcao (float): Novo TP a ser aplicado
            mongo_uri (str): URI de conexão MongoDB origem (opcional)
            mongo_uri_local (str): URI de conexão MongoDB local (opcional)
            database_local (str): Nome da base de dados local
            salvar_resultado (bool): Se deve salvar resultado no MongoDB local
            sobrescrever (bool): Se deve sobrescrever registros existentes
            modo_correcao (str): "COMPLETO" ou "CORRECAO_MONETARIA"
        """
        self.tp_original = Decimal(str(tp_original))
        self.tp_correcao = Decimal(str(tp_correcao))
        self.fator_conversao = self.tp_correcao / self.tp_original
        self.salvar_resultado = salvar_resultado
        self.sobrescrever = sobrescrever
        self.modo_correcao = modo_correcao.upper()
        
        # Conectores MongoDB
        self.mongo_connector = MongoConnector(mongo_uri)
        self.mongo_local = MongoConnectorLocal(mongo_uri_local, database_local) if salvar_resultado else None
        
        print(f"TP Original: {self.tp_original}")
        print(f"TP Correção: {self.tp_correcao}")
        print(f"Fator de Conversão: {self.fator_conversao}")
        print(f"Modo de Correção: {self.modo_correcao}")
        print(f"Salvar resultado: {'Sim' if salvar_resultado else 'Não'}")
        if salvar_resultado:
            print(f"Base local: {database_local}")
        
        # Validar modo de correção
        if self.modo_correcao not in ["COMPLETO", "CORRECAO_MONETARIA"]:
            raise ValueError(f"Modo de correção inválido: {self.modo_correcao}. Use 'COMPLETO' ou 'CORRECAO_MONETARIA'")
        
        # Campos que serão recalculados
        self.campos_raiz = [
            'valorLancamentoTotal', 'valorNaoReconhecido', 'valorReconhecido',
            'valorReconhecivel', 'valorNaoPassivelRecuperacao', 'valorReconhecidoExploracao',
            'valorReconhecidoProducao', 'valorRecusado', 'overHeadExploracao',
            'overHeadProducao', 'overHeadTotal', 'valorReconhecidoComOH'
        ]
        
        self.campos_correcoes = [
            'valorReconhecido', 'valorReconhecidoComOH', 'overHeadExploracao',
            'overHeadProducao', 'overHeadTotal', 'diferencaValor',
            'valorReconhecidoComOhOriginal', 'valorLancamentoTotal',
            'valorNaoPassivelRecuperacao', 'valorReconhecivel', 'valorNaoReconhecido',
            'valorReconhecidoExploracao', 'valorReconhecidoProducao',
            'igpmAcumuladoReais' , 'valorRecuperado', 'valorRecuperadoTotal',
            'igpmAcumulado'
        ]
        
        self.campos_sem_correcao = [
            'valorRecuperado', 'valorRecuperadoTotal', 'igpmAcumulado'
        ]
        
        self.comparacoes = []
    
    def _converter_decimal(self, valor):
        """Converte valor para Decimal, tratando tipos MongoDB"""
        if valor is None:
            return Decimal('0')
        
        if isinstance(valor, Decimal128):
            return valor.to_decimal()
        elif isinstance(valor, (int, float)):
            return Decimal(str(valor))
        elif isinstance(valor, str):
            if 'E' in valor.upper():
                return Decimal(valor)
            return Decimal(valor)
        else:
            return Decimal(str(valor))
    
    def _recalcular_valor(self, valor_atual, campo):
        """
        Recalcula um valor aplicando o novo TP
        
        Args:
            valor_atual: Valor atual (com TP original aplicado)
            campo: Nome do campo para logging
            
        Returns:
            Decimal128: Novo valor no formato MongoDB
        """
        if valor_atual is None:
            return Decimal128('0')
        
        valor_decimal = self._converter_decimal(valor_atual)
        
        if valor_decimal == 0:
            return Decimal128('0')
        
        # Aplicar fator de conversão
        novo_valor = valor_decimal * self.fator_conversao
        
        # Arredondar para 15 casas decimais
        novo_valor = novo_valor.quantize(Decimal('0.000000000000001'), rounding=ROUND_HALF_UP)
        
        # Registrar comparação
        diferenca = novo_valor - valor_decimal
        self.comparacoes.append({
            'campo': campo,
            'valor_original': float(valor_decimal),
            'valor_novo': float(novo_valor),
            'diferenca': float(diferenca),
            'percentual_variacao': float((diferenca / valor_decimal * 100) if valor_decimal != 0 else 0)
        })
        
        return Decimal128(novo_valor)
    
    def recalcular_cco(self, cco_doc):
        """
        Recalcula todos os valores da CCO com o novo TP
        
        Args:
            cco_doc (dict): Documento da CCO do MongoDB
            
        Returns:
            dict: CCO com valores recalculados
        """
        cco_nova = deepcopy(cco_doc)
        self.comparacoes = []
        
        cco_id = cco_doc.get('_id', 'N/A')
        print(f"\n=== Recalculando CCO: {cco_id} ===")
        print(f"Modo: {self.modo_correcao}")
        
        if self.modo_correcao == "COMPLETO":
            return self._recalcular_completo(cco_nova, cco_id)
        else:  # CORRECAO_MONETARIA
            return self._adicionar_correcao_monetaria(cco_nova, cco_id)
    
    def _recalcular_completo(self, cco_nova, cco_id):
        """
        Recálculo completo da CCO (modo original)
        
        Args:
            cco_nova (dict): CCO para recalcular
            cco_id: ID da CCO para logging
            
        Returns:
            dict: CCO recalculada
        """
        print("\n--- Recálculo COMPLETO ---")
        
        # Recalcular campos da raiz
        print("\n--- Campos raiz ---")
        for campo in self.campos_raiz:
            if campo in cco_nova:
                valor_atual = cco_nova[campo]
                valor_novo = self._recalcular_valor(valor_atual, f"raiz.{campo}")
                cco_nova[campo] = valor_novo
                
                print(f"{campo}: {self._converter_decimal(valor_atual):,.2f} -> {valor_novo.to_decimal():,.2f}")
        
        # Recalcular correções monetárias
        if 'correcoesMonetarias' in cco_nova and cco_nova['correcoesMonetarias']:
            print("\n--- Correções monetárias ---")
            for i, correcao in enumerate(cco_nova['correcoesMonetarias']):
                print(f"\nCorreção {i+1} - {correcao.get('tipo', 'N/A')}")
                
                for campo in self.campos_correcoes:
                    if campo in correcao:
                        valor_atual = correcao[campo]
                        valor_novo = self._recalcular_valor(valor_atual, f"correcao[{i}].{campo}")
                        correcao[campo] = valor_novo
                        
                        print(f"  {campo}: {self._converter_decimal(valor_atual):,.2f} -> {valor_novo.to_decimal():,.2f}")
        
        return cco_nova
    
    def _adicionar_correcao_monetaria(self, cco_nova, cco_id):
        """
        Adiciona nova correção monetária do tipo RETIFICACAO
        
        Args:
            cco_nova (dict): CCO para adicionar correção
            cco_id: ID da CCO para logging
            
        Returns:
            dict: CCO com nova correção monetária
        """
        print("\n--- Adicionando CORREÇÃO MONETÁRIA ---")
        
        # Obter valores atuais (da última correção ou da raiz)
        valores_atuais, ultimoValorReconhecidoComOH  = self._obter_valores_atuais(cco_nova)
        
        
        # Calcular novos valores
        nova_correcao = self._criar_correcao_retificacao(valores_atuais, cco_nova, ultimoValorReconhecidoComOH)
        
        
        
        # Inicializar array de correções se não existir
        if 'correcoesMonetarias' not in cco_nova:
            cco_nova['correcoesMonetarias'] = []
            
        if nova_correcao is not None and 'valorReconhecidoComOH' in nova_correcao and self._converter_decimal(nova_correcao['valorReconhecidoComOH']) != 0 :
            print(f"\n✓ Ajustando CCO para 'Não recuperado'. Novo valor reconhecido com OH: {self._converter_decimal(nova_correcao['valorReconhecidoComOH']):,.5f}")
            cco_nova['flgRecuperado'] = False
        
        # Adicionar nova correção
        cco_nova['correcoesMonetarias'].append(nova_correcao)
        
        print(f"\n✓ Nova correção RETIFICACAO adicionada")
        print(f"  Total de correções: {len(cco_nova['correcoesMonetarias'])}")
        
        return cco_nova
    
    def _obter_valores_atuais(self, cco_doc):
        """
        Obtém valores atuais da CCO (da última correção ou da raiz)
        
        Args:
            cco_doc (dict): Documento da CCO
            
        Returns:
            dict: Valores atuais por campo
        """
        valores = {}
        ultimoValorReconhecidoComOH = None
        
        # Se tem correções monetárias, usar a última
        if 'correcoesMonetarias' in cco_doc and cco_doc['correcoesMonetarias']:
            fonte = cco_doc['correcoesMonetarias'][-1]
            print(f"Usando valores da última correção: {fonte.get('tipo', 'N/A')}")
        else:
            fonte = cco_doc
            print("Usando valores da raiz da CCO")
            
        
        if 'correcoesMonetarias' in cco_doc and cco_doc['correcoesMonetarias']:
            # iterar nas correcoesMonetarias na ordem invertida, até encontrar valor na propriedade valorReconhecidoComOH
            for correcao in reversed(cco_doc['correcoesMonetarias']):
                if 'valorReconhecidoComOH' in correcao and (self._converter_decimal(correcao['valorReconhecidoComOH']) != 0): #TODO considerar valores negativos?!
                    if self._converter_decimal(correcao['valorReconhecidoComOH']) < 0:
                        print("ATENÇÃO: Correção monetária encontrada com valorReconhecidoComOH negativo")
                        
                    ultimoValorReconhecidoComOH = correcao['valorReconhecidoComOH']
                    print(f"Usando valorReconhecidoComOH da última correção: {ultimoValorReconhecidoComOH}")
                    break
                
        else:
            ultimoValorReconhecidoComOH = cco_doc['valorReconhecidoComOH']
            print("Nenhuma correção monetária encontrada, usando valor da raiz da CCO para valorReconhecidoComOH: {ultimoValorReconhecidoComOH}")
            
            
        
        
        # Extrair campos relevantes
        for campo in self.campos_correcoes:
            if campo in fonte:
                valores[campo] = fonte[campo]
            elif campo in cco_doc:  # Fallback para raiz se não estiver na correção
                valores[campo] = cco_doc[campo]
            else:
                valores[campo] = Decimal128('0')
        
        return valores, ultimoValorReconhecidoComOH
    
    def _criar_correcao_retificacao(self, valores_atuais, cco_doc, ultimoValorReconhecidoComOH = None):
        """
        Cria nova correção monetária do tipo RETIFICACAO
        
        Args:
            valores_atuais (dict): Valores atuais da CCO
            cco_doc (dict): Documento CCO original
            
        Returns:
            dict: Nova correção monetária
        """
        timestamp_agora = datetime.now(timezone.utc)
        
        nova_correcao = {
            'tipo': 'RETIFICACAO',
            'subTipo': f'TP_CORRECTION_{self.tp_original}_TO_{self.tp_correcao}',
            'dataCorrecao': timestamp_agora,
            'dataCriacaoCorrecao': timestamp_agora,
            'ativo': True,
            'observacao': f'Retificação automática TP de {self.tp_original} para {self.tp_correcao}',
            'taxaCorrecao': valores_atuais.get('taxaCorrecao',  Decimal128('0')), 
            'igpmAcumulado': valores_atuais.get('igpmAcumulado', Decimal128('0')),
            'transferencia': False,
            
            # Campos básicos copiados da CCO
            'contrato': cco_doc.get('contratoCpp', ''),
            'campo': cco_doc.get('campo', ''),
            'remessa': cco_doc.get('remessa', 0),
            'faseRemessa': cco_doc.get('faseRemessa', ''),
            'exercicio': cco_doc.get('exercicio', 0),
            'periodo': cco_doc.get('periodo', 0),
            'quantidadeLancamento': cco_doc.get('quantidadeLancamento', 0)
        }
        
        print("\n--- Calculando valores da correção RETIFICACAO ---")
        
        # Recalcular cada campo e calcular diferenças
        for campo in self.campos_correcoes:
            
            if campo in self.campos_sem_correcao: # Ignorar campos que não devem ser alterados
                print(f"--- Ignorando campo {campo}. Não precisa ser recalculado ---")
                valor_atual = valores_atuais.get(campo, Decimal128('0'))
                nova_correcao[campo] = valor_atual
            else:
            
                valor_atual = valores_atuais.get(campo, Decimal128('0'))
                valor_novo = self._recalcular_valor(valor_atual, f"retificacao.{campo}")
                diferenca = valor_novo.to_decimal() - self._converter_decimal(valor_atual)
                
                nova_correcao[campo] = valor_novo
                
                # Para alguns campos, calcular a diferença explicitamente
                if campo == 'valorReconhecidoComOH' or campo == 'diferencaValor' or 'valorReconhecidoComOhOriginal' in campo:

                    if ultimoValorReconhecidoComOH is not None and cco_doc.get('flgRecuperado', False) == True:
                        
                        print(f"### Usando valorReconhecidoComOH anterior para calcular diferença: {ultimoValorReconhecidoComOH}")
                        valorReconhecidoComOHRecalculado = self._recalcular_valor(ultimoValorReconhecidoComOH, f"retificacao_ultimo_valorReconhecidoComOH.{campo}")
                        print(f"### Valor reconhecido com OH recalculado: {valorReconhecidoComOHRecalculado}")
                        diferencaValorReconhecidoComOH = valorReconhecidoComOHRecalculado.to_decimal() - self._converter_decimal(ultimoValorReconhecidoComOH)
                        print(f"### Diferenca valor reconhecido com OH: {diferencaValorReconhecidoComOH}")
                        nova_correcao['valorReconhecidoComOH'] = Decimal128(diferencaValorReconhecidoComOH)
                        nova_correcao['diferencaValor'] = Decimal128(diferencaValorReconhecidoComOH) # verificar

                        nova_correcao['valorReconhecidoComOhOriginal'] = Decimal128(self._converter_decimal(ultimoValorReconhecidoComOH))
                        
                    elif campo == 'diferencaValor':
                        nova_correcao['diferencaValor'] = Decimal128(diferenca) # verificar
                        
                # elif campo == 'igpmAcumuladoReais':
                #     nova_correcao['igpmAcumuladoReais'] = Decimal128(diferenca)
                
                print(f"  {campo}: {self._converter_decimal(valor_atual):,.2f} -> {valor_novo.to_decimal():,.2f} (Δ: {diferenca:,.2f})")
            
      

        # Aplicar fator de conversão    
        
        # Adicionar metadados específicos da retificação
        nova_correcao['metadadosRetificacao'] = {
            'tpOriginal': Decimal128(str(self.tp_original)),
            'tpCorrecao': Decimal128(str(self.tp_correcao)),
            'fatorConversao': Decimal128(str(self.fator_conversao)),
            'versaoScript': '2.0',
            'tipoOperacao': 'RETIFICACAO_TP'
        }
        
        return nova_correcao
    
    def gerar_csv_comparacao(self, nome_arquivo, cco_id=None):
        """Gera CSV com comparação dos valores"""
        with open(nome_arquivo, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'cco_id', 'campo', 'valor_original', 'valor_novo', 
                'diferenca', 'percentual_variacao', 'tp_original', 'tp_correcao'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            
            for comp in self.comparacoes:
                writer.writerow({
                    'cco_id': str(cco_id) or 'N/A',
                    'campo': comp['campo'],
                    'valor_original': f"{comp['valor_original']:.15f}".replace('.', ','),
                    'valor_novo': f"{comp['valor_novo']:.15f}".replace('.', ','),
                    'diferenca': f"{comp['diferenca']:.15f}".replace('.', ','),
                    'percentual_variacao': f"{comp['percentual_variacao']:.6f}".replace('.', ','),
                    'tp_original': str(self.tp_original).replace('.', ','),
                    'tp_correcao': str(self.tp_correcao).replace('.', ',')
                })
        
        print(f"\n✓ CSV salvo: {nome_arquivo}")
    
    def salvar_json_resultado(self, cco_recalculada, arquivo_saida):
        """Salva resultado em JSON, convertendo tipos MongoDB para serialização"""
        def converter_para_json(obj):
            if isinstance(obj, Decimal128):
                return float(obj.to_decimal())
            elif isinstance(obj, ObjectId):
                return str(obj)
            elif hasattr(obj, 'isoformat'):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {key: converter_para_json(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [converter_para_json(item) for item in obj]
            else:
                return obj
        
        cco_json = converter_para_json(cco_recalculada)
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump(cco_json, f, indent=2, ensure_ascii=False)
    
    def processar_por_id(self, cco_id, observacoes=None):
        """
        Processa CCO buscando por ID no MongoDB
        
        Args:
            cco_id (str): ID da CCO no MongoDB
            observacoes (str): Observações adicionais para salvamento
        """
        print(f"\n=== Processando CCO por ID: {cco_id} ===")
        
        # Conectar ao MongoDB origem
        if not self.mongo_connector.conectar():
            return False
        
        # Conectar ao MongoDB local se necessário
        if self.salvar_resultado and self.mongo_local:
            if not self.mongo_local.conectar():
                print("⚠️  Falha na conexão local, continuando sem salvamento...")
                self.salvar_resultado = False
        
        try:
            # Buscar CCO
            cco_original = self.mongo_connector.buscar_cco_por_id(cco_id)
            if not cco_original:
                return False
            
            # Verificar se já existe recálculo
            if self.salvar_resultado and self.mongo_local and not self.sobrescrever:
                existente = self.mongo_local.verificar_se_existe(cco_id, float(self.tp_correcao))
                if existente:
                    resposta = input("Deseja continuar mesmo assim? (s/N): ")
                    if resposta.lower() != 's':
                        print("❌ Operação cancelada pelo usuário")
                        return False
            
            # Recalcular
            cco_recalculada = self.recalcular_cco(cco_original)
            
            # Salvar resultados
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            arquivo_json = f"cco_{cco_id}_recalculado_TP_{str(self.tp_correcao).replace('.', '_')}_{timestamp}.json"
            arquivo_csv = f"cco_{cco_id}_comparacao_TP_{str(self.tp_correcao).replace('.', '_')}_{timestamp}.csv"
            
            # Salvar arquivos locais
            self.salvar_json_resultado(cco_recalculada, arquivo_json)
            self.gerar_csv_comparacao(arquivo_csv, cco_id)
            
            # Salvar no MongoDB local
            novo_id_mongo = None
            if self.salvar_resultado and self.mongo_local:
                novo_id_mongo = self.mongo_local.salvar_cco_recalculada(
                    cco_recalculada, 
                    tp_original=float(self.tp_original),
                    tp_correcao=float(self.tp_correcao),
                    observacoes=observacoes
                )
            
            self._exibir_resumo(arquivo_json, arquivo_csv, novo_id_mongo)
            return True
            
        finally:
            self.mongo_connector.fechar_conexao()
            if self.mongo_local:
                self.mongo_local.fechar_conexao()
    
    def processar_por_arquivo(self, caminho_arquivo, observacoes=None):
        """
        Processa CCO a partir de arquivo JSON
        
        Args:
            caminho_arquivo (str): Caminho para arquivo JSON
            observacoes (str): Observações adicionais para salvamento
        """
        print(f"\n=== Processando arquivo: {caminho_arquivo} ===")
        
        # Conectar ao MongoDB local se necessário
        if self.salvar_resultado and self.mongo_local:
            if not self.mongo_local.conectar():
                print("⚠️  Falha na conexão local, continuando sem salvamento...")
                self.salvar_resultado = False
        
        try:
            with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Tentar parsear diretamente como JSON
            try:
                cco_original = json.loads(content)
            except json.JSONDecodeError:
                # Se falhar, tentar converter BSON para JSON
                cco_original = self._parse_bson_json(content)
            
            cco_id = cco_original.get('_id')
            
            # Verificar se já existe recálculo
            if self.salvar_resultado and self.mongo_local and cco_id and not self.sobrescrever:
                existente = self.mongo_local.verificar_se_existe(cco_id, float(self.tp_correcao))
                if existente:
                    resposta = input("Deseja continuar mesmo assim? (s/N): ")
                    if resposta.lower() != 's':
                        print("❌ Operação cancelada pelo usuário")
                        return False
            
            # Recalcular
            cco_recalculada = self.recalcular_cco(cco_original)
            
            # Salvar resultados
            nome_base = os.path.splitext(caminho_arquivo)[0]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            arquivo_json = f"{nome_base}_recalculado_TP_{str(self.tp_correcao).replace('.', '_')}_{timestamp}.json"
            arquivo_csv = f"{nome_base}_comparacao_TP_{str(self.tp_correcao).replace('.', '_')}_{timestamp}.csv"
            
            # Salvar arquivos locais
            self.salvar_json_resultado(cco_recalculada, arquivo_json)
            self.gerar_csv_comparacao(arquivo_csv, cco_id)
            
            # Salvar no MongoDB local
            novo_id_mongo = None
            if self.salvar_resultado and self.mongo_local:
                novo_id_mongo = self.mongo_local.salvar_cco_recalculada(
                    cco_recalculada,
                    tp_original=float(self.tp_original),
                    tp_correcao=float(self.tp_correcao),
                    observacoes=observacoes or f"Recálculo a partir de arquivo: {caminho_arquivo}"
                )
            
            self._exibir_resumo(arquivo_json, arquivo_csv, novo_id_mongo)
            return True
            
        except FileNotFoundError:
            print(f"✗ Arquivo não encontrado: {caminho_arquivo}")
            return False
        except Exception as e:
            print(f"✗ Erro ao processar arquivo: {e}")
            return False
        finally:
            if self.mongo_local:
                self.mongo_local.fechar_conexao()
    
    def listar_recalculos_salvos(self):
        """Lista recálculos salvos no MongoDB local"""
        if not self.salvar_resultado or not self.mongo_local:
            print("❌ Funcionalidade de salvamento não está habilitada")
            return
        
        if not self.mongo_local.conectar():
            return
        
        try:
            self.mongo_local.listar_recalculos()
        finally:
            self.mongo_local.fechar_conexao()
    
    def _parse_bson_json(self, content):
        """Parse básico de JSON com tipos BSON"""
        conversoes = [
            (r'NumberDecimal\("([^"]+)"\)', r'"\1"'),
            (r'NumberLong\((\d+)\)', r'\1'),
            (r'ObjectId\("([^"]+)"\)', r'"\1"'),
            (r'ISODate\("([^"]+)"\)', r'"\1"'),
        ]
        
        content_processado = content
        for padrao, substituicao in conversoes:
            content_processado = re.sub(padrao, substituicao, content_processado)
        
        return json.loads(content_processado)
    
    def _exibir_resumo(self, arquivo_json, arquivo_csv, novo_id_mongo=None):
        """Exibe resumo da operação"""
        print(f"\n=== RESUMO ===")
        print(f"✓ JSON recalculado: {arquivo_json}")
        print(f"✓ CSV comparativo: {arquivo_csv}")
        if novo_id_mongo:
            print(f"✓ Salvo no MongoDB: {novo_id_mongo}")
        print(f"📊 Campos recalculados: {len(self.comparacoes)}")
        print(f"📈 Fator conversão: {self.fator_conversao}")
        
        if self.comparacoes:
            diferencas = [abs(c['diferenca']) for c in self.comparacoes]
            print(f"💰 Maior diferença: R$ {max(diferencas):,.2f}")
            print(f"💰 Diferença média: R$ {sum(diferencas)/len(diferencas):,.2f}")


def main():
    """Função principal"""
    if len(sys.argv) < 4:
        print("Uso: python recalculo_tp_mongo.py <entrada> <tp_original> <tp_correcao> [opções]")
        print("")
        print("Entrada pode ser:")
        print("  - ID da CCO (para buscar no MongoDB)")
        print("  - Caminho para arquivo JSON")
        print("  - 'listar' (para listar recálculos salvos)")
        print("")
        print("Opções:")
        print("  --mongo-uri <uri>           : URI MongoDB origem")
        print("  --mongo-local <uri>         : URI MongoDB local (padrão: mongodb://localhost:27017/)")
        print("  --database-local <nome>     : Nome da base local (padrão: temp_recalculos)")
        print("  --no-save                   : Não salvar no MongoDB local")
        print("  --sobrescrever              : Sobrescrever registros existentes")
        print("  --observacoes <texto>       : Observações para o recálculo")
        print("  --modo <tipo>               : COMPLETO ou CORRECAO_MONETARIA (padrão: COMPLETO)")
        print("")
        print("Tipos de correção:")
        print("  COMPLETO                    : Recalcula todos os valores da CCO")
        print("  CORRECAO_MONETARIA          : Adiciona nova correção RETIFICACAO")
        print("")
        print("Exemplos:")
        print("  python recalculo_tp_mongo.py DMGgBNfeQwGhbb_YFlVC5AAAAAA 60.407 60.408")
        print("  python recalculo_tp_mongo.py cco_exemplo.json 60,407 60,408 --observacoes 'Teste recálculo'")
        print("  python recalculo_tp_mongo.py cco_id 60.407 60.408 --modo CORRECAO_MONETARIA")
        print("  python recalculo_tp_mongo.py listar")
        print("  python recalculo_tp_mongo.py cco_id 60.407 60.408 --mongo-uri mongodb://prod:27017 --mongo-local mongodb://localhost:27017")
        sys.exit(1)
    
    entrada = sys.argv[1]
    
    # Comando especial para listar
    if entrada.lower() == 'listar':
        # Parâmetros padrão para listagem
        recalculador = TPRecalculatorMongoDB(0, 0, salvar_resultado=True)
        recalculador.listar_recalculos_salvos()
        return
    
    # Validar parâmetros obrigatórios
    if len(sys.argv) < 4:
        print("✗ Erro: TP_ORIGINAL e TP_CORRECAO são obrigatórios")
        sys.exit(1)
    
    try:
        tp_original = float(sys.argv[2].replace(',', '.'))
        tp_correcao = float(sys.argv[3].replace(',', '.'))
    except ValueError:
        print("✗ Erro: TP_ORIGINAL e TP_CORRECAO devem ser números válidos")
        sys.exit(1)
    
    # Parsear argumentos opcionais
    mongo_uri = None
    mongo_uri_local = None
    database_local = "temp_recalculos"
    salvar_resultado = False
    sobrescrever = False
    observacoes = None
    modo_correcao = "COMPLETO"
    
    i = 4
    while i < len(sys.argv):
        arg = sys.argv[i]
        
        if arg == '--mongo-uri' and i + 1 < len(sys.argv):
            mongo_uri = sys.argv[i + 1]
            i += 2
        elif arg == '--mongo-local' and i + 1 < len(sys.argv):
            mongo_uri_local = sys.argv[i + 1]
            i += 2
        elif arg == '--database-local' and i + 1 < len(sys.argv):
            database_local = sys.argv[i + 1]
            i += 2
        elif arg == '--observacoes' and i + 1 < len(sys.argv):
            observacoes = sys.argv[i + 1]
            i += 2
        elif arg == '--modo' and i + 1 < len(sys.argv):
            modo_correcao = sys.argv[i + 1].upper()
            if modo_correcao not in ["COMPLETO", "CORRECAO_MONETARIA"]:
                print(f"✗ Erro: Modo inválido '{modo_correcao}'. Use 'COMPLETO' ou 'CORRECAO_MONETARIA'")
                sys.exit(1)
            i += 2
        elif arg == '--no-save':
            salvar_resultado = False
            i += 1
        elif arg == '--sobrescrever':
            sobrescrever = True
            i += 1
        else:
            print(f"⚠️  Argumento desconhecido ignorado: {arg}")
            i += 1
    
    # Criar recalculador
    recalculador = TPRecalculatorMongoDB(
        tp_original, 
        tp_correcao, 
        mongo_uri=mongo_uri,
        mongo_uri_local=mongo_uri_local,
        database_local=database_local,
        salvar_resultado=salvar_resultado,
        sobrescrever=sobrescrever,
        modo_correcao=modo_correcao
    )
    
    #TODO verificar chamada desse método
    print("********* PARADA FORCADA --- NANALISAR!!!! - util\\recalculo_tp.py:main()")
    if True == True:
        return
    
    # Determinar tipo de entrada
    if os.path.isfile(entrada):
        # É um arquivo
        sucesso = recalculador.processar_por_arquivo(entrada, observacoes)
    else:
        # Assumir que é um ID de CCO
        sucesso = recalculador.processar_por_id(entrada, observacoes)
    
    if not sucesso:
        print("✗ Processamento falhou")
        sys.exit(1)
    else:
        print("✓ Processamento concluído com sucesso")


if __name__ == "__main__":
    main()