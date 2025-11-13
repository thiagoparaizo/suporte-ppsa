# Regras de Remessas e CCOs - Sistema de Reconhecimento de Gastos

## 1. Visão Geral do Sistema

O sistema é responsável por gerenciar o processo de reconhecimento de gastos durante as operações de **Exploração e Produção de Petróleo** no Pré-Sal Brasileiro. O fluxo envolve dois principais atores:

- **Operadores**: Empresas que operam dentro de contratos de partilha para explorar e produzir petróleo
- **PPSA (Pré-Sal Petróleo S.A.)**: Empresa gestora que analisa e valida os gastos enviados pelos operadores

## 2. Conceitos Fundamentais

### 2.1 CCO (Conta Custo Óleo)
A **CCO (conta_custo_oleo_entity)** é uma entidade que representa uma "entrada" em uma conta corrente do sistema. Características principais:

- Cada registro consolida valores de gastos reconhecidos
- Funciona como um sistema de saldo acumulativo
- Valores podem ser modificados ao longo do tempo por correções monetárias, recuperações e ajustes
- Possui um ciclo de vida desde o reconhecimento até a recuperação total

### 2.2 Remessa
Uma **remessa** é o conjunto de gastos enviados mensalmente pelos operadores, contendo:

- Detalhamento de gastos de um período específico
- Atributos de identificação (tipo, valores, classificações)
- Dados do contrato e campo de atuação
- Informações do período de competência

## 3. Processo de Reconhecimento de Gastos

### 3.1 Fases do Processo
O processo de reconhecimento possui **5 fases sequenciais**:

1. **MEN (Mensal)**: Fase inicial quando a remessa é enviada
2. **ROP (RESPOSTA AO OPERADOR)**: Segunda tentativa de reconhecimento
3. **RAD (RECURSO ADMINISTRATIVO)**: Terceira tentativa de reconhecimento  
4. **REC (RECALCULO)**: Quarta tentativa de reconhecimento
5. **REV (REVISÃO)**: Fase de revisão


**Objetivo**: Cada fase oferece ao operador uma nova oportunidade de ajustar e tentar reconhecer gastos que foram rejeitados (não reconhecidos) em fases anteriores.

### 3.2 Classificações de Gastos
Durante a análise, cada gasto pode receber as seguintes classificações:

- **Reconhecidos**: Gastos aprovados e aceitos
- **Não Reconhecidos**: Gastos rejeitados que podem ser reajustados
- **Recusados**: Gastos definitivamente rejeitados
- **Não Passíveis de Recuperação**: Gastos que não podem ser recuperados posteriormente

### 3.3 Critérios de Análise
- **Análise Manual**: Realizada pela equipe da PPSA
- **Regras Automáticas**: Sistema aplica validações automáticas baseadas em regras pré-definidas, como reconhecimento automático.

## 4. Estrutura da CCO

### 4.1 Campos Principais
```json
{
  "_id": "Identificador único da CCO",
  "contratoCpp": "Código do contrato",
  "campo": "Nome do campo petrolífero",
  "remessa": "Número da remessa",
  "remessaExposicao": "Número de exposição da remessa",
  "faseRemessa": "Fase atual (MEN/ROP/RAD/REC/REV)",
  "exercicio": "Ano de exercício",
  "periodo": "Período (mês)",
  "mesAnoReferencia": "Referência MM/YYYY"
}
```

### 4.2 Valores Financeiros
```json
{
  "valorLancamentoTotal": "Valor total lançado na remessa",
  "valorReconhecido": "Valor reconhecido (sem overhead)",
  "valorReconhecidoComOH": "Valor reconhecido com overhead",
  "valorNaoReconhecido": "Valor não reconhecido",
  "valorReconhecivel": "Valor total passível de reconhecimento",
  "valorNaoPassivelRecuperacao": "Valor que não pode ser recuperado"
}
```

### 4.3 Overhead
```json
{
  "overHeadExploracao": "Overhead para atividades de exploração",
  "overHeadProducao": "Overhead para atividades de produção",
  "overHeadTotal": "Overhead total aplicado"
}
```

### 4.4 Controle de Status
```json
{
  "flgRecuperado": "Boolean - indica se foi totalmente recuperado",
  "origemDosGastos": "GASTO_EXCLUSIVO, GASTO_JAZIDA_COMPARTILHADA, GASTO_ATIVO_COMARTILHADO, GASTO_AEGV",
  "dataReconhecimento": "Data do reconhecimento (criação da CCO)",
  "dataLancamento": "Data de cdo lancamento dos gastos",
}
```

## 5. Sistema de Correções Monetárias

### 5.1 Array correcoesMonetarias
Cada CCO possui um array `correcoesMonetarias` que registra **todas as alterações** realizadas no registro ao longo do tempo. A **última entrada** sempre representa o **status atual** da CCO.

### 5.2 Tipos de Correções

#### 5.2.1 IPCA
- **Propósito**: Correção monetária por índice IPCA
- **Frequência**: Aplicada quando a CCO completa 1 ano com saldo (ou valor negativo)
- **Impacto**: Atualiza valores conforme inflação
- **Observação**: Só não aplicado se o valor for igual a zero

```json
{
  "tipo": "IPCA",
  "subTipo": "DEFAULT",
  "taxaCorrecao": "Taxa aplicada",
  "igpmAcumulado": "Índice acumulado",
  "igpmAcumuladoReais": "Valor em reais do ajuste",
  "diferencaValor": "Diferença entre o valor atual e o ajustado",
  "dataCorrecao": "Data da correção (data em que a correção deve ser considerada)",
  "dataCriacaoCorrecao": "Data de criação do registro da correção"
}
```

#### 5.2.2 IGPM
- **Propósito**: Correção monetária por índice IGPM
- **Aplicação**: Similar ao IPCA, mas usando índice IGPM

#### 5.2.3 RECUPERACAO
- **Propósito**: "Saque" de valores da CCO
- **Contexto**: Quando campo entra em produção e permite recuperação dos investimentos
- **Impacto**: Reduz o `valorReconhecidoComOH`

```json
{
  "tipo": "RECUPERACAO",
  "valorRecuperado": "Valor recuperado nesta operação",
  "valorRecuperadoTotal": "Valor total recuperado acumulado"
}
```

#### 5.2.4 RETIFICACAO
- **Propósito**: Ajustes manuais nos valores
- **Motivo**: Correção de erros ou reclassificações
- **Observação**: Detalhes no campo `observacao`

#### 5.2.5 INVALIDACAO_RECONHECIMENTO_PARCIAL
- **Propósito**: Invalidação parcial do reconhecimento
- **Resultado**: Transferência de valores para outra CCO

### 5.3 Regras de Precedência
1. **Última Correção**: Sempre prevalece sobre valores da raiz. A dataCorrecao indica a ordenação das correções
2. **Atributos Faltantes**: Se não existir na correção, usa-se o valor da raiz da CCO
3. **Status Atual da CCO**: Determinado pela última entrada em `correcoesMonetarias`
4. **Atributos de Status da Correção Monetária**: Determinado pelo atributo `ativo`. `ativo = true` indica que a correção é considerada e `ativo = false` indica que a correção foi desativada

## 6. Processo de Recuperação

### 6.1 Condições para Recuperação
- Campo deve estar **em produção** (não mais em exploração)
- CCO deve ter saldo disponível (`valorReconhecidoComOH > 0`)
- CCO não deve estar marcada como totalmente recuperada (`flgRecuperado = false`)
- quando o saldo da CCO for negativo, ela será considerada na composição do saldo total, e será compensada na recuperação, se necessário. Nesse caso, a CCO deve ser marcada como totalmente recuperada (`flgRecuperado = true`) e o saldo da CCO deve ser zerado (`valorReconhecidoComOH = 0`)

### 6.2 Cálculo de Recuperação
- Baseado na **produção mensal** do campo
- Limitado por **regras específicas** de recuperação mensal
- Considera o **saldo atual** de todas as CCOs do contrato/campo

### 6.3 Sequência de Recuperação
1. Identifica CCOs elegíveis para recuperação (`flgRecuperado = false` e `valorReconhecidoComOH != 0`)
2. Calcula valor máximo recuperável no período
3. Aplica recuperação até zerar CCOs
4. Atualiza `flgRecuperado = true` quando CCO é totalmente recuperada (igualando `valorReconhecidoComOH = 0`)

## 7. Origem dos Gastos

### 7.1 GASTO_EXCLUSIVO
- Gastos específicos de um único campo/contrato
- Recuperação direta pelo próprio campo

### 7.2 GASTO_JAZIDA_COMPARTILHADA
- Gastos compartilhados entre múltiplos campos
- Requer rateio proporcional na recuperação
- Exemplo: Infraestrutura compartilhada

## 8. Regras de Negócio Importantes

### 8.1 Geração de CCO
- CCO é gerada **apenas** quando existe pelo menos 1 gasto reconhecido na fase
- Uma CCO por **contrato + campo + remessa + fase**
- Consolida todos os gastos reconhecidos da remessa/fase

### 8.2 Overhead
- Calculado como percentual sobre valores reconhecidos gastos na fase de produção
- Diferenciado entre exploração e produção
- Sempre incluso no `valorReconhecidoComOH`
- faixa de cálculo de OH depende do montante do total produzido

### 8.3 Correção Monetária IPCA/IGPM
- A rotina de autualização é iniciado a partir do processo Atualizar Indice Correção Monetária, iniciado automaticamente todas início de mes
- Aplicada **automaticamente** após 1 ano a partir da data de reconhecimento (mes e ano de reconhecimento)
- Considera o índice configurado no sistema (IPCA ou IGPM)
- Baseada em índices oficiais (IPCA/IGPM), recuperado nas coleçoes ipca_entity e igpm_entity. 
- Para IPCA, considera o registro armazenado no mes ano de referência
- Preserva histórico completo de alterações


### 8.4 Transferências de valores
- CCOs podem ter valores transferidos entre si
- Registrado como `INVALIDACAO_RECONHECIMENTO_PARCIAL`
- Mantém rastreabilidade completa

## 9. Ciclo de Vida da CCO

```
Criação → Correções Monetárias → Recuperação Parcial → Recuperação Total
   ↓              ↓                      ↓                    ↓
Remessa    Anualmente/Ajustes      Produção Mensal      flgRecuperado=true
```

### 9.1 Estados da CCO
1. **Criada**: Nenhuma correção aplicada
2. **Em Correção**: Recebendo ajustes monetários
3. **Em Recuperação**: Valores sendo recuperados mensalmente
4. **Recuperada**: Totalmente recuperada (`flgRecuperado = true`)

## 10. Monitoramento e Auditoria

### 10.1 Rastreabilidade
- Histórico completo em `correcoesMonetarias`
- Versionamento através do campo `version`
- Data de criação e modificação de cada correção

### 10.2 Validações de Integridade
- Soma de valores deve ser consistente
- Recuperação não pode exceder saldo disponível
- Fases devem seguir sequência temporal

## 11. Exemplos Práticos

### 11.1 CCO Bacalhau Norte
- **Contrato**: Norte_de_Carcará_P2
- **Origem**: GASTO_EXCLUSIVO
- **Fase**: ROP
- **Status**: Não recuperada (`flgRecuperado: false`)
- **Correções**: 9 correções aplicadas (IPCA, IGPM, RETIFICACAO)

### 11.2 CCO Itapu  
- **Contrato**: Itapu
- **Origem**: GASTO_JAZIDA_COMPARTILHADA
- **Fase**: ROP
- **Status**: Recuperada (`flgRecuperado: true`)
- **Recuperação Total**: R$ 61.703.433,61

---

*Esta documentação serve como guia completo para entendimento das regras de negócio do sistema de reconhecimento de gastos e gestão de CCOs.*