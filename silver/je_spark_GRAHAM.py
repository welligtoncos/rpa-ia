#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script PySpark Unificado - Auditoria de Lançamentos Contábeis
============================================================

Versão unificada combinando as melhores práticas dos dois scripts.
VERSÃO COM SISTEMA DE LOG INTEGRADO E ORDEM DE COLUNAS PRESERVADA
"""
import unicodedata
from unidecode import unidecode
from pyspark.sql.functions import col, lower, when, lit, regexp_replace
import os
import sys
import time
import logging
import json
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from pyspark.sql.functions import col, when, abs, pmod, lit
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import abs as spark_abs  
import re
from unidecode import unidecode
from pyspark.sql import SparkSession, functions as F

# ========================================  
# ⭐ CONFIGURAÇÕES DE PERFORMANCE
# ========================================

# ⭐ NOVA VARIÁVEL GLOBAL - Ordem original das colunas
ORDEM_COLUNAS_ORIGINAL = None

# Configuração de threads (ajuste conforme sua máquina)
NUMERO_THREADS = 6  # ⭐ Aumentado para melhor performance

# Memória do Spark (ajuste conforme disponível)
SPARK_DRIVER_MEMORY = "8g"  # ⭐ Aumentado de 4g para 8g
SPARK_EXECUTOR_MEMORY = "8g"  # ⭐ Aumentado de 4g para 8g
SPARK_MAX_RESULT_SIZE = "8g"  # ⭐ Aumentado de 2g para 4g

# Configuração de salvamento XLSX
XLSX_CHUNK_SIZE = 100000  # ⭐ Salvar em chunks de 100k linhas
XLSX_TIMEOUT = 600  # ⭐ Timeout de 10 minutos para XLSX
XLSX_MAX_ROWS = 1000000  # ⭐ Limite do Excel (1 milhão)

# ========================================
# CONFIGURAÇÃO DO SISTEMA DE LOG
# ========================================

def configurar_logging():
    """Configura o sistema de logging para auditoria"""
    
    # Criar diretório de logs se não existir
    log_dir = os.path.join(SAIDA_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Nome do arquivo de log com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"auditoria_log_{timestamp}.log")
    
    # Configurar logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger('AuditoriaContabil')
    logger.info("=" * 70)
    logger.info("🚀 SISTEMA DE LOG INICIALIZADO")
    logger.info(f"📁 Arquivo de log: {log_file}")
    logger.info("=" * 70)
    
    return logger, log_file

def log_configuracao_inicial(logger):
    """Registra a configuração inicial no log"""
    logger.info("📋 CONFIGURAÇÃO INICIAL:")
    logger.info(f"   • Arquivo TB_RAZAO: {ARQ_RAZAO}")
    logger.info(f"   • Arquivo TB_FDS: {ARQ_FDS}")
    logger.info(f"   • Diretório de saída: {SAIDA_DIR}")
    logger.info(f"   • Separador CSV: '{CSV_SEPARATOR}'")
    logger.info(f"   • Feriados configurados: {len(FERIADOS)} datas")
    
    # Log da configuração dos testes
    logger.info("🧪 CONFIGURAÇÃO DOS TESTES:")
    testes_ativos = [teste for teste, ativo in TESTES_CONFIG.items() if ativo]
    testes_inativos = [teste for teste, ativo in TESTES_CONFIG.items() if not ativo]
    
    logger.info(f"   ✅ Testes ATIVOS ({len(testes_ativos)}): {', '.join(testes_ativos)}")
    logger.info(f"   ❌ Testes INATIVOS ({len(testes_inativos)}): {', '.join(testes_inativos)}")


def log_resultados_testes(logger, df_resultado, df_original, tempo_processamento):
    """Registra os resultados detalhados dos testes"""
    logger.info("🧪 RESULTADOS DOS TESTES DE AUDITORIA:")
    logger.info(f"   ⏱️ Tempo de processamento: {tempo_processamento:.2f} segundos")

    total_original = int(df_original.count())
    total_manter = int(df_resultado.filter(df_resultado["FLAG"] == "MANTER").count())
    total_retirar = int(df_resultado.filter(df_resultado["FLAG"] == "RETIRAR").count())

    logger.info(f"   📊 Total analisado: {total_original:,} registros")
    logger.info(f"   ✅ MANTER: {total_manter:,} registros ({(total_manter/total_original*100):.2f}%)")
    logger.info(f"   🚩 RETIRAR: {total_retirar:,} registros ({(total_retirar/total_original*100):.2f}%)")

    # ── RESUMO DA TABELA RESULTADO ──────────────────────────────
    logger.info("=" * 60)
    logger.info("📋 TABELA: RESULTADO (após testes)")
    logger.info(f"   📊 Linhas    : {total_original:,}")
    logger.info(f"   📐 Colunas   : {len(df_resultado.columns)}")
    logger.info("   🗂️  Lista de colunas:")
    for i, col_name in enumerate(df_resultado.columns, 1):
        dtype = dict(df_resultado.dtypes)[col_name]
        # Marca colunas de teste e auxiliares para fácil identificação
        tag = ""
        if col_name.startswith("TESTE_"):
            ativo = TESTES_CONFIG.get(col_name, False)
            tag = " ✅ ATIVO" if ativo else " ❌ INATIVO"
        elif col_name in COLUNAS_PARA_REMOVER:
            tag = " 🗑️ será removida"
        elif col_name == "FLAG":
            tag = " 🚩 FLAG"
        logger.info(f"      {i:02d}. {col_name:<35} [{dtype}]{tag}")
    logger.info("=" * 60)

    # ── DETALHES POR TESTE ──────────────────────────────────────
    logger.info("🔍 DETALHES POR TESTE:")
    colunas_teste = [c for c in df_resultado.columns if c.startswith("TESTE_")]
    for coluna in sorted(colunas_teste):
        if TESTES_CONFIG.get(coluna, False):
            count = int(df_resultado.filter(df_resultado[coluna] == "SIM").count())
            percentual = (count / total_original * 100) if total_original > 0 else 0
            status = "🎯" if count > 0 else "✅"
            logger.info(f"   {status} {coluna}: {count:,} detecções ({percentual:.2f}%)")


def log_salvamento_resultado(logger, caminho_salvo, registros_exportados, tempo_salvamento):
    """Registra informações sobre o salvamento do resultado"""
    logger.info("💾 SALVAMENTO DO RESULTADO:")
    logger.info(f"   ⏱️ Tempo de salvamento: {tempo_salvamento:.2f} segundos")
    logger.info(f"   📁 Arquivo salvo em: {caminho_salvo}")
    logger.info(f"   📊 Registros exportados: {registros_exportados:,}")

def verificar_colunas_remocao(df, logger):
    """Verifica e lista quais colunas serão removidas na exportação"""
    logger.info("🔍 VERIFICAÇÃO DE COLUNAS PARA REMOÇÃO:")
    
    colunas_originais = df.columns
    
    # Testes inativos
    testes_inativos = [teste for teste, ativo in TESTES_CONFIG.items() if not ativo]
    colunas_testes_inativos = [c for c in testes_inativos if c in colunas_originais]
    
    # Colunas auxiliares definidas
    colunas_auxiliares_existentes = [c for c in COLUNAS_PARA_REMOVER if c in colunas_originais]
    
    # Colunas que permanecerão
    colunas_removidas = set(colunas_testes_inativos + colunas_auxiliares_existentes)
    colunas_finais = [c for c in colunas_originais if c not in colunas_removidas]
    
    logger.info(f"   📊 Total de colunas originais: {len(colunas_originais)}")
    logger.info(f"   🗑️ Testes inativos a remover: {colunas_testes_inativos}")
    logger.info(f"   🗑️ Colunas auxiliares a remover: {colunas_auxiliares_existentes}")
    logger.info(f"   ✅ Colunas que permanecerão ({len(colunas_finais)}): {colunas_finais}")
    
    return colunas_finais

def log_resumo_final(logger, sucesso, tempo_total):
    """Registra o resumo final da execução"""
    logger.info("=" * 70)
    logger.info("📋 RESUMO FINAL DA EXECUÇÃO:")
    logger.info(f"   ⏱️ Tempo total de execução: {tempo_total:.2f} segundos")
    logger.info(f"   🎯 Status: {'✅ SUCESSO' if sucesso else '❌ ERRO'}")
    logger.info(f"   📅 Data/Hora de conclusão: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 70)

def gerar_relatorio_markdown(logger, tempo_total):
    """Gera um relatório em Markdown - muito mais confiável que JSON"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        md_file = os.path.join(SAIDA_DIR, "logs", f"relatorio_auditoria_{timestamp}.md")
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(md_file), exist_ok=True)
        
        # Construir relatório em Markdown
        conteudo = f"""# 📊 Relatório de Auditoria Contábil

## 📋 Metadados da Execução

- **Data/Hora**: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
- **Versão do Script**: 4.1 com Preservação de Ordem de Colunas
- **Tempo Total de Execução**: {tempo_total:.2f} segundos
- **Arquivo TB_RAZAO**: `{ARQ_RAZAO}`
- **Arquivo TB_FDS**: `{ARQ_FDS}`
- **Diretório de Saída**: `{SAIDA_DIR}`

---

## 🧪 Configuração dos Testes

### ✅ Testes Ativos
"""

        # Adicionar testes ativos
        testes_ativos = [teste for teste, ativo in TESTES_CONFIG.items() if ativo]
        for teste in testes_ativos:
            conteudo += f"- **{teste}**: Ativo ✓\n"

        conteudo += "\n### ❌ Testes Inativos\n"
        
        # Adicionar testes inativos
        testes_inativos = [teste for teste, ativo in TESTES_CONFIG.items() if not ativo]
        for teste in testes_inativos:
            conteudo += f"- **{teste}**: Inativo ✗\n"

        conteudo += f"""

---

## 🔤 Configuração de Palavras-Chave

### TESTE_1_4 (Histórico com palavras específicas)
```
{', '.join(PALAVRAS_TESTE_1_4)}
```

### TESTE_1_5 (Ajuste/Acerto)
```
{', '.join(PALAVRAS_TESTE_1_5)}
```

### TESTE_1_6 (Reversão/Estorno)
```
{', '.join(PALAVRAS_TESTE_1_6)}
```

### TESTE_1_7 (Outros/Diversas)
```
{', '.join(PALAVRAS_TESTE_1_7)}
```

### TESTE_7_6 (Variação Cambial)
```
{', '.join(PALAVRAS_TESTE_7_6)}
```

### TESTE_1_8 (Nomes Específicos)
"""
        
        for i, nome in enumerate(NOMES_TESTE_1_8, 1):
            conteudo += f"- Nome {i}: `{nome}`\n"

        conteudo += f"""

---

## 🏢 Configuração de Grupos

### Classificação de Resultado (TESTE_2_1)
```
{', '.join(CLASS_RESULTADO_TESTE_2_1)}
```

### Grupos de Receita (TESTE_6_1)
```
{', '.join(GRUPOS_RECEITA_TESTE_6_1)}
```

### CLASS_CONTA_RESULTADO (TESTE_7_1)
```
{', '.join(CLASS_CONTA_TESTE_7_1)}
```

### Receitas para Imobilizado (TESTE_7_2)
```
{', '.join(GRUPOS_RECEITA_TESTE_7_2)}
```

---

## 📅 Feriados Configurados

```
{', '.join(FERIADOS)}
```

---

## ⚙️ Configurações Técnicas

- **Separador CSV**: `{CSV_SEPARATOR}`
- **Colunas Removidas na Exportação**: 
  ```
  {', '.join(COLUNAS_PARA_REMOVER)}
  ```

---

*Relatório gerado automaticamente pelo sistema de auditoria contábil v4.1*
"""

        # Salvar o arquivo Markdown
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(conteudo)
        
        logger.info(f"📄 Relatório Markdown gerado: {md_file}")
        return md_file
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar relatório Markdown: {str(e)}")
        return None

def gerar_relatorio_csv_configuracao(logger):
    """Gera um CSV simples com a configuração para análise externa"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_config_file = os.path.join(SAIDA_DIR, "logs", f"configuracao_testes_{timestamp}.csv")
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(csv_config_file), exist_ok=True)
        
        # Construir dados para CSV
        linhas = []
        linhas.append("TESTE,STATUS,CATEGORIA,DESCRICAO")
        
        descricoes = {
            "TESTE_1_1": "Data nula",
            "TESTE_1_2": "Feriados/Fins de semana", 
            "TESTE_1_3": "Histórico nulo",
            "TESTE_1_4": "Histórico com palavras específicas",
            "TESTE_1_5": "Histórico com ajuste/acerto",
            "TESTE_1_6": "Histórico com reversão/estorno",
            "TESTE_1_7": "Histórico com outros/diversas",
            "TESTE_1_8": "Histórico com nomes específicos",
            "TESTE_2_1": "Sequência de lançamentos",
            "TESTE_3_1": "Valor redondo",
            "TESTE_4_1": "Sequência por histórico/movimento/data",
            "TESTE_6_1": "Movimento alto com receitas",
            "TESTE_7_1": "Classe conta resultado",
            "TESTE_7_2": "Débito imobilizado com receitas",
            "TESTE_7_3": "Teste sempre nulo",
            "TESTE_7_4": "Débito/Crédito em contas específicas",
            "TESTE_7_5": "Crédito receitas sem contas a receber",
            "TESTE_7_6": "Variação cambial",
            "TESTE_7_7": "Movimento negativo em caixa",
            "TESTE_7_9": "Pessoas politicamente expostas"
        }
        
        for teste, ativo in TESTES_CONFIG.items():
            categoria = "HISTORICO" if teste.startswith("TESTE_1") else "CLASSIFICACAO"
            status = "ATIVO" if ativo else "INATIVO"
            descricao = descricoes.get(teste, "Descrição não disponível")
            linhas.append(f"{teste},{status},{categoria},{descricao}")
        
        # Salvar CSV
        with open(csv_config_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(linhas))
        
        logger.info(f"📊 CSV de configuração gerado: {csv_config_file}")
        return csv_config_file
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar CSV de configuração: {str(e)}")
        return None

def gerar_relatorio_python_config(logger):
    """Gera um arquivo Python com toda a configuração atual"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        py_config_file = os.path.join(SAIDA_DIR, "logs", f"configuracao_backup_{timestamp}.py")
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(py_config_file), exist_ok=True)
        
        # Construir arquivo Python
        conteudo = f'''#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup da Configuração de Auditoria Contábil
Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
"""

# ========================================
# CONFIGURAÇÃO DOS TESTES
# ========================================

TESTES_CONFIG = {{
'''
        
        for teste, ativo in TESTES_CONFIG.items():
            conteudo += f"    '{teste}': {ativo},\n"
        
        conteudo += f'''}}

# ========================================
# PALAVRAS-CHAVE DOS TESTES
# ========================================

PALAVRAS_TESTE_1_4 = {PALAVRAS_TESTE_1_4}

PALAVRAS_TESTE_1_5 = {PALAVRAS_TESTE_1_5}

PALAVRAS_TESTE_1_6 = {PALAVRAS_TESTE_1_6}

PALAVRAS_TESTE_1_7 = {PALAVRAS_TESTE_1_7}

PALAVRAS_TESTE_7_6 = {PALAVRAS_TESTE_7_6}

NOMES_TESTE_1_8 = {NOMES_TESTE_1_8}

NOMES_TESTE_7_9 = {NOMES_TESTE_7_9}

# ========================================
# GRUPOS DE CLASSIFICAÇÃO
# ========================================

CLASS_RESULTADO_TESTE_2_1 = {CLASS_RESULTADO_TESTE_2_1}

GRUPOS_RECEITA_TESTE_6_1 = {GRUPOS_RECEITA_TESTE_6_1}

CLASS_CONTA_TESTE_7_1 = {CLASS_CONTA_TESTE_7_1}

GRUPOS_RECEITA_TESTE_7_2 = {GRUPOS_RECEITA_TESTE_7_2}

GRUPO_IMOBILIZADO_TESTE_7_2 = {GRUPO_IMOBILIZADO_TESTE_7_2}

# ========================================
# FERIADOS
# ========================================

FERIADOS = {FERIADOS}

# ========================================
# CONFIGURAÇÕES TÉCNICAS
# ========================================

CSV_SEPARATOR = "{CSV_SEPARATOR}"

COLUNAS_PARA_REMOVER = {COLUNAS_PARA_REMOVER}

# Para restaurar esta configuração, copie as variáveis acima 
# para o script principal
'''

        # Salvar arquivo Python
        with open(py_config_file, 'w', encoding='utf-8') as f:
            f.write(conteudo)
        
        logger.info(f"🐍 Backup Python gerado: {py_config_file}")
        return py_config_file
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar backup Python: {str(e)}")
        return None

# ========================================
# CONFIGURAÇÃO AUTOMÁTICA DO AMBIENTE
# ========================================

# Detectar e configurar automaticamente o Hadoop no Windows
import os

# Configuração do Hadoop - ATUALIZADO para 3.3.6
os.environ["HADOOP_HOME"] = r"C:\hadoop\hadoop-3.3.6"
os.environ["HADOOP_CONF_DIR"] = r"C:\hadoop\hadoop-3.3.6\etc\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\hadoop-3.3.6\bin"

# Configuração do Python para PySpark
os.environ["PYSPARK_PYTHON"] = r"C:\ProgramData\anaconda3\envs\spark_ambiente_rsm_admin_2\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\ProgramData\anaconda3\envs\spark_ambiente_rsm_admin_2\python.exe"


# ========================================
# CONFIGURAÇÃO PRINCIPAL        
# ========================================

# Caminhos dos arquivos
ARQ_RAZAO = r"C:\AZURE_DEVOS_REPOS\RSM_DATALAKE_JOURNAL_ENTRIES\034-DTLK-Data analytics_GRAHAM_31\silver\razao 11e 12.csv"
ARQ_FDS = r"C:\welligton-projects-datalake-auditoria\dtlk-journal-entries\004-DLTK-Data analytics_CASC_31122025\silver\TB_LISTA_FDS.csv"
SAIDA_DIR = r"C:\AZURE_DEVOS_REPOS\RSM_DATALAKE_JOURNAL_ENTRIES\034-DTLK-Data analytics_GRAHAM_31\gold"

# Separador do CSV
CSV_SEPARATOR = ";"

# Feriados para verificação
 

# Feriados para verificação
 

# ==========================================================
# 🔹 FERIADOS 2025
# ==========================================================

# 🔹 Dataset oficial de feriados
FERIADOS = [ 
"01/01/2025",
"20/01/2025",
"13/02/2025",
"29/03/2025",
"21/04/2025",
"23/04/2025",
"01/05/2025",
"30/05/2025",
"07/09/2025",
"12/10/2025",
"02/11/2025",
"15/11/2025",
"20/11/2025",
"25/12/2025"
]

# 🔹 Dataset oficial de finais de semana (pré-calculado igual Spark)
FINAIS_DE_SEMANA = [
    "04/01/2025","05/01/2025",
    "11/01/2025","12/01/2025",
    "18/01/2025","19/01/2025",
    "25/01/2025","26/01/2025",
    "01/02/2025","02/02/2025",
    "08/02/2025","09/02/2025",
    "15/02/2025","16/02/2025",
    "22/02/2025","23/02/2025",
    "01/03/2025","02/03/2025",
    "08/03/2025","09/03/2025",
    "15/03/2025","16/03/2025",
    "22/03/2025","23/03/2025",
    "29/03/2025","30/03/2025",
    "05/04/2025","06/04/2025",
    "12/04/2025","13/04/2025",
    "19/04/2025","20/04/2025",
    "26/04/2025","27/04/2025",
    "03/05/2025","04/05/2025",
    "10/05/2025","11/05/2025",
    "17/05/2025","18/05/2025",
    "24/05/2025","25/05/2025",
    "31/05/2025","01/06/2025",
    "07/06/2025","08/06/2025",
    "14/06/2025","15/06/2025",
    "21/06/2025","22/06/2025",
    "28/06/2025","29/06/2025",
    "05/07/2025","06/07/2025",
    "12/07/2025","13/07/2025",
    "19/07/2025","20/07/2025",
    "26/07/2025","27/07/2025",
    "02/08/2025","03/08/2025",
    "09/08/2025","10/08/2025",
    "16/08/2025","17/08/2025",
    "23/08/2025","24/08/2025",
    "30/08/2025","31/08/2025",
    "06/09/2025","13/09/2025","14/09/2025",
    "20/09/2025","21/09/2025",
    "27/09/2025","28/09/2025",
    "04/10/2025","05/10/2025",
    "11/10/2025","18/10/2025","19/10/2025",
    "25/10/2025","26/10/2025",
    "01/11/2025","08/11/2025","09/11/2025",
    "16/11/2025",
    "22/11/2025","23/11/2025",
    "29/11/2025","30/11/2025",
    "06/12/2025","07/12/2025",
    "13/12/2025","14/12/2025",
    "20/12/2025","21/12/2025",
    "27/12/2025","28/12/2025",
]





def log_carregamento_dados(logger, df_razao, df_fds, tempo_carregamento):
    """Registra informações sobre o carregamento dos dados"""
    logger.info("📂 CARREGAMENTO DE DADOS:")
    logger.info(f"   ⏱️ Tempo de carregamento: {tempo_carregamento:.2f} segundos")

    if df_razao is not None:
        count_razao = df_razao.count()
        logger.info("=" * 60)
        logger.info("📋 TABELA: TB_RAZAO")
        logger.info(f"   📊 Linhas    : {count_razao:,}")
        logger.info(f"   📐 Colunas   : {len(df_razao.columns)}")
        logger.info("   🗂️  Lista de colunas:")
        for i, col_name in enumerate(df_razao.columns, 1):
            dtype = dict(df_razao.dtypes)[col_name]
            logger.info(f"      {i:02d}. {col_name:<35} [{dtype}]")
        logger.info("=" * 60)

    if df_fds is not None and df_fds.count() > 0:
        count_fds = df_fds.count()
        logger.info("📋 TABELA: TB_FDS")
        logger.info(f"   📊 Linhas    : {count_fds:,}")
        logger.info(f"   📐 Colunas   : {len(df_fds.columns)}")
        logger.info("   🗂️  Lista de colunas:")
        for i, col_name in enumerate(df_fds.columns, 1):
            dtype = dict(df_fds.dtypes)[col_name]
            logger.info(f"      {i:02d}. {col_name:<35} [{dtype}]")
        logger.info("=" * 60)
    else:
        logger.info("   ⚠️ TB_FDS: Usando apenas lista de feriados predefinidos")

def log_dominios_colunas(df, logger):
    """
    Loga os valores únicos das colunas de domínio
    para validação de normalização e consistência.
    """
    logger.info("=" * 70)
    logger.info("🔎 VALIDAÇÃO DE DOMÍNIOS DAS COLUNAS TEXTUAIS")
    logger.info("=" * 70)

    colunas_dominios = [
        "GRUPO_CONTA",
        "GRUPO_CTP",
        "CLASS_CONTA",
        "CLASS_CTP"
    ]

    for coluna in colunas_dominios:
        if coluna in df.columns:
            logger.info(f"\n==================== {coluna} ====================")

            valores = (
                df.select(coluna)
                  .distinct()
                  .orderBy(coluna)
                  .limit(200)   # evita explosão se tiver sujeira
                  .collect()
            )

            for row in valores:
                valor = row[coluna]
                logger.info(f"   '{valor}'")

        else:
            logger.warning(f"⚠️ Coluna '{coluna}' não encontrada no DataFrame")

    logger.info("=" * 70)

# ========================================
# Detectar o encoding automaticamente
# ========================================
from charset_normalizer import from_path

def detectar_encoding(caminho_arquivo: str, fallback: str = "utf-8"):
    """
    Retorna o encoding mais provável do arquivo CSV.
    Tenta identificar BOM, senão usa charset-normalizer.
    """
    # 1) BOM rápido
    with open(caminho_arquivo, "rb") as f:
        inicio = f.read(4)
    if inicio.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"  # UTF-8 com BOM (excel-friendly)
    if inicio.startswith(b"\xff\xfe"):
        return "utf-16"     # LE BOM
    if inicio.startswith(b"\xfe\xff"):
        return "utf-16-be"  # BE BOM

    # 2) Heurística por análise
    res = from_path(caminho_arquivo).best()
    if res and res.encoding:
        return res.encoding.lower()

    # 3) Fallback seguro
    return fallback

# ========================================
# Helper: ler CSV no Spark com encoding detectado
# ========================================
def ler_csv_spark(spark, caminho, sep=";", infer_schema=True, logger=None):
    enc = detectar_encoding(caminho)
    if logger:
        logger.info(f"📥 Encoding detectado para '{caminho}': {enc}")
    df = spark.read \
        .option("header", True) \
        .option("sep", sep) \
        .option("inferSchema", infer_schema) \
        .option("encoding", enc) \
        .option("multiline", "true") \
        .csv(caminho)
    return df


# ========================================
# CONSTANTES PARA TESTES DE HISTÓRICO
# ========================================
 # ==========================================================
# 🔹 LISTAS TEXTUAIS (evita NameError)
# ==========================================================

PALAVRAS_TESTE_1_4 = [
    'segundo', 'acordo', 'ordem', 'mail', 
    'conforme', 'controller', 'pedido'
]

# Palavras-chave para TESTE_1_5 (ajuste/acerto)
PALAVRAS_TESTE_1_5 = [
    'ajuste', 'acerto'
]

# Palavras-chave para TESTE_1_6 (reversão/estorno)
PALAVRAS_TESTE_1_6 = [
    'reversao', 'reversão', 'estorno'
]

# Palavras-chave para TESTE_1_7 (outros/diversas)
PALAVRAS_TESTE_1_7 = [
    'outros', 'outras', 'desconhecidos', 'diversos'
]

# Palavras-chave para TESTE_7_6 (variação cambial)
PALAVRAS_TESTE_7_6 = [
    'variacao cambial', 'variação cambial', 'cambial'
]
 


NOMES_TESTE_1_8 = [
"GABRIELA POIATTO TEIXEIRA",
"GABRIELA",
"POIATTO",
"TEIXEIRA",

"TATIANE SILVA MATHEW",
"TATIANE",
"SILVA",
"MATHEW",

"DENNY JOSEPH",
"DENNY",
"JOSEPH",

"WANDERLEI ANTONIO",
"WANDERLEI",
"ANTONIO",

"TIAGO ALEXANDRE",
"TIAGO",
"ALEXANDRE"
]

NOMES_TESTE_7_9 = ['']

# ==========================================================
# TESTE_2_1 >> CLASS_CTP
# ==========================================================
CLASS_RESULTADO_TESTE_2_1 = ['Resultado']

# ==========================================================
# TESTE_6_1 >> GRUPO_CONTA
# ==========================================================

GRUPOS_RECEITA_TESTE_6_1 = [
    'Receita líquida'
    'Receitas financeiras'
]

# ==========================================================
# TESTE_7_1 >> CLASS_CONTA
# ==========================================================

CLASS_CONTA_TESTE_7_1 = ['Resultado']


# ==========================================================
# TESTE_7_2 >> GRUPO_CTP >>> GRUPOS_RECEITA_TESTE_7_2
# TESTE_7_2 >> GRUPO_CONTA >>> GRUPO_IMOBILIZADO_TESTE_7_2
# ==========================================================

GRUPOS_RECEITA_TESTE_7_2 = [
    'Outras receitas',
    'Receitas financeiras',
    'Receita bruta'
]

GRUPO_IMOBILIZADO_TESTE_7_2 = ['Imobilizado']

# ==========================================================
# TESTE_7_4 GRUPO_CONTA >>> GRUPO_IMOBILIZADO_TESTE_7_4
# TESTE_7_4 GRUPO_CONTA >>> GRUPOS_EXCLUIR_TESTE_7_4
# ==========================================================

GRUPO_IMOBILIZADO_TESTE_7_4 = ['Imobilizado']

GRUPOS_EXCLUIR_TESTE_7_4 = [
    'Caixa e equivalentes de caixa',  
    'Fornecedores'
]

# ==========================================================
# TESTE_7_5  GRUPO_CONTA >>> GRUPOS_RECEITA_TESTE_7_5
# TESTE_7_5  GRUPO_CTP >>> GRUPOS_CONTAS_RECEBER_TESTE_7_5
# ==========================================================

GRUPOS_RECEITA_TESTE_7_5 = [
    'receita líquida',
    'receitas financeiras'
]

GRUPOS_CONTAS_RECEBER_TESTE_7_5 = ['contas a receber']

# ==========================================================
# TESTE_7_6  GRUPO_CONTA >>> GRUPOS_EXCLUIR_TESTE_7_6
# ==========================================================

GRUPOS_EXCLUIR_TESTE_7_6 = [
    'Emprestimos',
    'Partes relacionadas'
]

# ==========================================================
# TESTE_7_7 GRUPO_CONTA >>> GRUPO_CAIXA_TESTE_7_7
# TESTE_7_7 CLASS_CTP >>> CLASS_RESULTADO_TESTE_7_7
# ==========================================================
 

GRUPO_CAIXA_TESTE_7_7 = ['Caixa e equivalentes de caixa']

CLASS_RESULTADO_TESTE_7_7 = ['Resultado']


# ========================================
# CONFIGURAÇÃO UNIFICADA DOS TESTES
# ========================================


TESTES_CONFIG = {
    'TESTE_1_1': False,
    'TESTE_1_2': False,
    'TESTE_1_3': True,
    'TESTE_1_4': True,
    'TESTE_1_5': True,
    'TESTE_1_6': False,
    'TESTE_1_7': True,
    'TESTE_1_8': True,
    'TESTE_2_1': False,
    'TESTE_3_1': False,
    'TESTE_4_1': False,
    'TESTE_5_1': False,
    'TESTE_6_1': False,
    'TESTE_7_1': False,
    'TESTE_7_2': False,
    'TESTE_7_3': False,
    'TESTE_7_4': False,
    'TESTE_7_5': False,
    'TESTE_7_6': False,
    'TESTE_7_7': False,
    'TESTE_7_9': False
}

# Colunas que serão removidas antes da exportação
COLUNAS_PARA_REMOVER = [
    "FLAG",
    "LINHA_SEQ", 
    "LINHA_SEQ_TESTE4", 
    "Semana_Resumida", 
    "VALOR_REDONDO"
]

# ========================================
# FUNÇÕES UTILITÁRIAS
# ========================================

def criar_spark_session():
    """Cria sessão Spark com configuração OTIMIZADA"""
    try:
        print(f"🚀 Configurando Spark OTIMIZADO com {NUMERO_THREADS} threads")
        print(f"💾 Memória Driver: {SPARK_DRIVER_MEMORY} | Executor: {SPARK_EXECUTOR_MEMORY}")
        
        spark = SparkSession.builder \
            .appName("AuditoriaContabil_Otimizada") \
            .master(f"local[{NUMERO_THREADS}]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", str(NUMERO_THREADS * 4)) \
            .config("spark.default.parallelism", str(NUMERO_THREADS * 4)) \
            .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
            .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
            .config("spark.driver.maxResultSize", SPARK_MAX_RESULT_SIZE) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")\
            .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
            .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.autoBroadcastJoinThreshold", "20971520") \
            .config("spark.network.timeout", "800s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.storage.memoryFraction", "0.6") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "2g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        print(f"✅ Spark {spark.version} iniciado com sucesso")
        return spark
        
    except Exception as e:
        print(f"❌ Erro ao criar Spark: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None  
    
def limpar_nomes_colunas(df):
    """Limpa e padroniza nomes das colunas - VERSÃO CORRIGIDA PARA BOM"""
    colunas_limpas = []
    for col in df.columns:
        nome_limpo = (col.strip()
                     # REMOVER BOM (Byte Order Mark) e caracteres problemáticos
                     .replace("ï»¿", "")
                     .replace('"', "")
                     .replace("'", "")
                     # Substituir espaços e caracteres especiais
                     .replace(" ", "_")
                     .replace("-", "_")
                     .replace(".", "_")
                     # Corrigir encoding problemático
                     .replace("Ã¡", "a").replace("Ã©", "e").replace("Ã­", "i")
                     .replace("Ã³", "o").replace("Ãº", "u").replace("Ã§", "c")
                     .replace("Ã", "a").replace("Ã£o", "ao").replace("Ã§Ã£o", "cao")
                     .replace("LanÃ§amento", "Lancamento")
                     .replace("DescriÃ§Ã£o", "Descricao")
                     .replace("Contra partida", "Contra_partida")
                     .replace("Saldo RSM", "Saldo_RSM")
                     # Limpeza de acentos normais
                     .replace("á", "a").replace("é", "e").replace("í", "i")
                     .replace("ó", "o").replace("ú", "u").replace("ç", "c")
                     .replace("Á", "A").replace("É", "E").replace("Í", "I")
                     .replace("Ó", "O").replace("Ú", "U").replace("Ç", "C")
                     .replace("ã", "a").replace("õ", "o").replace("Ã", "A"))
        colunas_limpas.append(nome_limpo)
    
    return df.toDF(*colunas_limpas)

def verificar_colunas_necessarias(df, colunas_necessarias):
    """Verifica se as colunas necessárias existem no DataFrame"""
    colunas_faltantes = [col_name for col_name in colunas_necessarias if col_name not in df.columns]
    
    if colunas_faltantes:
        return False
    return True

def criar_condicao_like_palavras(coluna, palavras):
    """Cria condição LIKE para múltiplas palavras em uma coluna"""
    if not palavras:
        return lit(False)
    
    condicoes = []
    for palavra in palavras:
        palavra_lower = palavra.lower()
        condicoes.extend([
            lower(coluna).like(f'{palavra_lower}%'),
            lower(coluna).like(f'%{palavra_lower}%'),
            lower(coluna).like(f'%{palavra_lower}')
        ])
    
    # Combina todas as condições com OR
    condicao_final = condicoes[0]
    for cond in condicoes[1:]:
        condicao_final = condicao_final | cond
    
    return condicao_final

# ========================================
# CARREGAMENTO COMPLETO DOS DADOS
# ========================================

def carregar_dados(spark, logger):
    """Carrega todos os arquivos necessários (método principal)"""
    logger.info("📂 Iniciando carregamento completo dos dados...")
    inicio_carregamento = time.time()
    
    # Carregar TB_RAZAO
    df_razao = carregar_dados_razao(spark, logger)
    if df_razao is None:
        logger.error("❌ Falha crítica no carregamento de TB_RAZAO")
        return None, None
    
    # Carregar TB_FDS
    df_fds = carregar_dados_fds(spark, logger)
    
    # Log do carregamento completo
    tempo_carregamento = time.time() - inicio_carregamento
    log_carregamento_dados(logger, df_razao, df_fds, tempo_carregamento)
    
    logger.info("✅ Carregamento completo concluído com sucesso")
    return df_razao, df_fds

# ========================================
# CARREGAMENTO DOS DADOS TB_RAZAO
# ========================================

def carregar_dados_razao(spark, logger):
    """Carrega o arquivo TB_RAZAO com tratamento de erros e logging"""
    global ORDEM_COLUNAS_ORIGINAL  # ⭐ IMPORTANTE
    
    logger.info("📂 Iniciando carregamento TB_RAZAO...")
    
    if not os.path.exists(ARQ_RAZAO):
        logger.error(f"❌ Arquivo TB_RAZAO não encontrado: {ARQ_RAZAO}")
        return None
    
    try:
        df_razao = None
        encodings_to_try = ["UTF-8", "windows-1252", "ISO-8859-1", "latin1"]
        
        logger.info("🔄 Carregando TB_RAZAO...")
        for encoding in encodings_to_try:
            try:
                logger.info(f"   🔄 Tentando encoding: {encoding}")
                df_razao = spark.read \
                    .option("header", True) \
                    .option("sep", CSV_SEPARATOR) \
                    .option("inferSchema", True) \
                    .option("encoding", encoding) \
                    .option("multiline", "true") \
                    .csv(ARQ_RAZAO)
                
                # ⭐ CAPTURAR ORDEM ORIGINAL IMEDIATAMENTE (antes de limpar nomes)
                if ORDEM_COLUNAS_ORIGINAL is None:
                    ORDEM_COLUNAS_ORIGINAL = df_razao.columns.copy()
                    logger.info(f"🧷 Ordem original capturada: {len(ORDEM_COLUNAS_ORIGINAL)} colunas")
                
                # Limpar nomes mantendo a ordem
                df_razao = limpar_nomes_colunas(df_razao)
                
                # ⭐ ATUALIZAR ORDEM COM NOMES LIMPOS (mesmo número de colunas)
                ORDEM_COLUNAS_ORIGINAL = df_razao.columns.copy()
                logger.info(f"✅ Nomes limpos aplicados - primeira coluna: '{ORDEM_COLUNAS_ORIGINAL[0]}'")
                
                count_razao = df_razao.count()
                
                if count_razao > 0:
                    logger.info(f"   ✅ Sucesso com encoding {encoding} - {count_razao} registros")
                    break
                else:
                    logger.warning(f"   ⚠️ Arquivo carregado mas vazio com encoding {encoding}")
                    df_razao = None
                
            except Exception as e:
                logger.warning(f"   ⚠️ Falhou com encoding {encoding}: {str(e)[:200]}")
                df_razao = None
                continue
        
        # Se ainda não conseguiu carregar, tentar sem encoding
        if df_razao is None:
            logger.info("   🔄 Tentativa final sem especificar encoding")
            try:
                df_razao = spark.read \
                    .option("header", True) \
                    .option("sep", CSV_SEPARATOR) \
                    .option("inferSchema", True) \
                    .option("multiline", "true") \
                    .csv(ARQ_RAZAO)
                
                # ⭐ CAPTURAR ORDEM
                if ORDEM_COLUNAS_ORIGINAL is None:
                    ORDEM_COLUNAS_ORIGINAL = df_razao.columns.copy()
                
                df_razao = limpar_nomes_colunas(df_razao)
                ORDEM_COLUNAS_ORIGINAL = df_razao.columns.copy()
                
                count_razao = df_razao.count()
                
                if count_razao > 0:
                    logger.info(f"   ✅ Carregado sem encoding específico - {count_razao} registros")
                else:
                    logger.error("   ❌ Arquivo carregado mas está vazio")
                    return None
                    
            except Exception as e:
                logger.error(f"   ❌ Falha final ao carregar TB_RAZAO: {str(e)}")
                return None
        
        if df_razao is None:
            logger.error("❌ Não foi possível carregar TB_RAZAO com nenhum método")
            return None
        
        # Verificar colunas essenciais
        colunas_essenciais = ["CONTA"]
        colunas_faltando = [col for col in colunas_essenciais if col not in df_razao.columns]
        
        if colunas_faltando:
            logger.error(f"❌ Colunas essenciais não encontradas: {colunas_faltando}")
            return None
        
        logger.info("✅ TB_RAZAO carregado com sucesso")
        return df_razao
        
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao carregar TB_RAZAO: {str(e)}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return None

# ========================================
# CARREGAMENTO DOS DADOS TB_FDS
# ========================================

def carregar_dados_fds(spark, logger):
    """Carrega o arquivo TB_FDS com tratamento de erros e logging"""
    logger.info("📂 Iniciando carregamento TB_FDS...")
    
    # Verificar se o arquivo TB_FDS existe
    if not os.path.exists(ARQ_FDS):
        logger.info("📄 TB_FDS não encontrada - criando DataFrame vazio")
        # Criar DataFrame vazio com schema mínimo
        schema_fds = StructType([StructField("DATA", StringType(), True)])
        return spark.createDataFrame([], schema=schema_fds)
    
    try:
        # Carregar TB_FDS - MÚLTIPLAS TENTATIVAS DE ENCODING
        df_fds = None
        encodings_to_try = ["UTF-8", "windows-1252", "ISO-8859-1", "latin1"]
        
        logger.info("🔄 Carregando TB_FDS...")
        for encoding in encodings_to_try:
            try:
                logger.info(f"   🔄 Tentando encoding: {encoding}")
                df_fds = spark.read \
                    .option("header", True) \
                    .option("sep", CSV_SEPARATOR) \
                    .option("inferSchema", True) \
                    .option("encoding", encoding) \
                    .option("multiline", "true") \
                    .csv(ARQ_FDS)
                
                df_fds = limpar_nomes_colunas(df_fds)
                count_fds = df_fds.count()
                logger.info(f"   ✅ Sucesso com encoding {encoding} - {count_fds} registros")
                break
                
            except Exception as e:
                logger.warning(f"   ⚠️ Falhou com encoding {encoding}: {str(e)[:100]}")
                df_fds = None
                continue
        
        # Se ainda não conseguiu carregar, tentar sem especificar encoding
        if df_fds is None:
            logger.info("   🔄 Tentativa final sem especificar encoding")
            try:
                df_fds = spark.read \
                    .option("header", True) \
                    .option("sep", CSV_SEPARATOR) \
                    .option("inferSchema", True) \
                    .option("multiline", "true") \
                    .csv(ARQ_FDS)
                df_fds = limpar_nomes_colunas(df_fds)
                count_fds = df_fds.count()
                logger.info(f"   ✅ FDS carregado sem encoding específico - {count_fds} registros")
            except Exception as e:
                logger.error(f"   ❌ Erro ao carregar TB_FDS: {e}")
                # Criar DataFrame vazio com schema mínimo
                schema_fds = StructType([StructField("DATA", StringType(), True)])
                df_fds = spark.createDataFrame([], schema=schema_fds)
        
        logger.info("✅ TB_FDS carregado com sucesso")
        return df_fds
        
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao carregar TB_FDS: {str(e)}")
        logger.error(f"❌ Tipo do erro: {type(e).__name__}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        # Retornar DataFrame vazio em caso de erro
        schema_fds = StructType([StructField("DATA", StringType(), True)])
        return spark.createDataFrame([], schema=schema_fds)
    
def converter_colunas_monetarias(df, cols, logger=None):
    for c in cols:
        if c in df.columns:
            # se já for numérico, ok; se for string BR, normaliza
            df = df.withColumn(c, regexp_replace(col(c).cast("string"), r"\.", ""))
            df = df.withColumn(c, regexp_replace(col(c), ",", "."))
            df = df.withColumn(c, col(c).cast("double"))
            if logger:
                logger.info(f"   ✅ Coluna {c} normalizada para double")
    return df

def converter_movimento_para_numerico(df, logger):
    """Converte coluna MOVIMENTO de formato brasileiro para numérico"""
    from pyspark.sql.functions import regexp_replace, col
    
    logger.info("🔢 Convertendo MOVIMENTO para formato numérico...")
    
    if "MOVIMENTO" in df.columns:
        # Verifica o tipo atual
        tipo_atual = dict(df.dtypes)["MOVIMENTO"]
        logger.info(f"   📋 Tipo atual de MOVIMENTO: {tipo_atual}")
        
        # Se for string, converter
        if tipo_atual == "string":
            logger.info("   🔄 MOVIMENTO é string - convertendo para numérico...")
            
            # Remove pontos de milhar e troca vírgula por ponto
            df = df.withColumn("MOVIMENTO",
                regexp_replace(col("MOVIMENTO"), r"\.", "")  # Remove pontos
            )
            df = df.withColumn("MOVIMENTO",
                regexp_replace(col("MOVIMENTO"), ",", ".")  # Vírgula → ponto
            )
            
            # Converte para double
            df = df.withColumn("MOVIMENTO", col("MOVIMENTO").cast("double"))
            
            logger.info("   ✅ MOVIMENTO convertido para double")
        else:
            logger.info("   ✅ MOVIMENTO já é numérico")
    
    return df

def remover_acentos(texto):
    if texto is None:
        return ""
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
 
def aplicar_testes_auditoria(df_razao, df_fds, feriados, logger):
    """Aplica todos os testes de auditoria de forma unificada com logging"""
    logger.info("🧪 Iniciando aplicação dos testes de auditoria...")
    inicio_processamento = time.time()

    window_seq = Window.partitionBy("CONTA", "MOVIMENTO") \
                    .orderBy(col("CONTA").asc())

    window_seq_teste4 = Window.partitionBy("HISTORICO", "MOVIMENTO", "DATA") \
                            .orderBy(col("HISTORICO").asc())

    df = df_razao \
        .withColumn("LINHA_SEQ", row_number().over(window_seq)) \
        .withColumn("LINHA_SEQ_TESTE4", row_number().over(window_seq_teste4))

    # Aplicar testes condicionalmente
    for teste, ativo in TESTES_CONFIG.items():
        if ativo:
            logger.info(f"   🔄 Aplicando {teste}...")
            
            if teste == "TESTE_1_1":
                df = df.withColumn(teste, when(col("DATA").isNull(), "SIM"))
                        
            elif teste == "TESTE_1_2":

                try:
                    logger.info("   📅 Aplicando TESTE_1_2 - Feriados + Finais de Semana")

                    from datetime import datetime
                    from pyspark.sql.functions import coalesce, to_date

                    # 🔹 Converter DATA tentando dois formatos
                    df = df.withColumn(
                        "DATA_DATE",
                        coalesce(
                            to_date(col("DATA"), "dd/MM/yyyy"),
                            to_date(col("DATA"), "yyyy-MM-dd")
                        )
                    )

                    # 🔹 Unir todas datas bloqueadas
                    todas_datas = FERIADOS + FINAIS_DE_SEMANA

                    # 🔹 Converter lista para formato ISO (yyyy-MM-dd)
                    datas_formatadas = [
                        datetime.strptime(f, "%d/%m/%Y").strftime("%Y-%m-%d")
                        for f in todas_datas
                    ]

                    # 🔹 Aplicar teste comparando como string (mais seguro)
                    df = df.withColumn(
                        teste,
                        when(
                            date_format(col("DATA_DATE"), "yyyy-MM-dd").isin(datas_formatadas),
                            "SIM"
                        )
                    )

                    total_sim = df.filter(col(teste) == "SIM").count()
                    logger.info(f"   🎯 Detectados: {total_sim}")

                    df = df.drop("DATA_DATE")

                except Exception as e:
                    logger.error(f"Erro TESTE_1_2: {e}")
                    df = df.withColumn(teste, lit(None))


   
            elif teste == "TESTE_1_3":
                df = df.withColumn(teste, when(col("HISTORICO").isNull(), "SIM"))
            
            elif teste == "TESTE_1_4":
                df = df.withColumn(teste, 
                    when(criar_condicao_like_palavras(col("HISTORICO"), PALAVRAS_TESTE_1_4), "SIM"))
            
            elif teste == "TESTE_1_5":
                df = df.withColumn(teste, 
                    when(criar_condicao_like_palavras(col("HISTORICO"), PALAVRAS_TESTE_1_5), "SIM"))
            
            elif teste == "TESTE_1_6":
                df = df.withColumn(teste, 
                    when(criar_condicao_like_palavras(col("HISTORICO"), PALAVRAS_TESTE_1_6), "SIM"))
            
            elif teste == "TESTE_1_7":
                df = df.withColumn(teste, 
                    when(criar_condicao_like_palavras(col("HISTORICO"), PALAVRAS_TESTE_1_7), "SIM"))
            
            elif teste == "TESTE_1_8":

                if NOMES_TESTE_1_8:

                    logger.info(f"   👤 Detectando nomes específicos: {len(NOMES_TESTE_1_8)} nomes")

                    

                    # 🔹 Normalizar HISTORICO uma única vez
                    df = df.withColumn(
                        "HISTORICO_NORM",
                        lower(col("HISTORICO"))
                    )

                    regex_lista = []

                    for nome in NOMES_TESTE_1_8:
                        nome_norm = unidecode(nome.strip().lower())
                        palavras = nome_norm.split()

                        # 🔹 Nome curto (até 5 caracteres e uma palavra)
                        if len(palavras) == 1 and len(palavras[0]) <= 5:
                            palavra = re.escape(palavras[0])
                            logger.info(f"   🔍 Palavra isolada com fronteira: '{palavra}'")
                            regex_lista.append(rf"\b{palavra}\b")

                        # 🔹 Nome composto ou maior
                        else:
                            nome_regex = re.escape(nome_norm)
                            logger.info(f"   🔍 Nome completo: '{nome_norm}'")
                            regex_lista.append(nome_regex)

                    # 🔹 Criar regex única
                    regex_final = "|".join(regex_lista)

                    condicao_final = col("HISTORICO_NORM").rlike(regex_final)

                    df = df.withColumn(
                        teste,
                        when(condicao_final, lit("SIM")).otherwise(lit(None))
                    )

                    df = df.drop("HISTORICO_NORM")

                    count_detectado = df.filter(col(teste) == "SIM").count()
                    logger.info(f"   🎯 {count_detectado:,} registros detectados")

                    if 0 < count_detectado <= 20:
                        logger.info("   📋 Exemplos detectados:")
                        exemplos = (
                            df.filter(col(teste) == "SIM")
                            .select("HISTORICO")
                            .limit(10)
                            .collect()
                        )
                        for i, row in enumerate(exemplos, 1):
                            logger.info(f"      {i}. {row['HISTORICO'][:100]}")

                else:
                    df = df.withColumn(teste, lit(None))


            # ⭐ NOVO TESTE_7_9 - Pessoas Politicamente Expostas
            elif teste == "TESTE_7_9":
                if NOMES_TESTE_7_9:
                    logger.info(f"   👥 Detectando pessoas politicamente expostas: {len(NOMES_TESTE_7_9)} nomes")
                    condicao_nomes = lit(False)
                    
                    for nome in NOMES_TESTE_7_9:
                        condicao_nomes = condicao_nomes | lower(col("HISTORICO")).like(f"%{nome.lower()}%")
                    
                    df = df.withColumn(teste, when(condicao_nomes, "SIM"))
                    
                    count_detectado = df.filter(col(teste) == "SIM").count()
                    logger.info(f"   🎯 {count_detectado:,} registros detectados com pessoas expostas")
                else:
                    df = df.withColumn(teste, lit(None))
            
            elif teste == "TESTE_2_1":
                if verificar_colunas_necessarias(df, ["CLASS_CTP"]):
                    df = df.withColumn(teste, 
                        when((col("LINHA_SEQ") > 1) & (col("CLASS_CTP").isin(CLASS_RESULTADO_TESTE_2_1)), "SIM"))
                else:
                    logger.warning(f"⚠️ {teste}: Colunas necessárias não encontradas - teste desabilitado")
                    df = df.withColumn(teste, lit(None))
            
            elif teste == "TESTE_3_1":
                if verificar_colunas_necessarias(df, ["MOVIMENTO"]):

                    logger.info("   💰 Aplicando TESTE_3_1 - Valores redondos múltiplos de 10")

                    df = df.withColumn(
                        "MOV_DEC",
                        col("MOVIMENTO").cast(DecimalType(18, 2))
                    )

                    valor_abs = spark_abs(col("MOV_DEC"))

                    df = df.withColumn(
                        teste,
                        when(
                            (valor_abs.isNotNull()) &
                            (valor_abs >= 10) &                 # mínimo 10
                            (pmod(valor_abs, 1) == 0) &          # sem centavos
                            (pmod(valor_abs, 10) == 0),          # 🔥 termina em zero
                            lit("SIM")
                        ).otherwise(lit(None))
                    )

                    count_detectado = df.filter(col(teste) == "SIM").count()
                    logger.info(f"   🎯 TESTE_3_1: {count_detectado:,} valores redondos detectados")

                    df = df.drop("MOV_DEC")

                else:
                    logger.warning(f"⚠️ {teste}: Coluna 'MOVIMENTO' não encontrada")
                    df = df.withColumn(teste, lit(None))






            
            elif teste == "TESTE_4_1":
                df = df.withColumn(teste, 
                    when(col("LINHA_SEQ_TESTE4") >= 3, "SIM"))
            elif teste == "TESTE_5_1":
                logger.info("   🎲 Aplicando TESTE_5_1 - Amostra aleatória de 25 lançamentos")

                # Criar ID temporário
                df = df.withColumn("__ROW_ID_T51", monotonically_increasing_id())

                seed = 42

                # Criar amostra
                amostra_ids = (
                    df.select("__ROW_ID_T51")
                      .orderBy(rand(seed))
                      .limit(25)
                      .withColumn("T51_FLAG", lit(1))
                )

                # Join para marcar sorteados
                df = df.join(amostra_ids, on="__ROW_ID_T51", how="left")

                df = df.withColumn(
                    teste,
                    when(col("T51_FLAG") == 1, lit("SIM")).otherwise(lit(None))
                )

                qtd = df.filter(col(teste) == "SIM").count()
                logger.info(f"   🎯 TESTE_5_1: {qtd} lançamentos selecionados")

                df = df.drop("T51_FLAG", "__ROW_ID_T51")


            
            elif teste == "TESTE_6_1":  
                if verificar_colunas_necessarias(df, ["MOVIMENTO", "GRUPO_CONTA"]):
                    df = df.withColumn(teste, when(
                        (col("MOVIMENTO") > 30000) & 
                        (col("GRUPO_CONTA").isin(GRUPOS_RECEITA_TESTE_6_1)), 
                        "SIM"))
                else:
                    logger.warning(f"⚠️ {teste}: Colunas necessárias não encontradas - teste desabilitado")
                    df = df.withColumn(teste, lit(None))
            
            elif teste == "TESTE_7_1":
                if verificar_colunas_necessarias(df, ["CLASS_CONTA"]):
                    df = df.withColumn(teste, 
                        when(col("CLASS_CONTA").isin(CLASS_CONTA_TESTE_7_1), "SIM"))
                else:
                    logger.warning(f"⚠️ {teste}: Coluna 'CLASS_CONTA' não encontrada - teste desabilitado")
                    df = df.withColumn(teste, lit(None))
            
            elif teste == "TESTE_7_2":
                if verificar_colunas_necessarias(df, ["DEBITO", "GRUPO_CONTA", "GRUPO_CTP"]):
                    df = df.withColumn(teste, when(
                        (col("DEBITO") != 0) & 
                        (col("GRUPO_CONTA").isin(GRUPO_IMOBILIZADO_TESTE_7_2)) & 
                        (col("GRUPO_CTP").isin(GRUPOS_RECEITA_TESTE_7_2)), 
                        "SIM"))
                else:
                    logger.warning(f"⚠️ {teste}: Colunas necessárias não encontradas - teste desabilitado")
                    df = df.withColumn(teste, lit(None))
                        
            elif teste == "TESTE_7_3":

                if verificar_colunas_necessarias(df, ["DATA", "CONTA", "MOVIMENTO"]):

                    # 🔹 Converter DATA para date real
                    df = df.withColumn(
                        "DATA_TMP",
                        coalesce(
                            to_date(col("DATA"), "dd/MM/yyyy"),
                            to_date(col("DATA"), "yyyy-MM-dd")
                        )
                    )

                    # 🔹 Base 31/12/2025
                    base_2025 = df.filter(
                        (year(col("DATA_TMP")) == 2025) &
                        (month(col("DATA_TMP")) == 12) &
                        (dayofmonth(col("DATA_TMP")) == 31)
                    ).select("CONTA", "MOVIMENTO").distinct()

                    base_2025 = base_2025.withColumn("FLAG_BASE", lit(1))

                    # 🔹 Join com janeiro 2026
                    df = df.join(base_2025, on=["CONTA", "MOVIMENTO"], how="left")

                    df = df.withColumn(
                        teste,
                        when(
                            (year(col("DATA_TMP")) == 2026) &
                            (month(col("DATA_TMP")) == 1) &
                            (col("FLAG_BASE") == 1),
                            "SIM"
                        ).otherwise(None)
                    ).drop("FLAG_BASE", "DATA_TMP")
            
            # -----------------------------
            # TESTE_7_4 (COALESCE COMO OFICIAL)
            # -----------------------------
            elif teste == "TESTE_7_4":

                deb = F.coalesce(F.col("DEBITO").cast("double"), F.lit(0.0))
                cred = F.coalesce(F.col("CREDITO").cast("double"), F.lit(0.0))

                df = df.withColumn(
                    teste,
                    F.when(
                        (
                            (deb != 0) &
                            (F.col("GRUPO_CONTA").isin(*GRUPO_IMOBILIZADO_TESTE_7_4))
                        )
                        |
                        (
                            (cred != 0) &
                            (~F.col("GRUPO_CONTA").isin(*GRUPOS_EXCLUIR_TESTE_7_4))
                        ),
                        "SIM"
                    ).otherwise(None)
                )
            
            # -----------------------------
            # TESTE_7_5
            # -----------------------------
            elif teste == "TESTE_7_5":

                credito = F.col("CREDITO").cast("double")

                df = df.withColumn(
                    teste,
                    F.when(
                        (credito.isNotNull()) &              # não pode ser null
                        (credito != 0) &                     # pode ser positivo ou negativo
                        (F.col("GRUPO_CONTA").isin(GRUPOS_RECEITA_TESTE_7_5)) &
                        (~F.trim(F.lower(F.col("GRUPO_CTP"))).eqNullSafe("contas a receber")),
                        "SIM"
                    )
                )
        
            elif teste == "TESTE_7_6":
                if verificar_colunas_necessarias(df, ["GRUPO_CONTA"]):
                    df = df.withColumn(teste, when(
                        criar_condicao_like_palavras(col("HISTORICO"), PALAVRAS_TESTE_7_6) &
                        (~col("GRUPO_CONTA").isin(GRUPOS_EXCLUIR_TESTE_7_6)), 
                        "SIM"))
                else:
                    logger.warning(f"⚠️ {teste}: Colunas necessárias não encontradas - teste desabilitado")
                    df = df.withColumn(teste, lit(None))
                            
            elif teste == "TESTE_7_7":

                if verificar_colunas_necessarias(df, ["MOVIMENTO", "GRUPO_CONTA", "CLASS_CTP", "DATA"]):

                    logger.info("   💰 Aplicando TESTE_7_7 - Caixa negativo Jan/Fev contra Resultado")

                    # 🔹 Normalizar listas (lowercase)
                    grupos_caixa = [g.lower() for g in GRUPO_CAIXA_TESTE_7_7]
                    class_resultado = [c.lower() for c in CLASS_RESULTADO_TESTE_7_7]

                    # 🔹 Garantir DATA como date
                    df = df.withColumn(
                        "DATA_TMP",
                        coalesce(
                            to_date(col("DATA"), "dd/MM/yyyy"),
                            to_date(col("DATA"), "yyyy-MM-dd")
                        )
                    )

                    # 🔹 Garantir MOVIMENTO numérico
                    mov = col("MOVIMENTO").cast("double")

                    df = df.withColumn(
                        teste,
                        when(
                            (mov < 0) &
                            (trim(lower(col("GRUPO_CONTA"))).isin(grupos_caixa)) &
                            (trim(lower(col("CLASS_CTP"))).isin(class_resultado)) &
                            (month(col("DATA_TMP")).isin([1, 2])),
                            "SIM"
                        ).otherwise(None)
                    )

                    total_sim = df.filter(col(teste) == "SIM").count()
                    logger.info(f"   🎯 TESTE_7_7 detectados: {total_sim}")

                    df = df.drop("DATA_TMP")

                else:
                    logger.warning(f"⚠️ {teste}: colunas necessárias não encontradas")
                    df = df.withColumn(teste, lit(None))


            else:
                df = df.withColumn(teste, lit(None))
        else:
            df = df.withColumn(teste, lit(None))

    # Criar FLAG final
    conditions = []
    for teste, ativo in TESTES_CONFIG.items():
        if ativo:
            conditions.append(col(teste).isNull())

    if conditions:
        flag_condition = conditions[0]
        for condition in conditions[1:]:
            flag_condition = flag_condition & condition
        df = df.withColumn(
            "FLAG",
            when(flag_condition, lit("RETIRAR")).otherwise(lit("MANTER"))
        )
    else:
        df = df.withColumn("FLAG", lit("MANTER"))

    tempo_processamento = time.time() - inicio_processamento
    logger.info(f"✅ Testes aplicados com sucesso em {tempo_processamento:.2f} segundos")

    return df

# ========================================
# RELATÓRIO E SALVAMENTO (COM LOG)
# ========================================

def gerar_relatorio_detalhado(df_resultado, df_original, logger):
    """Gera relatório detalhado dos resultados com logging"""
    logger.info("📊 Gerando relatório detalhado...")
    
    total_original = df_original.count()
    total_manter = df_resultado.filter(col("FLAG") == "MANTER").count()
    total_retirar = df_resultado.filter(col("FLAG") == "RETIRAR").count()
    
    # Log básico dos resultados
    tempo_processamento = 0  # Será calculado na função principal
    log_resultados_testes(logger, df_resultado, df_original, tempo_processamento)
    
    # Continuar com o relatório na tela (compatibilidade)
    print("\n" + "=" * 70)
    print("📊 RELATÓRIO DETALHADO DE AUDITORIA")
    print("=" * 70)
    
    print(f"📈 Total de registros analisados: {total_original:,}")
    print(f"✅ Registros mantidos (MANTER): {total_manter:,}")
    print(f"🚩 Registros a retirar (RETIRAR): {total_retirar:,}")
    print(f"📊 Percentual a retirar: {(total_retirar / total_original * 100):.2f}%")
    
    print("\n🧪 CONFIGURAÇÃO DOS TESTES:")
    print("-" * 50)
    for teste, ativo in TESTES_CONFIG.items():
        status = "✓ ATIVO" if ativo else "✗ INATIVO"
        print(f"• {teste}: {status}")
    
    print("\n🔍 RESULTADOS POR TESTE:")
    print("-" * 50)
    
    colunas_teste = [col_name for col_name in df_resultado.columns if col_name.startswith("TESTE_")]
    for coluna in colunas_teste:
        count = df_resultado.filter(col(coluna) == "SIM").count()
        if count > 0:
            percentual = (count / total_original * 100)
            print(f"• {coluna}: {count:,} registros ({percentual:.2f}%)")
    
    print("\n" + "=" * 70)

def salvar_resultado_final(df, logger, nome_arquivo="resultado_auditoria"):
    """
    Salva o resultado OTIMIZADO mantendo a ORDEM ORIGINAL das colunas
    ⭐ VERSÃO ESTÁVEL - SEM USAR toPandas()
    """
    global ORDEM_COLUNAS_ORIGINAL
    import os, time, glob, shutil
    import pandas as pd
    from pyspark.sql.functions import col

    logger.info("💾 Salvando resultado OTIMIZADO COM PRESERVAÇÃO DE ORDEM...")
    t0 = time.time()
    os.makedirs(SAIDA_DIR, exist_ok=True)

    try:
        # 1️⃣ Filtrar MANTER
        df = df.filter(col("FLAG") == "MANTER")
        total_registros = df.count()
        logger.info(f"   📊 Total a salvar: {total_registros:,} registros")

        # 2️⃣ Identificar colunas a remover
        testes_inativos = {k for k, v in TESTES_CONFIG.items() if not v}
        drop_cols = set(
            [c for c in df.columns if c in testes_inativos or c in COLUNAS_PARA_REMOVER]
        )

        logger.info(f"   🗑️ Colunas a remover ({len(drop_cols)}): {sorted(drop_cols)}")

        # 3️⃣ Construir ordem final preservando original
        colunas_disponiveis = set(df.columns)

        colunas_originais_mantidas = [
            c for c in ORDEM_COLUNAS_ORIGINAL
            if c in colunas_disponiveis and c not in drop_cols
        ]

        colunas_novas = [
            c for c in df.columns
            if c not in ORDEM_COLUNAS_ORIGINAL and c not in drop_cols
        ]

        valid_types = {
            f.name for f in df.schema.fields
            if f.dataType.simpleString() != "void"
        }

        ordem_final = [
            c for c in (colunas_originais_mantidas + colunas_novas)
            if c in valid_types
        ]

        logger.info(f"   📐 Ordem final definida com {len(ordem_final)} colunas")

        df = df.select(*ordem_final)

        # 4️⃣ SALVAR CSV (OFICIAL)
        caminho_csv_dir = os.path.join(SAIDA_DIR, f"{nome_arquivo}_csv")
        logger.info(f"   📁 Salvando CSV: {caminho_csv_dir}")

        df.coalesce(1).write \
            .option("header", True) \
            .option("delimiter", ";") \
            .option("encoding", "UTF-8") \
            .mode("overwrite") \
            .csv(caminho_csv_dir)

        logger.info("   ✅ CSV salvo com sucesso")

        # 5️⃣ SALVAR XLSX MODO SEGURO (SEM toPandas)
        xlsx_path = os.path.join(SAIDA_DIR, f"{nome_arquivo}.xlsx")

        if total_registros > XLSX_MAX_ROWS:
            logger.warning("   ⚠️ Arquivo grande demais para Excel. XLSX não gerado.")
            xlsx_path = None
        else:
            try:
                logger.info("   📗 Gerando XLSX via CSV temporário (modo seguro)...")

                temp_dir = os.path.join(SAIDA_DIR, "_temp_xlsx")

                df.coalesce(1).write \
                    .option("header", True) \
                    .option("delimiter", ";") \
                    .mode("overwrite") \
                    .csv(temp_dir)

                csv_file = glob.glob(os.path.join(temp_dir, "*.csv"))[0]

                pdf = pd.read_csv(csv_file, sep=";")

                pdf.to_excel(xlsx_path, index=False, engine="openpyxl")

                shutil.rmtree(temp_dir)

                logger.info("   ✅ XLSX salvo com sucesso (modo seguro)")

            except Exception as e_xlsx:
                logger.warning(f"   ⚠️ Erro ao salvar XLSX: {e_xlsx}")
                xlsx_path = None

        tempo_total = time.time() - t0

        log_salvamento_resultado(logger, caminho_csv_dir, total_registros, tempo_total)

        print("\n✅ SALVAMENTO CONCLUÍDO!")
        print(f"📁 CSV: {caminho_csv_dir}")
        if xlsx_path:
            print(f"📗 XLSX: {xlsx_path}")
        print(f"📊 Registros: {total_registros:,}")
        print(f"📋 Colunas: {len(df.columns)}")
        print(f"⏱️ Tempo: {tempo_total:.2f}s")

        return {
            "csv_dir": caminho_csv_dir,
            "xlsx_file": xlsx_path,
            "registros": total_registros,
            "colunas": len(df.columns),
            "tempo": tempo_total
        }

    except Exception as e:
        logger.error(f"❌ Erro crítico ao salvar: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ========================================
# FUNÇÃO PRINCIPAL (COM LOG)
# ========================================

def main():
    """Função principal unificada com sistema de log completo"""
    print("🎯 AUDITORIA CONTÁBIL - VERSÃO 4.1 COM PRESERVAÇÃO DE ORDEM")
    print("=" * 70)

    inicio_execucao = time.time()

    # Configurar logging
    logger, log_file = configurar_logging()

    # Log da configuração inicial
    log_configuracao_inicial(logger)

    # Verificar arquivos
    if not os.path.exists(ARQ_RAZAO):
        logger.error(f"❌ Arquivo TB_RAZAO não encontrado: {ARQ_RAZAO}")
        print(f"❌ Arquivo TB_RAZAO não encontrado: {ARQ_RAZAO}")
        return False

    # Criar sessão Spark
    logger.info("🚀 Iniciando sessão Spark...")
    spark = criar_spark_session()
    if not spark:
        logger.error("❌ Falha ao criar sessão Spark")
        return False

    logger.info(f"✅ Spark {spark.version} iniciado com sucesso")

    try:
        # Garante existência antes de usar em qualquer caminho
        tempo_total = 0.0
        arquivos_gerados = []
        inicio_total = time.time()

        # Carregar dados
        logger.info("🚀 Iniciando processamento de auditoria...")
        df_razao = carregar_dados_razao(spark, logger)
        df_fds = carregar_dados_fds(spark, logger)
        if df_razao is None:
            logger.error("❌ Falha no carregamento dos dados")
            return False

         # ⭐ ADICIONAR AQUI - Converter MOVIMENTO para numérico
                # =====================================================
        # 🔹 NORMALIZAÇÃO DE DADOS (OBRIGATÓRIO)
        # =====================================================

        # 1️⃣ Converter colunas monetárias (BR → double real)
        df_razao = converter_colunas_monetarias(
            df_razao,
            ["MOVIMENTO", "DEBITO", "CREDITO"],
            logger
        )

        # 2️⃣ Normalizar colunas textuais (lower + trim)
        from pyspark.sql.functions import lower, trim, col

        for c in ["GRUPO_CONTA","CLASS_CONTA","CLASS_CTP","GRUPO_CTP","HISTORICO"]:
            if c in df_razao.columns:
                df_razao = df_razao.withColumn(
                    c,
                    lower(trim(col(c).cast("string")))
                )

        logger.info("✅ Normalização numérica e textual concluída")

        # =====================================================
        # 🔒 NORMALIZAÇÃO DEFINITIVA DAS LISTAS (OBRIGATÓRIO)
        # =====================================================

        def normalizar_lista(lista):
            return [str(x).strip().lower() for x in lista] if lista else []

        # TESTE 2_1
        CLASS_RESULTADO_TESTE_2_1[:] = normalizar_lista(CLASS_RESULTADO_TESTE_2_1)

        # TESTE 6_1
        GRUPOS_RECEITA_TESTE_6_1[:] = normalizar_lista(GRUPOS_RECEITA_TESTE_6_1)

        # TESTE 7_1
        CLASS_CONTA_TESTE_7_1[:] = normalizar_lista(CLASS_CONTA_TESTE_7_1)

        # TESTE 7_2
        GRUPOS_RECEITA_TESTE_7_2[:] = normalizar_lista(GRUPOS_RECEITA_TESTE_7_2)
        GRUPO_IMOBILIZADO_TESTE_7_2[:] = normalizar_lista(GRUPO_IMOBILIZADO_TESTE_7_2)

        # TESTE 7_4
        GRUPO_IMOBILIZADO_TESTE_7_4[:] = normalizar_lista(GRUPO_IMOBILIZADO_TESTE_7_4)
        GRUPOS_EXCLUIR_TESTE_7_4[:] = normalizar_lista(GRUPOS_EXCLUIR_TESTE_7_4)

        # TESTE 7_5
        GRUPOS_RECEITA_TESTE_7_5[:] = normalizar_lista(GRUPOS_RECEITA_TESTE_7_5)
        GRUPOS_CONTAS_RECEBER_TESTE_7_5[:] = normalizar_lista(GRUPOS_CONTAS_RECEBER_TESTE_7_5)

        # TESTE 7_6
        GRUPOS_EXCLUIR_TESTE_7_6[:] = normalizar_lista(GRUPOS_EXCLUIR_TESTE_7_6)

        # TESTE 7_7
        GRUPO_CAIXA_TESTE_7_7[:] = normalizar_lista(GRUPO_CAIXA_TESTE_7_7)
        CLASS_RESULTADO_TESTE_7_7[:] = normalizar_lista(CLASS_RESULTADO_TESTE_7_7)

        logger.info("🔒 Listas globais normalizadas com sucesso")

        

        # 🔎 LOG DOS DOMÍNIOS ÚNICOS
        log_dominios_colunas(df_razao, logger)


        # =====================================================
        # 🔹 PADRONIZAÇÃO DEFINITIVA DA DATA → dd/MM/yyyy
        # =====================================================
  
        if "DATA" in df_razao.columns:
            logger.info("📅 Padronizando DATA para formato brasileiro dd/MM/yyyy...")

            df_razao = df_razao.withColumn(
                "DATA_TMP",
                coalesce(
                    to_date(col("DATA"), "dd/MM/yyyy"),
                    to_date(col("DATA"), "yyyy-MM-dd"),
                    to_date(col("DATA"), "yyyy-MM-dd HH:mm:ss")
                )
            )

            df_razao = df_razao.withColumn(
                "DATA",
                date_format(col("DATA_TMP"), "dd/MM/yyyy")
            ).drop("DATA_TMP")

            logger.info("✅ DATA padronizada com sucesso")

        # Aplicar testes
        df_resultado = aplicar_testes_auditoria(df_razao, df_fds, FERIADOS, logger)

        # Log de tempo parcial (processamento)
        tempo_processamento = time.time() - inicio_execucao
        log_resultados_testes(logger, df_resultado, df_razao, tempo_processamento)

        # Relatório detalhado e verificação de colunas
        gerar_relatorio_detalhado(df_resultado, df_razao, logger)
        colunas_finais = verificar_colunas_remocao(df_resultado, logger)

        # Salvar resultado (CSV + XLSX)
        salvo = salvar_resultado_final(df_resultado, logger)

        # Gerar relatórios adicionais (configuração)
        tempo_total = time.time() - inicio_execucao
        logger.info("📊 Gerando relatórios de configuração...")
        md_file = gerar_relatorio_markdown(logger, tempo_total)
        csv_config = gerar_relatorio_csv_configuracao(logger)
        py_backup = gerar_relatorio_python_config(logger)

        if md_file:
            arquivos_gerados.append(f"📄 Relatório Markdown: {md_file}")
        if csv_config:
            arquivos_gerados.append(f"📊 CSV Configuração: {csv_config}")
        if py_backup:
            arquivos_gerados.append(f"🐍 Backup Python: {py_backup}")

        # Sucesso se o salvamento retornou dados
        sucesso = bool(salvo)

        # Log final + prints
        log_resumo_final(logger, sucesso, tempo_total)

        if sucesso:
            print("\n🎉 PROCESSO CONCLUÍDO COM SUCESSO!")
            print(f"📁 CSV (pasta): {salvo.get('csv_dir')}")
            if salvo.get("xlsx_file"):
                print(f"📗 XLSX: {salvo.get('xlsx_file')}")
            print(f"📄 Log detalhado em: {log_file}")
            for arquivo in arquivos_gerados:
                print(arquivo)
            return True
        else:
            print("\n⚠️ Processo concluído com problemas na gravação")
            print(f"📄 Log detalhado em: {log_file}")
            for arquivo in arquivos_gerados:
                print(arquivo)
            return False

    except Exception as e:
        tempo_total = time.time() - inicio_execucao
        logger.error(f"❌ Erro no processamento: {e}")
        log_resumo_final(logger, False, tempo_total)
        print(f"\n❌ Erro no processamento: {e}")
        import traceback
        logger.error(f"📜 Detalhes: {traceback.format_exc()}")
        print(f"📜 Detalhes: {traceback.format_exc()}")
        return False

    finally:
        if spark:
            spark.stop()
            logger.info("🛑 Spark finalizado")
            print("🛑 Spark finalizado")


# ========================================
# FUNÇÕES AUXILIARES PARA CONFIGURAÇÃO
# ========================================

def adicionar_palavra_teste(teste, palavra):
    """Adiciona uma palavra às constantes de teste"""
    global PALAVRAS_TESTE_1_4, PALAVRAS_TESTE_1_5, PALAVRAS_TESTE_1_6, PALAVRAS_TESTE_1_7, PALAVRAS_TESTE_7_6
    
    if teste == "TESTE_1_4":
        PALAVRAS_TESTE_1_4.append(palavra)
        print(f"✅ Palavra '{palavra}' adicionada ao {teste}")
    elif teste == "TESTE_1_5":
        PALAVRAS_TESTE_1_5.append(palavra)
        print(f"✅ Palavra '{palavra}' adicionada ao {teste}")
    elif teste == "TESTE_1_6":
        PALAVRAS_TESTE_1_6.append(palavra)
        print(f"✅ Palavra '{palavra}' adicionada ao {teste}")
    elif teste == "TESTE_1_7":
        PALAVRAS_TESTE_1_7.append(palavra)
        print(f"✅ Palavra '{palavra}' adicionada ao {teste}")
    elif teste == "TESTE_7_6":
        PALAVRAS_TESTE_7_6.append(palavra)
        print(f"✅ Palavra '{palavra}' adicionada ao {teste}")
    else:
        print(f"⚠️ Teste '{teste}' não encontrado")

def listar_palavras_teste():
    """Lista todas as palavras configuradas por teste"""
    print("\n📋 PALAVRAS CONFIGURADAS POR TESTE:")
    print("-" * 60)
    print(f"• TESTE_1_4: {PALAVRAS_TESTE_1_4}")
    print(f"• TESTE_1_5: {PALAVRAS_TESTE_1_5}")
    print(f"• TESTE_1_6: {PALAVRAS_TESTE_1_6}")
    print(f"• TESTE_1_7: {PALAVRAS_TESTE_1_7}")
    print(f"• TESTE_7_6: {PALAVRAS_TESTE_7_6}")
    print(f"• NOMES_TESTE_1_8: {NOMES_TESTE_1_8}")
    print("-" * 60)

# ========================================
# EXECUÇÃO
# ========================================

if __name__ == "__main__":
    print("🚀 INICIANDO SCRIPT UNIFICADO DE AUDITORIA v4.1")
    print("=" * 70)
    
    sucesso = main()
    
    if sucesso:
        print("\n🎉 EXECUÇÃO CONCLUÍDA COM SUCESSO!")
    else:
        print("\n❌ ERRO NA EXECUÇÃO")
        sys.exit(1)