#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup da Configuração de Auditoria Contábil
Gerado em: 05/03/2026 15:56:23
"""

# ========================================
# CONFIGURAÇÃO DOS TESTES
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
    'TESTE_7_9': False,
}

# ========================================
# PALAVRAS-CHAVE DOS TESTES
# ========================================

PALAVRAS_TESTE_1_4 = ['segundo', 'acordo', 'ordem', 'mail', 'conforme', 'controller', 'pedido']

PALAVRAS_TESTE_1_5 = ['ajuste', 'acerto']

PALAVRAS_TESTE_1_6 = ['reversao', 'reversão', 'estorno']

PALAVRAS_TESTE_1_7 = ['outros', 'outras', 'desconhecidos', 'diversos']

PALAVRAS_TESTE_7_6 = ['variacao cambial', 'variação cambial', 'cambial']

NOMES_TESTE_1_8 = ['GABRIELA POIATTO TEIXEIRA', 'GABRIELA', 'POIATTO', 'TEIXEIRA', 'TATIANE SILVA MATHEW', 'TATIANE', 'SILVA', 'MATHEW', 'DENNY JOSEPH', 'DENNY', 'JOSEPH', 'WANDERLEI ANTONIO', 'WANDERLEI', 'ANTONIO', 'TIAGO ALEXANDRE', 'TIAGO', 'ALEXANDRE']

NOMES_TESTE_7_9 = ['']

# ========================================
# GRUPOS DE CLASSIFICAÇÃO
# ========================================

CLASS_RESULTADO_TESTE_2_1 = ['resultado']

GRUPOS_RECEITA_TESTE_6_1 = ['receita líquidareceitas financeiras']

CLASS_CONTA_TESTE_7_1 = ['resultado']

GRUPOS_RECEITA_TESTE_7_2 = ['outras receitas', 'receitas financeiras', 'receita bruta']

GRUPO_IMOBILIZADO_TESTE_7_2 = ['imobilizado']

# ========================================
# FERIADOS
# ========================================

FERIADOS = ['01/01/2025', '20/01/2025', '13/02/2025', '29/03/2025', '21/04/2025', '23/04/2025', '01/05/2025', '30/05/2025', '07/09/2025', '12/10/2025', '02/11/2025', '15/11/2025', '20/11/2025', '25/12/2025']

# ========================================
# CONFIGURAÇÕES TÉCNICAS
# ========================================

CSV_SEPARATOR = ";"

COLUNAS_PARA_REMOVER = ['FLAG', 'LINHA_SEQ', 'LINHA_SEQ_TESTE4', 'Semana_Resumida', 'VALOR_REDONDO']

# Para restaurar esta configuração, copie as variáveis acima 
# para o script principal
