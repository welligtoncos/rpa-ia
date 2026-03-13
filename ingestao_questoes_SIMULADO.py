from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
import re


# ──────────────────────────────────────────────
# EXTRAÇÃO DE SIMULADOS (gabarito por questão)
# ──────────────────────────────────────────────

def extrair_simulado_pagina(driver):
    """
    Extrai todas as questões de uma página de simulado/gabarito.
    Estrutura esperada: div.q-answer-sheet-question
    Captura: enunciado, alternativas, resposta marcada, resposta correta,
             resultado (Certa/Errada) e comentário do professor.
    """
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    questoes_data = []

    blocos = soup.find_all('div', class_='q-answer-sheet-question')
    print(f"  Encontradas {len(blocos)} questões na página")

    for bloco in blocos:
        try:
            # ── Número da questão ──────────────────────────────────────
            resumed_stmt = bloco.find('span', class_='q-resumed-statement')
            numero_questao = 'N/A'
            enunciado_resumido = ''
            if resumed_stmt:
                texto_completo = resumed_stmt.get_text(separator=' ', strip=True)
                match = re.match(r'^(\d+)[.\s]*(.*)', texto_completo, re.DOTALL)
                if match:
                    numero_questao = match.group(1)
                    enunciado_resumido = match.group(2).strip()

            # ── Resultado (Certa / Errada) ─────────────────────────────
            resultado = 'N/A'
            certa = bloco.find('span', class_='q-correct')
            errada = bloco.find('span', class_='q-incorrect')
            if certa and 'Certa' in certa.get_text():
                resultado = 'Certa'
            elif errada and 'Errada' in errada.get_text():
                resultado = 'Errada'

            # ── Enunciado completo (versão expandida) ──────────────────
            enunciado_div = bloco.find('div', class_='q-question-enunciation')
            enunciado = enunciado_div.get_text(separator=' ', strip=True) if enunciado_div else enunciado_resumido

            # ── Resposta marcada pelo usuário e resposta correta ───────
            resposta_marcada = 'N/A'
            resposta_correta = 'N/A'

            # Span com "Você marcou X, mas a alternativa correta é Y"
            # ou "Correto! Você marcou X."
            q_answer_div = bloco.find('div', class_='q-answer')
            if q_answer_div:
                texto_answer = q_answer_div.get_text(separator=' ', strip=True)

                # Errada: "Você marcou a resposta A, mas a alternativa correta é C."
                match_errada = re.search(
                    r'marcou a resposta\s+(\S+).*?correta[^é]*é\s+([A-Z\d]+)',
                    texto_answer, re.IGNORECASE | re.DOTALL
                )
                # Certa: "Correto! Você marcou a resposta B." ou "Você marcou a resposta C."
                match_certa = re.search(
                    r'marcou a resposta\s+([A-Z\d]+)',
                    texto_answer, re.IGNORECASE
                )

                if match_errada:
                    resposta_marcada = match_errada.group(1).strip()
                    resposta_correta = match_errada.group(2).strip()
                elif match_certa:
                    resposta_marcada = match_certa.group(1).strip()
                    resposta_correta = resposta_marcada  # acertou

            # Fallback: pegar input checked
            if resposta_correta == 'N/A':
                input_checked = bloco.find('input', {'type': 'radio', 'checked': True})
                if input_checked:
                    resposta_correta = input_checked.get('value', 'N/A')
                    resposta_marcada = resposta_correta

            # ── Alternativas ───────────────────────────────────────────
            alternativas_dict = {}
            opcoes_div = bloco.find('ul', class_='q-question-options')
            if opcoes_div:
                for label in opcoes_div.find_all('label', class_='q-radio-button'):
                    inp = label.find('input', type='radio')
                    letra = inp.get('value', '') if inp else ''

                    # Certo/Errado (questões V/F)
                    if letra in ('C', 'E') and label.find('div', class_='q-item-enum'):
                        texto_alt = label.find('div', class_='q-item-enum').get_text(strip=True)
                        if texto_alt in ('Certo', 'Errado'):
                            alternativas_dict['Alternativa_Certo'] = 'Certo'
                            alternativas_dict['Alternativa_Errado'] = 'Errado'
                            continue

                    item_enum = label.find('div', class_='q-item-enum')
                    texto_alt = item_enum.get_text(separator=' ', strip=True) if item_enum else ''
                    if letra:
                        alternativas_dict[f'Alternativa_{letra}'] = texto_alt

            # ── Comentário do professor ────────────────────────────────
            comentario = 'N/A'
            q_text_div = bloco.find('div', class_='q-text')
            if q_text_div:
                comentario = q_text_div.get_text(separator='\n', strip=True)
                comentario = re.sub(r'\n{3,}', '\n\n', comentario)  # limpa espaços extras

            # ── Monta registro ─────────────────────────────────────────
            dados = {
                'Numero_Questao': numero_questao,
                'Resultado': resultado,
                'Enunciado': enunciado,
                'Resposta_Marcada': resposta_marcada,
                'Resposta_Correta': resposta_correta,
                'Comentario_Professor': comentario,
            }
            dados.update(alternativas_dict)
            questoes_data.append(dados)

            status = '✓' if resultado == 'Certa' else '✗'
            print(f"  {status} Q{numero_questao} | Marcou: {resposta_marcada} | Correta: {resposta_correta}")

        except Exception as e:
            print(f"  ! Erro ao extrair questão: {e}")
            continue

    return questoes_data


def extrair_simulado_completo(driver):
    """Percorre todas as páginas do simulado e extrai as questões."""
    todas_questoes = []
    pagina_atual = 1

    while True:
        print(f"\n--- Página {pagina_atual} ---")
        sleep(3)

        questoes_pagina = extrair_simulado_pagina(driver)
        todas_questoes.extend(questoes_pagina)
        print(f"  Total acumulado: {len(todas_questoes)} questões")

        # Tenta ir para a próxima página
        try:
            botao_proxima = driver.find_element(
                By.XPATH,
                "//a[@rel='next' or (contains(@class,'next') and not(contains(@class,'disabled')))]"
            )
            classe = botao_proxima.get_attribute('class') or ''
            if 'disabled' in classe:
                print("  ✓ Última página")
                break

            driver.execute_script("arguments[0].scrollIntoView(true);", botao_proxima)
            sleep(1)
            driver.execute_script("arguments[0].click();", botao_proxima)
            pagina_atual += 1

        except Exception:
            print("  ✓ Sem próxima página")
            break

    return todas_questoes


# ──────────────────────────────────────────────
# EXTRAÇÃO DE CADERNOS (formato antigo — mantido)
# ──────────────────────────────────────────────

def extrair_gabarito_pagina(soup):
    gabarito = {}
    try:
        fieldset_feedback = soup.find('fieldset', class_='q-questions-feedback')
        if fieldset_feedback:
            for feedback in fieldset_feedback.find_all('div', class_='q-feedback'):
                index_span = feedback.find('span', class_='q-index')
                answer_span = feedback.find('span', class_='q-answer')
                if index_span and answer_span:
                    indice = index_span.get_text(strip=True).replace(':', '')
                    resposta = answer_span.get_text(strip=True)
                    gabarito[indice] = resposta
            print(f"✓ Gabarito extraído: {len(gabarito)} respostas")
    except Exception as e:
        print(f"✗ Erro ao extrair gabarito: {e}")
    return gabarito


def extrair_questoes_pagina(driver):
    questoes_data = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    gabarito = extrair_gabarito_pagina(soup)
    questoes = soup.find_all('div', class_='js-question-item')
    print(f"Encontradas {len(questoes)} questões na página atual")

    for questao in questoes:
        try:
            question_div = questao.find('div', class_='js-question')
            question_id = question_div.get('data-question-id', 'N/A') if question_div else 'N/A'

            q_index_span = questao.find('span', class_='q-index')
            indice_questao = q_index_span.get_text(strip=True) if q_index_span else None

            resposta_correta = 'N/A'
            if indice_questao and indice_questao in gabarito:
                resposta_correta = gabarito[indice_questao]

            if resposta_correta == 'N/A':
                right_answer = questao.find('span', class_='js-question-right-answer')
                if right_answer:
                    resposta_correta = right_answer.get_text(strip=True)

            if resposta_correta == 'N/A':
                alternativa_correta = questao.find('label', class_='q-correct')
                if alternativa_correta:
                    input_radio = alternativa_correta.find('input', type='radio')
                    if input_radio:
                        resposta_correta = input_radio.get('value', 'N/A')

            enunciado_div = questao.find('div', class_='q-question-enunciation')
            enunciado = enunciado_div.get_text(strip=True) if enunciado_div else 'N/A'

            info_div = questao.find('div', class_='q-question-info')
            ano = banca = orgao = prova = 'N/A'
            if info_div:
                for span in info_div.find_all('span'):
                    texto = span.get_text(strip=True)
                    if 'Ano:' in texto:
                        ano = texto.replace('Ano:', '').strip()
                    elif 'Banca:' in texto:
                        link = span.find('a')
                        if link:
                            banca = link.get_text(strip=True)
                    elif 'Órgão:' in texto:
                        link = span.find('a')
                        if link:
                            orgao = link.get_text(strip=True)
                    elif 'Prova:' in texto:
                        link = span.find('a')
                        if link:
                            prova = link.get_text(strip=True)

            breadcrumb = questao.find('div', class_='q-question-breadcrumb')
            disciplina = assunto = 'N/A'
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) > 0:
                    disciplina = links[0].get_text(strip=True)
                if len(links) > 1:
                    assunto = links[1].get_text(strip=True)

            alternativas_dict = {}
            opcoes_div = questao.find('div', class_='q-question-options')
            if opcoes_div:
                for label in opcoes_div.find_all('label', class_='q-radio-button'):
                    inp = label.find('input', type='radio')
                    letra = inp.get('value', '') if inp else ''
                    alt_div = label.find('div', class_='q-item-enum')
                    texto_alt = alt_div.get_text(strip=True) if alt_div else ''
                    if letra:
                        alternativas_dict[f'Alternativa_{letra}'] = texto_alt

            dados_questao = {
                'ID_Questao': f'Q{question_id}',
                'Indice_Pagina': indice_questao or 'N/A',
                'Ano': ano, 'Banca': banca, 'Orgao': orgao, 'Prova': prova,
                'Disciplina': disciplina, 'Assunto': assunto,
                'Enunciado': enunciado, 'Resposta_Correta': resposta_correta,
            }
            dados_questao.update(alternativas_dict)
            questoes_data.append(dados_questao)
            print(f"✓ Questão {question_id} | Resposta: {resposta_correta}")

        except Exception as e:
            print(f"✗ Erro ao extrair questão: {e}")
            continue

    return questoes_data


# ──────────────────────────────────────────────
# LISTAGEM E EXTRAÇÃO DE CADERNOS
# ──────────────────────────────────────────────

def listar_cadernos_disponiveis(driver):
    print("\n=== Buscando cadernos disponíveis ===")
    sleep(2)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    cadernos = []

    links_cadernos = soup.find_all('a', href=re.compile(r'/questoes-de-concursos/questoes\?notebook_ids'))
    for link in links_cadernos:
        try:
            href = link.get('href', '')
            nome = link.get_text(strip=True)
            match = re.search(r'notebook_ids%5B%5D=(\d+)', href)
            if match:
                caderno_id = match.group(1)
                parent = link.find_parent('div', class_='q-content')
                num_questoes = 'N/A'
                if parent:
                    subtitle = parent.find('div', class_='q-subtitle')
                    if subtitle:
                        m = re.search(r'(\d+)\s+questões', subtitle.get_text(strip=True))
                        if m:
                            num_questoes = m.group(1)
                cadernos.append({'id': caderno_id, 'nome': nome,
                                 'href': href, 'num_questoes': num_questoes})
        except Exception:
            continue

    ids_vistos = set()
    cadernos_unicos = []
    for c in cadernos:
        if c['id'] not in ids_vistos:
            cadernos_unicos.append(c)
            ids_vistos.add(c['id'])
    return cadernos_unicos


def extrair_caderno(driver, caderno_info, wait):
    print(f"\n{'='*60}")
    print(f"📚 CADERNO: {caderno_info['nome']}")
    print(f"📊 Questões: {caderno_info['num_questoes']} | 🔑 ID: {caderno_info['id']}")
    print(f"{'='*60}\n")

    todas_questoes = []
    caderno_clicado = False

    for tentativa, seletor in enumerate([
        (By.XPATH, f"//a[contains(@href, '{caderno_info['id']}')]"),
        (By.XPATH, f"//a[contains(text(), '{caderno_info['nome']}')]"),
    ]):
        try:
            elem = driver.find_element(*seletor)
            driver.execute_script("arguments[0].scrollIntoView(true);", elem)
            sleep(0.5)
            driver.execute_script("arguments[0].click();", elem)
            caderno_clicado = True
            print(f"✓ Caderno aberto")
            break
        except Exception:
            continue

    if not caderno_clicado:
        print("✗ Não foi possível abrir o caderno")
        return None

    sleep(5)
    pagina_atual = 1

    while True:
        print(f"\n--- Página {pagina_atual} ---")
        sleep(3)
        questoes_pagina = extrair_questoes_pagina(driver)
        todas_questoes.extend(questoes_pagina)
        print(f"Total: {len(todas_questoes)} questões")

        try:
            botao = driver.find_element(
                By.XPATH, "//a[@rel='next' or contains(@class, 'next')]"
            )
            if 'disabled' in (botao.get_attribute('class') or ''):
                break
            driver.execute_script("arguments[0].scrollIntoView(true);", botao)
            sleep(1)
            driver.execute_script("arguments[0].click();", botao)
            pagina_atual += 1
        except Exception:
            break

    return todas_questoes


# ──────────────────────────────────────────────
# SALVAMENTO
# ──────────────────────────────────────────────

def salvar_dataframe(questoes, nome_arquivo_base, modo='caderno'):
    if not questoes:
        print("✗ Nenhuma questão para salvar")
        return None

    df = pd.DataFrame(questoes)

    if modo == 'simulado':
        colunas_base = [
            'Numero_Questao', 'Resultado',
            'Enunciado', 'Resposta_Marcada', 'Resposta_Correta',
            'Comentario_Professor',
        ]
    else:
        colunas_base = [
            'ID_Questao', 'Indice_Pagina', 'Ano', 'Banca', 'Orgao', 'Prova',
            'Disciplina', 'Assunto', 'Enunciado', 'Resposta_Correta',
        ]

    colunas_alt = sorted([c for c in df.columns if c.startswith('Alternativa_')])
    colunas_final = colunas_base + colunas_alt

    for col in colunas_final:
        if col not in df.columns:
            df[col] = ''

    df = df[colunas_final]

    nome_arquivo = f"{nome_arquivo_base.replace(' ', '_').replace('-', '_')}.xlsx"
    nome_arquivo = re.sub(r'[^\w\-_\.]', '', nome_arquivo)
    df.to_excel(nome_arquivo, index=False, engine='openpyxl')

    print(f"\n✅ Arquivo salvo: {nome_arquivo}")
    print(f"📊 Total: {len(df)} questões")

    if modo == 'simulado':
        certas = df[df['Resultado'] == 'Certa'].shape[0]
        erradas = df[df['Resultado'] == 'Errada'].shape[0]
        print(f"✅ Certas: {certas} | ✗ Erradas: {erradas}")
    else:
        encontradas = df[df['Resposta_Correta'] != 'N/A'].shape[0]
        print(f"✅ Respostas encontradas: {encontradas}/{len(df)}")

    return df


# ──────────────────────────────────────────────
# LISTAGEM DE SIMULADOS
# ──────────────────────────────────────────────

def listar_simulados_disponiveis(driver):
    """Lista simulados/provas disponíveis na área do usuário."""
    print("\n=== Buscando simulados disponíveis ===")
    sleep(2)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    simulados = []

    # Tenta encontrar links de simulados/provas
    padroes = [
        re.compile(r'/simulados/'),
        re.compile(r'/provas/'),
        re.compile(r'/practice-tests/'),
        re.compile(r'simulado'),
    ]

    links_vistos = set()
    for padrao in padroes:
        for link in soup.find_all('a', href=padrao):
            href = link.get('href', '')
            if href in links_vistos:
                continue
            links_vistos.add(href)
            nome = link.get_text(strip=True) or href
            simulados.append({'nome': nome, 'href': href})

    return simulados


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def run_chrome_headless():
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(service=Service(), options=options)

    try:
        driver.get("https://www.qconcursos.com/")
        print("✓ Site carregado")
        sleep(3)

        wait = WebDriverWait(driver, 15)

        # ── Login ────────────────────────────────────────────────────
        try:
            botao_entrar = driver.find_element(By.CSS_SELECTOR, "button.text-gray-800.font-semibold")
            driver.execute_script("arguments[0].click();", botao_entrar)
        except Exception:
            for botao in driver.find_elements(By.TAG_NAME, "button"):
                if "Entrar" in botao.text:
                    driver.execute_script("arguments[0].click();", botao)
                    break
        sleep(3)

        # Continuar com Google
        for seletor in [
            (By.CSS_SELECTOR, "button[q\\:id='aj']"),
            (By.XPATH, "//button[.//span[contains(text(), 'Continuar com Google')]]"),
        ]:
            try:
                btn = driver.find_element(*seletor)
                driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                sleep(0.5)
                driver.execute_script("arguments[0].click();", btn)
                break
            except Exception:
                continue
        sleep(4)

        # Email
        try:
            campo = wait.until(EC.presence_of_element_located((By.ID, "identifierId")))
            campo.clear()
            campo.send_keys("welligtoncos@gmail.com")
            sleep(2)
            driver.find_element(By.ID, "identifierNext").click()
        except Exception as e:
            print(f"✗ Email: {e}")
        sleep(4)

        # Senha
        try:
            campo = wait.until(EC.presence_of_element_located((By.NAME, "Passwd")))
            campo.clear()
            campo.send_keys("@Well32213115")
            sleep(2)
            driver.find_element(By.ID, "passwordNext").click()
        except Exception as e:
            print(f"✗ Senha: {e}")
        sleep(8)

        # ── Menu do usuário → Meu Painel ─────────────────────────────
        try:
            icone = driver.find_element(By.CSS_SELECTOR, "div.size-\\[32px\\].rounded-full.bg-gray-100")
            driver.execute_script("arguments[0].click();", icone)
        except Exception:
            pass
        sleep(2)

        try:
            painel = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Meu Painel')]")))
            driver.execute_script("arguments[0].click();", painel)
        except Exception:
            for link in driver.find_elements(By.TAG_NAME, "a"):
                if "Meu Painel" in link.text:
                    driver.execute_script("arguments[0].click();", link)
                    break
        sleep(3)

        # ── Escolha do modo ──────────────────────────────────────────
        print(f"\n{'='*60}")
        print("O QUE DESEJA EXTRAIR?")
        print("  1. Cadernos de questões")
        print("  2. Simulado / Gabarito (página atual ou URL)")
        print(f"{'='*60}")
        modo = input("Escolha (1 ou 2): ").strip()

        # ════════════════════════════════════════════════════════════
        if modo == '1':
            # Cadernos (fluxo original)
            try:
                cadernos_link = driver.find_element(
                    By.XPATH, "//a[.//span[contains(text(), 'Meus cadernos')]]"
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", cadernos_link)
                sleep(0.5)
                driver.execute_script("arguments[0].click();", cadernos_link)
            except Exception:
                print("✗ Erro ao acessar Meus cadernos")
            sleep(3)

            cadernos_disponiveis = listar_cadernos_disponiveis(driver)
            if not cadernos_disponiveis:
                print("✗ Nenhum caderno encontrado!")
                return

            print(f"\n{'='*70}\nCADERNOS DISPONÍVEIS:\n{'='*70}")
            for idx, c in enumerate(cadernos_disponiveis, 1):
                print(f"{idx}. {c['nome']}  [ID: {c['id']} | Questões: {c['num_questoes']}]")

            print(f"\n{'='*70}")
            print("Digite NÚMERO, vários separados por vírgula, ou TODOS")
            escolha = input("Escolha: ").strip()

            if escolha.upper() == 'TODOS':
                cadernos_para_extrair = cadernos_disponiveis
            elif ',' in escolha:
                indices = [int(x.strip()) for x in escolha.split(',')]
                cadernos_para_extrair = [cadernos_disponiveis[i-1] for i in indices
                                         if 1 <= i <= len(cadernos_disponiveis)]
            else:
                idx = int(escolha)
                cadernos_para_extrair = [cadernos_disponiveis[idx-1]]

            todos_dfs = {}
            for caderno_info in cadernos_para_extrair:
                driver.get("https://www.qconcursos.com/usuario/cadernos")
                sleep(3)
                questoes = extrair_caderno(driver, caderno_info, wait)
                if questoes:
                    df = salvar_dataframe(questoes, f"caderno_{caderno_info['nome']}", modo='caderno')
                    todos_dfs[caderno_info['nome']] = df

            print(f"\n{'='*60}")
            print(f"🎉 Cadernos extraídos: {len(todos_dfs)}")
            print(f"📊 Total de questões: {sum(len(d) for d in todos_dfs.values())}")

        # ════════════════════════════════════════════════════════════
        elif modo == '2':
            print("\nOpções:")
            print("  A) Já estou na página do simulado/gabarito (extrair daqui)")
            print("  B) Informar URL do simulado")
            sub = input("Escolha (A ou B): ").strip().upper()

            if sub == 'B':
                url = input("Cole a URL do simulado/gabarito: ").strip()
                driver.get(url)
                sleep(5)

            nome_sim = input("Nome para o arquivo de saída (ex: simulado_direito_constitucional): ").strip()
            if not nome_sim:
                nome_sim = "simulado"

            print("\n🔍 Iniciando extração do simulado...")
            questoes = extrair_simulado_completo(driver)

            if questoes:
                salvar_dataframe(questoes, nome_sim, modo='simulado')
            else:
                print("✗ Nenhuma questão extraída. Verifique se está na página correta.")

        else:
            print("✗ Opção inválida")

    except Exception as e:
        print(f"Erro geral: {e}")
        driver.save_screenshot("erro.png")

    finally:
        input("\nPressione Enter para fechar...")
        driver.quit()


if __name__ == "__main__":
    run_chrome_headless()