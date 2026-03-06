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

def extrair_gabarito_pagina(soup):
    """Extrai o gabarito geral da página"""
    gabarito = {}
    
    try:
        # Procura pelo fieldset com as respostas
        fieldset_feedback = soup.find('fieldset', class_='q-questions-feedback')
        
        if fieldset_feedback:
            feedbacks = fieldset_feedback.find_all('div', class_='q-feedback')
            
            for feedback in feedbacks:
                index_span = feedback.find('span', class_='q-index')
                answer_span = feedback.find('span', class_='q-answer')
                
                if index_span and answer_span:
                    # Remove o ":" do índice
                    indice = index_span.get_text(strip=True).replace(':', '')
                    resposta = answer_span.get_text(strip=True)
                    gabarito[indice] = resposta
            
            print(f"✓ Gabarito extraído: {len(gabarito)} respostas")
    except Exception as e:
        print(f"✗ Erro ao extrair gabarito: {e}")
    
    return gabarito

def extrair_questoes_pagina(driver):
    """Extrai todas as questões da página atual"""
    questoes_data = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Extrai o gabarito da página
    gabarito = extrair_gabarito_pagina(soup)
    
    # Encontra todas as questões
    questoes = soup.find_all('div', class_='js-question-item')
    
    print(f"Encontradas {len(questoes)} questões na página atual")
    
    for questao in questoes:
        try:
            question_div = questao.find('div', class_='js-question')
            question_id = question_div.get('data-question-id', 'N/A') if question_div else 'N/A'
            
            # Pega o índice da questão na página
            q_index_span = questao.find('span', class_='q-index')
            indice_questao = q_index_span.get_text(strip=True) if q_index_span else None
            
            # Busca a resposta correta
            resposta_correta = 'N/A'
            
            # Método 1: Pelo gabarito da página
            if indice_questao and indice_questao in gabarito:
                resposta_correta = gabarito[indice_questao]
            
            # Método 2: Busca dentro da própria questão (algumas páginas mostram assim)
            if resposta_correta == 'N/A':
                try:
                    # Procura por span com classe js-question-right-answer
                    right_answer = questao.find('span', class_='js-question-right-answer')
                    if right_answer:
                        resposta_correta = right_answer.get_text(strip=True)
                except:
                    pass
            
            # Método 3: Procura por alternativa marcada como correta
            if resposta_correta == 'N/A':
                try:
                    alternativa_correta = questao.find('label', class_='q-correct')
                    if alternativa_correta:
                        input_radio = alternativa_correta.find('input', type='radio')
                        if input_radio:
                            resposta_correta = input_radio.get('value', 'N/A')
                except:
                    pass
            
            enunciado_div = questao.find('div', class_='q-question-enunciation')
            enunciado = enunciado_div.get_text(strip=True) if enunciado_div else 'N/A'
            
            info_div = questao.find('div', class_='q-question-info')
            ano = 'N/A'
            banca = 'N/A'
            orgao = 'N/A'
            prova = 'N/A'
            
            if info_div:
                spans = info_div.find_all('span')
                for span in spans:
                    texto = span.get_text(strip=True)
                    if 'Ano:' in texto:
                        ano = texto.replace('Ano:', '').strip()
                    elif 'Banca:' in texto:
                        banca_link = span.find('a')
                        if banca_link:
                            banca = banca_link.get_text(strip=True)
                    elif 'Órgão:' in texto:
                        orgao_link = span.find('a')
                        if orgao_link:
                            orgao = orgao_link.get_text(strip=True)
                    elif 'Prova:' in texto:
                        prova_link = span.find('a')
                        if prova_link:
                            prova = prova_link.get_text(strip=True)
            
            breadcrumb = questao.find('div', class_='q-question-breadcrumb')
            disciplina = 'N/A'
            assunto = 'N/A'
            
            if breadcrumb:
                links = breadcrumb.find_all('a')
                if len(links) > 0:
                    disciplina = links[0].get_text(strip=True)
                if len(links) > 1:
                    assunto = links[1].get_text(strip=True)
            
            alternativas_dict = {}
            opcoes_div = questao.find('div', class_='q-question-options')
            
            if opcoes_div:
                labels = opcoes_div.find_all('label', class_='q-radio-button')
                
                for label in labels:
                    input_radio = label.find('input', type='radio')
                    letra = input_radio.get('value', '') if input_radio else ''
                    
                    alternativa_div = label.find('div', class_='q-item-enum')
                    texto_alternativa = alternativa_div.get_text(strip=True) if alternativa_div else ''
                    
                    if letra:
                        alternativas_dict[f'Alternativa_{letra}'] = texto_alternativa
            
            dados_questao = {
                'ID_Questao': f'Q{question_id}',
                'Indice_Pagina': indice_questao if indice_questao else 'N/A',
                'Ano': ano,
                'Banca': banca,
                'Orgao': orgao,
                'Prova': prova,
                'Disciplina': disciplina,
                'Assunto': assunto,
                'Enunciado': enunciado,
                'Resposta_Correta': resposta_correta,
            }
            
            dados_questao.update(alternativas_dict)
            questoes_data.append(dados_questao)
            
            print(f"✓ Questão {question_id} extraída | Resposta: {resposta_correta}")
            
        except Exception as e:
            print(f"✗ Erro ao extrair questão: {e}")
            continue
    
    return questoes_data

def listar_cadernos_disponiveis(driver):
    """Lista todos os cadernos disponíveis na página"""
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
                        texto_subtitle = subtitle.get_text(strip=True)
                        match_questoes = re.search(r'(\d+)\s+questões', texto_subtitle)
                        if match_questoes:
                            num_questoes = match_questoes.group(1)
                
                cadernos.append({
                    'id': caderno_id,
                    'nome': nome,
                    'href': href,
                    'num_questoes': num_questoes
                })
        except Exception as e:
            continue
    
    cadernos_unicos = []
    ids_vistos = set()
    for caderno in cadernos:
        if caderno['id'] not in ids_vistos:
            cadernos_unicos.append(caderno)
            ids_vistos.add(caderno['id'])
    
    return cadernos_unicos

def extrair_caderno(driver, caderno_info, wait):
    """Extrai todas as questões de um caderno específico"""
    print(f"\n{'='*60}")
    print(f"📚 CADERNO: {caderno_info['nome']}")
    print(f"📊 Questões: {caderno_info['num_questoes']}")
    print(f"🔑 ID: {caderno_info['id']}")
    print(f"{'='*60}\n")
    
    todas_questoes = []
    
    try:
        caderno_clicado = False
        
        try:
            link_caderno = driver.find_element(By.XPATH, f"//a[contains(@href, '{caderno_info['id']}')]")
            driver.execute_script("arguments[0].scrollIntoView(true);", link_caderno)
            sleep(0.5)
            driver.execute_script("arguments[0].click();", link_caderno)
            print(f"✓ Clicou no caderno (por ID)")
            caderno_clicado = True
        except:
            pass
        
        if not caderno_clicado:
            try:
                link_caderno = driver.find_element(By.XPATH, f"//a[contains(text(), '{caderno_info['nome']}')]")
                driver.execute_script("arguments[0].scrollIntoView(true);", link_caderno)
                sleep(0.5)
                driver.execute_script("arguments[0].click();", link_caderno)
                print(f"✓ Clicou no caderno (por nome)")
                caderno_clicado = True
            except:
                pass
        
        if not caderno_clicado:
            print(f"✗ Não foi possível clicar no caderno")
            return None
        
        sleep(5)
        print("✓ Caderno aberto! Iniciando extração...")
        
        pagina_atual = 1
        
        while True:
            print(f"\n--- Extraindo página {pagina_atual} ---")
            sleep(3)
            
            questoes_pagina = extrair_questoes_pagina(driver)
            todas_questoes.extend(questoes_pagina)
            
            print(f"Total extraído: {len(todas_questoes)} questões")
            
            try:
                botao_proxima = driver.find_element(By.XPATH, "//a[@rel='next' or contains(@class, 'next')]")
                
                if 'disabled' in botao_proxima.get_attribute('class'):
                    print("\n✓ Última página alcançada")
                    break
                
                driver.execute_script("arguments[0].scrollIntoView(true);", botao_proxima)
                sleep(1)
                driver.execute_script("arguments[0].click();", botao_proxima)
                print(f"✓ Indo para página {pagina_atual + 1}")
                pagina_atual += 1
                
            except:
                print("\n✓ Não há mais páginas")
                break
        
        return todas_questoes
        
    except Exception as e:
        print(f"✗ Erro ao extrair caderno: {e}")
        return None

def salvar_dataframe(questoes, nome_caderno):
    """Salva as questões em um arquivo Excel"""
    if not questoes:
        print("✗ Nenhuma questão para salvar")
        return
    
    df = pd.DataFrame(questoes)
    
    colunas_ordenadas = ['ID_Questao', 'Indice_Pagina', 'Ano', 'Banca', 'Orgao', 'Prova', 
                       'Disciplina', 'Assunto', 'Enunciado', 'Resposta_Correta']
    
    colunas_alternativas = [col for col in df.columns if col.startswith('Alternativa_')]
    colunas_alternativas.sort()
    
    colunas_final = colunas_ordenadas + colunas_alternativas
    
    for col in colunas_final:
        if col not in df.columns:
            df[col] = ''
    
    df = df[colunas_final]
    
    nome_arquivo = f"questoes_{nome_caderno.replace(' ', '_').replace('-', '_')}.xlsx"
    nome_arquivo = re.sub(r'[^\w\-_\.]', '', nome_arquivo)
    
    df.to_excel(nome_arquivo, index=False, engine='openpyxl')
    print(f"\n✅ Arquivo salvo: {nome_arquivo}")
    print(f"📊 Total de questões: {len(df)}")
    
    # Estatísticas de respostas
    respostas_encontradas = df[df['Resposta_Correta'] != 'N/A'].shape[0]
    print(f"✅ Respostas corretas encontradas: {respostas_encontradas}/{len(df)}")
    
    return df

def run_chrome_headless():
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    
    service = Service()
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.get("https://www.qconcursos.com/")
        print("Site carregado")
        sleep(3)
        
        wait = WebDriverWait(driver, 15)
        
        # 1. Entrar
        try:
            botao_entrar = driver.find_element(By.CSS_SELECTOR, "button.text-gray-800.font-semibold")
            driver.execute_script("arguments[0].click();", botao_entrar)
            print("✓ Clicou em Entrar")
        except:
            botoes = driver.find_elements(By.TAG_NAME, "button")
            for botao in botoes:
                if "Entrar" in botao.text:
                    driver.execute_script("arguments[0].click();", botao)
                    print("✓ Clicou em Entrar (alternativo)")
                    break
        
        sleep(3)
        
        # 2. Google
        clicou = False
        for tentativa in range(3):
            try:
                if tentativa == 0:
                    botao_google = driver.find_element(By.CSS_SELECTOR, "button[q\\:id='aj']")
                elif tentativa == 1:
                    botao_google = driver.find_element(By.XPATH, "//button[.//span[contains(text(), 'Continuar com Google')]]")
                else:
                    botoes = driver.find_elements(By.TAG_NAME, "button")
                    for botao in botoes:
                        if "Google" in botao.text:
                            botao_google = botao
                            break
                
                driver.execute_script("arguments[0].scrollIntoView(true);", botao_google)
                sleep(0.5)
                driver.execute_script("arguments[0].click();", botao_google)
                print("✓ Clicou em Continuar com Google")
                clicou = True
                break
            except:
                continue
        
        sleep(4)
        
        # 3. Email
        try:
            campo_email = wait.until(EC.presence_of_element_located((By.ID, "identifierId")))
            campo_email.clear()
            campo_email.send_keys("welligtoncos@gmail.com")
            print("✓ Email preenchido")
            
            sleep(2)
            botao_proximo = driver.find_element(By.ID, "identifierNext")
            driver.execute_script("arguments[0].click();", botao_proximo)
            print("✓ Próxima (email)")
        except Exception as e:
            print(f"✗ Erro no email: {e}")
        
        sleep(4)
        
        # 4. Senha
        try:
            campo_senha = wait.until(EC.presence_of_element_located((By.NAME, "Passwd")))
            campo_senha.clear()
            campo_senha.send_keys("@Well32213115")
            print("✓ Senha preenchida")
            
            sleep(2)
            botao_login = driver.find_element(By.ID, "passwordNext")
            driver.execute_script("arguments[0].click();", botao_login)
            print("✓ Login concluído")
        except Exception as e:
            print(f"✗ Erro na senha: {e}")
        
        sleep(8)
        
        # 5. Ícone usuário
        try:
            try:
                icone_usuario = driver.find_element(By.CSS_SELECTOR, "div.size-\\[32px\\].rounded-full.bg-gray-100")
            except:
                icone_usuario = driver.find_element(By.XPATH, "//div[contains(@class, 'rounded-full')]//svg")
                icone_usuario = icone_usuario.find_element(By.XPATH, "./..")
            
            driver.execute_script("arguments[0].scrollIntoView(true);", icone_usuario)
            sleep(1)
            driver.execute_script("arguments[0].click();", icone_usuario)
            print("✓ Ícone do usuário")
        except Exception as e:
            print(f"✗ Erro no ícone: {e}")
        
        sleep(2)
        
        # 6. Meu Painel
        try:
            meu_painel = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Meu Painel')]")))
            driver.execute_script("arguments[0].click();", meu_painel)
            print("✓ Meu Painel")
        except:
            links = driver.find_elements(By.TAG_NAME, "a")
            for link in links:
                if "Meu Painel" in link.text:
                    driver.execute_script("arguments[0].click();", link)
                    print("✓ Meu Painel (alt)")
                    break
        
        sleep(3)
        
        # 7. Meus cadernos
        try:
            cadernos_link = driver.find_element(By.XPATH, "//a[.//span[contains(text(), 'Meus cadernos')]]")
            driver.execute_script("arguments[0].scrollIntoView(true);", cadernos_link)
            sleep(0.5)
            driver.execute_script("arguments[0].click();", cadernos_link)
            print("✓ Meus cadernos")
        except:
            print("✗ Erro ao acessar Meus cadernos")
        
        sleep(3)
        
        cadernos_disponiveis = listar_cadernos_disponiveis(driver)
        
        if not cadernos_disponiveis:
            print("✗ Nenhum caderno encontrado!")
            return
        
        print(f"\n{'='*70}")
        print("📚 CADERNOS DISPONÍVEIS:")
        print(f"{'='*70}")
        for idx, caderno in enumerate(cadernos_disponiveis, 1):
            print(f"{idx}. {caderno['nome']}")
            print(f"   ID: {caderno['id']} | Questões: {caderno['num_questoes']}")
            print()
        
        print(f"{'='*70}")
        print("OPÇÕES:")
        print("  - Digite o NÚMERO do caderno para extrair")
        print("  - Digite 'TODOS' para extrair todos os cadernos")
        print("  - Digite vários números separados por vírgula (ex: 1,3,5)")
        print(f"{'='*70}\n")
        
        escolha = input("Sua escolha: ").strip()
        
        cadernos_para_extrair = []
        
        if escolha.upper() == 'TODOS':
            cadernos_para_extrair = cadernos_disponiveis
            print("\n✓ Extraindo TODOS os cadernos!\n")
        elif ',' in escolha:
            indices = [int(x.strip()) for x in escolha.split(',')]
            cadernos_para_extrair = [cadernos_disponiveis[i-1] for i in indices if 1 <= i <= len(cadernos_disponiveis)]
            print(f"\n✓ Extraindo {len(cadernos_para_extrair)} cadernos!\n")
        else:
            try:
                idx = int(escolha)
                if 1 <= idx <= len(cadernos_disponiveis):
                    cadernos_para_extrair = [cadernos_disponiveis[idx-1]]
                    print(f"\n✓ Extraindo: {cadernos_para_extrair[0]['nome']}\n")
            except:
                print("✗ Escolha inválida!")
                return
        
        todos_dataframes = {}
        
        for caderno_info in cadernos_para_extrair:
            driver.get("https://www.qconcursos.com/usuario/cadernos")
            sleep(3)
            
            questoes = extrair_caderno(driver, caderno_info, wait)
            
            if questoes:
                df = salvar_dataframe(questoes, caderno_info['nome'])
                todos_dataframes[caderno_info['nome']] = df
        
        print(f"\n\n{'='*70}")
        print("🎉 EXTRAÇÃO CONCLUÍDA!")
        print(f"{'='*70}")
        print(f"✅ Cadernos extraídos: {len(todos_dataframes)}")
        print(f"📊 Total de questões: {sum(len(df) for df in todos_dataframes.values())}")
        print(f"{'='*70}\n")
        
    except Exception as e:
        print(f"Erro geral: {e}")
        driver.save_screenshot("erro.png")
    
    finally:
        input("\nPressione Enter para fechar...")
        driver.quit()

if __name__ == "__main__":
    run_chrome_headless()