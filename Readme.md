# Meu RPA - Timesheet Block

 

## Pré-requisitos

Certifique-se de ter os seguintes requisitos instalados:

- **Python 3.8+**
- **Docker**
- **Git** (opcional, para controle de versão)

## Configuração do Ambiente

1. **Criação de um ambiente virtual**
```bash
   python -m venv venv
```

2. **Criação de um ambiente virtual**
```bash
   python -m venv venv
```

3. **Atualizar o pip**
```bash
   pip install --upgrade pip
```

4. **Instalar as dependências**
```bash
   pip install -r requirements.txt
```

5. **Gerar o arquivo requirements.txt atualizado Caso você adicione novas dependências, use o comando abaixo para atualizar o arquivo requirements.txt:**
```bash
   pip freeze > requirements.txt
```
6. **Ativar o ambiente virtual**

```bash
   source venv/bin/activate

   venv\Scripts\activate
```

## Construção da Imagem Docker

1. **Construir a imagem**
```bash
   docker build -t meu_rpa:latest .
```

2. **Execução da imagem**

/mnt/c/rsm-projects/Pollvo.RPA.Timesheet.Block/screenshots

```bash
docker run --rm -it -v /mnt/c/rsm-projects/Pollvo.RPA.Timesheet.Block/screenshots:/app/downloads/Timesheet/screenshots meu_rpa:latest
``` 

 

Liste de forma clara os aspectos principais que os apresentadores de IA devem explicar neste episódio.
Foque nos pontos essenciais, como: o que está sendo ensinado, o objetivo da tarefa, os passos importantes, os erros comuns, e o que o aluno deve aprender no final.
Explique sempre de maneira simples e fácil de entender.