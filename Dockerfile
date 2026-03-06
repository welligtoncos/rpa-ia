# Usa a imagem oficial do Python 3.12
FROM python:3.12

# Cria a pasta de trabalho
WORKDIR /app

# Cria pasta para armazenar screenshots (opcional)
RUN mkdir -p /app/downloads/Timesheet/screenshots

# Copia o arquivo de requisitos
COPY requirements.txt .

# Instala pacotes básicos + Google Chrome
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && curl -LO https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm -rf /var/lib/apt/lists/* ./google-chrome-stable_current_amd64.deb


# Instala as bibliotecas Python do requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Copia o restante do código para /app
COPY . .

# Define qual script será executado quando o contêiner subir
CMD ["python", "./main_blockTimeSheet.py"]
