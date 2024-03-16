FROM python:3.8.2-buster
# COPY . /usr/src/app

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

# Install PYTHON requirements
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# START WEBAPP SERVICE
# CMD [ "python", "/usr/src/app/main.py" ]

WORKDIR /app
