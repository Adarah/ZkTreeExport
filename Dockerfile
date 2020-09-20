FROM python:3.7-slim
COPY . /app
RUN python3 -m pip install -r app/requirements.txt
ENTRYPOINT ["python3", "app/src/main.py"]
