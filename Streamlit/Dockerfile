FROM python:3.10.12 
# Enter your own python Environment
WORKDIR  /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY main.py ./
# COPY search.py ./

# COPY .env ./
# copy Pipfile Pipfile.lock ./
# RUN pipenv install --system --deploy --ignore-pipfile
### Container Env same as local at this point
EXPOSE 8090
CMD ["streamlit", "run", "main.py", "--server.port", "8090"]