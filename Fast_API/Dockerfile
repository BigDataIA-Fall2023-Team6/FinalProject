FROM python:3.10.12 
# Enter your own python Environment

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install uvicorn gunicorn 
RUN python -m spacy download en_core_web_md

COPY main.py ./
COPY chatbot.py ./

# copy Pipfile Pipfile.lock ./

# RUN pipenv install --system --deploy --ignore-pipfile

EXPOSE 8000

CMD [ "gunicorn","-w", "4","-k","uvicorn.workers.UvicornWorker","-b","0.0.0.0:8000", "main:app" ]

# # Use an official Python runtime as a parent image
# FROM python:3.10.12

# # Set the working directory in the container to /app
# WORKDIR /app

# # Copy the current directory contents into the container at /app
# COPY . /app

# # Install the required packages from requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# # Download the SpaCy model
# RUN python -m spacy download en_core_web_md

# # Make port 8000 available to the world outside this container
# EXPOSE 8000

# # Define environment variable
# ENV NAME World

# # Run the application
# CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8000", "main:app"]