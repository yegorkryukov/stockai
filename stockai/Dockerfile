FROM python:3.8
EXPOSE 8501
WORKDIR /dockerbuild/
# COPY docker_requirements.txt ./docker_requirements.txt
COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt
COPY . ./
CMD streamlit run app.py

