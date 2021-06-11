FROM python:3.7.10

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
#RUN apt-get install -y vim
COPY . .

#RUN python3 copyDataBase.py

EXPOSE 8050

CMD ["bash"]



