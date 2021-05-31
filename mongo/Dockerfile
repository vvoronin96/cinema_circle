FROM mongo:4.4.6

# install Python 3
RUN apt-get update && apt-get install -y python3 python3-pip
RUN apt-get -y install python3.7-dev
COPY . .
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 27017