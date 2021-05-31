# IMDBparser
IMDB parser with MongoDB for course project

For starting our project you need to launch next command in the main project folder:

docker-compose --verbose -f ./docker-compose.yml run --service-ports imdb

After docker starts you should run update_data.py and main.py to start dash server with IMDB data.