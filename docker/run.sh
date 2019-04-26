docker run -itd --name master -h master -p 8080:80 arts 
docker run -itd --name worker -h worker arts 
docker run -itd --name control -h control arts
