docker run -itd --network apertif --name arts041 -h arts041 -p 8080:80 arts 
docker run -itd --network apertif --name arts001 -h arts001 arts 
docker run -itd --network apertif --name control -h control arts
