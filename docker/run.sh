docker run -itd --network testing --name master -h master arts
docker run -itd --network testing --name worker -h worker arts
docker run -itd --network testing --name control -h control arts