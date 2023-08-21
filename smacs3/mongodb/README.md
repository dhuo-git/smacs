#start a mongdb server:
docker run --rm --name mongo6-jammy -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=VRuAd2Nvmp4ELHh5 -e MONGO_INITDB_DATABASE=test -v /tmp/mongo-data:/data/db mongo:6-jammy

#docker exec -it mongo6-jammy mongosh -u admin -p VRuAd2Nvmp4ELHh5 --authenticationDatabase admin
(get container identifer via docker ps)

docker exec -it a6c7 mongosh 

