cd dc_rest_api;
docker build --no-cache -t docker.leibniz-lib.de:5000/dc_rest_api:latest .; docker push docker.leibniz-lib.de:5000/dc_rest_api:latest;
cd ..;
docker stack rm dc_rest_api;
docker stack deploy --with-registry-auth -c docker-compose-swarm.yml dc_rest_api;


