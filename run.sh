docker build -t dagster .
cd extractor
docker build -t extractor .
cd ..
cd transformation
docker build -t transformation .
docker image prune
cd ..
docker-compose up 
