# Distributed Data Analysis System

A Dockerized system for distributing data analysis tasks using RabbitMQ, designed to handle a specific problem as outlined here: https://github.com/atlas-outreach-data-tools/notebooks-collection-opendata/blob/master/13-TeV-examples/uproot_python/HyyAnalysisNew.ipynb.

The system is one GitHub repository that includes all files needed, all that you need to run is to have Docker installed and open in the background (https://docs.docker.com/engine/install/).

### Instructions:
1) Clone the repository:

git clone 
cd Assessment

2) Download data:

python downloadData.py

2) Build the system:

docker-compose build

3) Run system with desired number of workers:

docker-compose up --scale worker=N

4) After completion, check the /aggregatorOutput folder for the output.png plto. The aggregator will be the only container running. Close it if you wish with Ctrl+C.
