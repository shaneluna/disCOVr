# disCOVr

## Data
- [ReCOVery](https://github.com/apurvamulay/ReCOVery)

The ReCOVery dataset is the springboard for the remainder of our project. Tweets and articles that are referenced or used later on in this project all stem from here. Transformations (via APIs) are applied to the tweet ids and article text / metadata to then generate additional properties and datasets.

## Getting Started

1. PySpark will require Java 8 or later with the JAVA_HOME variable set. Download and install Java if missing.

2. It's recommended to create and activate a new python env; install the requirements.txt in your python environment.<br>
`pip install -r requirements.txt`

3. In the `secrets` folder,  create copies of the `configexample.yaml` and `discovr-2021-c63b2ee5e9f6example.json` files, rename to `config.yaml` and `discovr-2021-c63b2ee5e9f6.json`, respectively, and update each of the files with your personal config info. `config.yaml` will contain Twitter API and Neo4j config info. `discovr-2021-c63b2ee5e9f6example.json` will contain Google API config info. 

4. Download both csv files located at path ['ReCOVery/dataset'](https://github.com/apurvamulay/ReCOVery/tree/master/dataset) of the ReCOVery GitHub repository and store in path `data/reCOVery` of the disCOVr repo.

5. Run each of the cells in the `TweetHydrator.ipynb` notebook. The output will be json files of hydrated tweets in the `data/tweets` folder. There should be about roughly 1400 json files. 

6. Run each of the cells in the `GoogleAPI.ipynb` notebook. The output will be 3 csv files in the `data\news_topics` folder: `news_categories.csv`, `news_entities.csv`, and `news_sentiments.csv`. 

7. Run each of the cells in the `FakeboxAPI.ipynb` notebook. The notebook connects to a self-hosted Docker container that runs the bias detection model. The output will be the `news_biases.csv` file in the `data\news_biases` folder. The entire run takes north of five hours, so plan accordingly. 

8. Run each of the cells in the `Neo4jImporter.ipynb` notebook. This notebook will automate the data import process for Neo4j. The data in the json files and the csv files generated in the previous steps will be parsed and loaded into Neo4j.<br>
Be sure to read the Notes section at the top of the `Neo4jImporter.ipynb` notebook for properly installing and setting up APOC for your Neo4j installation. The assumption is that the Neo4j Desktop Application has already been installed on the user's machine. Below are the same notes for immediate reference. 

	### Neo4j Notes
	1. Install APOC (https://neo4j.com/developer/neo4j-apoc/)
	- From neo4j desktop, select db > plugins on right > apoc > install and restart
	- In settings, add the following line to the bottom: apoc.import.file.enabled=true

	2. The below uses your default neo4j import location.<br>
	- Your default neo4j import location may vary by operating system (https://neo4j.com/developer/guide-import-csv/)
	- Linux / macOS / Docker - <neo4j-home>/import<br>
	- Windows - <neo4j-home>/import<br>
	- Debian / RPM - /var/lib/neo4j/import<br>

	If desired, you can modify acceptable import locations.

	### Additional Notes
	If the user is using a Docker container, the user can use `docker/docker-compose.yml` for configuration. The personal config alterations will need to be made to the `docker-compose.yml`
