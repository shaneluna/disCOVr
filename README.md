# disCOVr

## Data
- [ReCOVery](https://github.com/apurvamulay/ReCOVery)

The ReCOVery dataset is the springboard for the remainder of our project. Tweets and articles that are referenced or used later on in this project all stem from here. Transformations (via APIs) are applied to the tweet ids and article text / metadata to then generate additional properties and datasets.

## Getting Started

1. PySpark will require Java 8 or later with the JAVA_HOME variable set. Download and install Java if missing.

2. It's recommended to create a new python env; install the requirements.txt in your python environment.<br>
`pip install -r requirements.txt`

3. Create a copies of the `configexample.yaml` and `discovr-2021-c63b2ee5e9f6example.json` files, rename to `config.yaml` and `discovr-2021-c63b2ee5e9f6.json`, respectively, and update with your config info.

4. Download both csv files located at path ['ReCOVery/dataset'](https://github.com/apurvamulay/ReCOVery/tree/master/dataset) of the ReCOVery GitHub repository and store in path 'disCOVr/data' of the disCOVr repo.

5. TweetHydratorAPI.ipynb 