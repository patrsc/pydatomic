# Installation

Start an instance of [MongoDB](https://www.mongodb.com/) using [Docker](https://www.docker.com/) (or use [other ways](https://www.mongodb.com/docs/manual/installation/) to install MongoDB):

```sh
docker run --name my-mongo -p 27017:27017 -v ~/mongodb-data:/data/db -d mongo:4.4-focal
```

Pydatomic requires Python 3.9 or above. You can install the package `pydatomic` using `pip`:

```sh
pip install pydatomic
```
