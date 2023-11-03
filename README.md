# Urban Genome

(The pilot study tools)


## Neo4j Development Server - Docker

To launch an instance of the development server with the correct runtime settings, you can use `docker-compose up` in this directory.

This will launch an instance of Neo4j on your local computer, create a `./data/neo4j/` directory to store the database and a `./data/neo4j-plugins/` directory for the required `APOC` and `APOC-Extended` plugins.

This instance should be accessible at `http://localhost:4747/` and will use the username `neo4j` and the password `demoServer`.