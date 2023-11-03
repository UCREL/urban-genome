from loguru import logger
from neo4j import GraphDatabase
import os
import csv

DB_URI = os.environ.get( "DB_URI", "bolt://localhost:7687" )
DB_USER = os.environ.get( "DB_USER", "neo4j" )
DB_PASS = os.environ.get( "DB_PASS", "demoServer" )

class NeoBridge:
    def __init__(self, name = "unknown", version = "0.0.1", uri = DB_URI, user = DB_USER, password = DB_PASS):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.name = name
        self.version = version

        self.addIndexedUUID( "Toolchain" )

        with self.driver.session() as session:
            session.run( "CREATE INDEX toolName IF NOT EXISTS FOR (t:Toolchain) ON (t.name)" );
            session.run( "MERGE (n:Toolchain {name: $name, version: $version}) return n.uuid", name=self.name, version=self.version )
            res = session.run( "MATCH (n:Toolchain {name: $name, version: $version}) RETURN n.uuid", name=self.name, version=self.version )
            self.uuid = res.single()[0];
            logger.info( f"Toolchain UUID = {self.uuid}" )
            session.close();

    def addIndexedUUID( self, label ):
        cName = f"{label.lower()}UUID"

        logger.debug( f"Creating index, constraint and automatic UUID for {label} (cName = {cName})" )

        with self.driver.session() as session:
            session.run( f"CREATE CONSTRAINT {cName} IF NOT EXISTS FOR (n:{label}) REQUIRE n.uuid IS UNIQUE" )
            session.run( f"CALL apoc.uuid.install('{label}', {{addToExistingNodes: true, uuidProperty: 'uuid'}})" )
            session.close()

    def close(self):
        self.driver.close()