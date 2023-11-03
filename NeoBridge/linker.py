#!/usr/bin/env python3

from loguru import logger
from optparse import OptionParser
from NeoBridge import NeoBridge

parser = OptionParser()
parser.add_option( "-i", "--input", dest="input", help="Read from a source .csv", metavar="FILE" )

class CorpusLinker(NeoBridge):
    
    def __init__( self ):
        super().__init__( name="corpuslinker", version="1.0.0" )

    def tryLinking( self ):
        with self.driver.session() as session:
            # Attempt to match any existing name-entities in the database...
            logger.info( "Attempting to match any Place->Entity similarities..." )
            session.run( "MATCH (p:Place), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type <> 'PERSON' MERGE (e)-[:Is]->(p)" )
            session.run( "MATCH (p:Place), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type = 'PERSON' MERGE (e)-[:Homophone]->(p)" )
            session.close()
        
        with self.driver.session() as session:
            logger.info( "Attempting to match any Place->Lemma similarities..." )
            session.run( "MATCH (p:Place), (l:Lemma) WHERE toLower(l.text) = toLower(p.name) MERGE (l)-[:Matches]->(p)" )
            session.close()

        with self.driver.session() as session:
            logger.info( "Attempting to match any County->Entity similarities..." )
            session.run( "MATCH (p:County), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type <> 'PERSON' MERGE (e)-[:Is]->(p)" )
            session.run( "MATCH (p:County), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type = 'PERSON' MERGE (e)-[:Homophone]->(p)" )
            session.close()

        with self.driver.session() as session:
            logger.info( "Attempting to match any County->Lemma similarities..." )
            session.run( "MATCH (p:County), (l:Lemma) WHERE toLower(l.text) = toLower(p.name) MERGE (l)-[:Matches]->(p)" )
            session.close()

        with self.driver.session() as session:
            logger.info( "Attempting to match any LocalAuthorityDistrict->Entity similarities..." )
            session.run( "MATCH (p:LocalAuthorityDistrict), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type <> 'PERSON' MERGE (e)-[:Is]->(p)" )
            session.run( "MATCH (p:LocalAuthorityDistrict), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type = 'PERSON' MERGE (e)-[:Homophone]->(p)" )
            session.close()

        with self.driver.session() as session:
            logger.info( "Attempting to match any LocalAuthorityDistrict->Lemma similarities..." )
            session.run( "MATCH (p:LocalAuthorityDistrict), (l:Lemma) WHERE toLower(l.text) = toLower(p.name) MERGE (l)-[:Matches]->(p)" )
            session.close()

        with self.driver.session() as session:
            logger.info( "Attempting to match any PlaceNameDescriptor->Entity similarities..." )
            session.run( "MATCH (p:PlaceNameDescriptor), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type <> 'PERSON' MERGE (e)-[:Is]->(p)" )
            session.run( "MATCH (p:PlaceNameDescriptor), (e:Entity) WHERE toLower(e.text) = toLower(p.name) AND e.type = 'PERSON' MERGE (e)-[:Homophone]->(p)" )
            session.close()

        with self.driver.session() as session:
            logger.info( "Attempting to match any PlaceNameDescriptor->Lemma similarities..." )
            session.run( "MATCH (p:PlaceNameDescriptor), (l:Lemma) WHERE toLower(l.text) = toLower(p.name) MERGE (l)-[:Matches]->(p)" )
            session.close()

if __name__ == "__main__":
    (options, args) = parser.parse_args()

    db = CorpusLinker()
    db.tryLinking()
