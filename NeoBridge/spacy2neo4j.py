#!/usr/bin/env python3

from email.policy import default
from optparse import OptionParser
from neo4j import GraphDatabase
from NeoBridge import NeoBridge
from loguru import logger
import xml.etree.ElementTree as ET
import spacy
from spacy import displacy
from spacy import tokenizer
import os
import sys

parser = OptionParser()
parser.add_option( "--xml", dest="inputXML", help="Read from a source .xml", metavar="FILE", default=None )
parser.add_option( "--txt", dest="inputTXT", help="Read from a source .txt", metavar="FILE", default=None )
parser.add_option( "--model", dest="spacy_data", help="Set the spacy training data to use", default="en_core_web_lg" )
parser.add_option( "--tokens", dest="doTokens", help="Insert tokens into the database", default=False, action="store_true" )
parser.add_option( "--entities", dest="doEntities", help="Insert entity spans into the database", default=False, action="store_true" )
parser.add_option( "--pymusas", dest="doPymusas", help="Include pymusas annotations", default=False, action="store_true" )
parser.add_option( "--pymusas-model", dest="pymusas_data", help="Set the pymusas model to use", default="en_dual_none_contextual" )

class Spacy2Neo4j(NeoBridge):
    
    def __init__( self ):
        super().__init__( name="spacy2neo4j", version="1.1.0" )

        with self.driver.session() as session:
            session.run( "CREATE INDEX tokenIndex IF NOT EXISTS FOR (t:Token) ON (t.index, t.paragraph)" )
            session.run( "CREATE INDEX tagIndex IF NOT EXISTS FOR (t:Tag) ON (t.type)" )
        
        self.addIndexedUUID( "Token" )
        self.addIndexedUUID( "Source" )
        self.addIndexedUUID( "Entity" )

    def update_source( self, title, url ):
        with self.driver.session() as session:
            session.run( "MERGE (n:Source {title: $t, url: $u})", t=title, u=url )
            res = session.run( "MATCH (n:Source {title: $t, url: $u}) RETURN n.uuid", t=title, u=url )
            return res.single()[0]
    
    def update_entity( self, srcUUID, paraIndex, entity):
        with self.driver.session() as session:
            session.run( "MERGE (e:Entity {text: $text, type: $type})", text = entity.text, type = entity.label_ )
            res = session.run( "MATCH (e:Entity {text: $text, type: $type}) RETURN e.uuid", text = entity.text, type = entity.label_ )
            eUUID = res.single()[0]
            session.close()

        for tokID in range(entity.start, entity.end):
            logger.debug( f"\t- Map {entity.label_} To {paraIndex}/{tokID} in {srcUUID}" )
            with self.driver.session() as session:
                session.run(
                    "MATCH (e:Entity {uuid:$uuid}), (t:Token {source: $source, paragraph: $iPara, index: $iToken}) MERGE (t)-[:Is]->(e)",
                    uuid = eUUID,
                    source = srcUUID,
                    iPara = paraIndex,
                    iToken = tokID
                )
                session.close()


    def update_token( self, srcUUID, paraIndex, token ):
        with self.driver.session() as session:
            session.run(
                "MERGE (t:Token {text: $text, paragraph: $iPara, index: $iToken, norm: $norm, language: $lang, source: $uuid})",
                uuid = srcUUID,
                text = token.text,
                iPara = paraIndex,
                iToken = token.i,
                norm = token.norm_,
                lang = token.lang_
            )
            res = session.run(
                "MATCH (n:Token {text: $text, paragraph: $iPara, index: $iToken, norm: $norm, language: $lang, source: $uuid}) RETURN n.uuid",
                uuid = srcUUID,
                text = token.text,
                iPara = paraIndex,
                iToken = token.i,
                norm = token.norm_,
                lang = token.lang_
            )
            tokUUID = res.single()[0]

            session.run( "MERGE (l:Lemma {text: $lemma, language: $lang})", lemma=token.lemma_, lang = token.lang_ )
            session.run(
                "MATCH (tok:Token {uuid: $tok}), (prop:Lemma {text: $lemma, language: $lang}) MERGE (tok)-[:Is]->(prop)",
                tok = tokUUID,
                lemma = token.lemma_,
                lang = token.lang_
            )
            

            # Add a tag value for this token
            session.run( "MERGE (tag:Tag {class: 'fine', type: $tokType})", tokType=token.tag_ )
            session.run(
                "MATCH (tok:Token {uuid: $tok}), (tag:Tag {class: 'fine', type: $tokType}) MERGE (tok)-[:Tagged]->(tag)",
                tok = tokUUID,
                tokType = token.tag_
            )

            session.run( "MERGE (tag:Tag {class: 'coarse', type: $posType})", posType=token.pos_ )
            session.run(
                "MATCH (tok:Token {uuid: $tok}), (tag:Tag {class: 'coarse', type: $posType}) MERGE (tok)-[:Tagged]->(tag)",
                tok = tokUUID,
                posType = token.pos_
            )

            # Dependencies have spans, this needs to be a group merge!
            #session.run( "MERGE (dep:Dependency {type: $depType})", depType=token.dep_ )
            #session.run(
            #    "MATCH (tok:Token {paragraph: $iPara, index: $iToken, source: $uuid}), (dep:Dependency {type: $depType}) MERGE (tok)<-[:DependsOn]-(dep)",
            #    uuid = srcUUID,
            #    iPara = paraIndex,
            #    iToken = token.i,
            #    depType = token.dep_
            #)

            if token.cluster != 0:
                session.run( "MERGE (grp:Cluster {id: $id, source: $uuid})", id=token.cluster, uuid = srcUUID )
                session.run(
                    "MATCH (tok:Token {uuid: $tok}), (grp:Cluster {id: $id, source: $uuid}) MERGE (tok)-[:PartOf]->(grp)",
                    tok = tokUUID,
                    uuid = srcUUID,
                    id = token.cluster
                )

            # Link to the previous node in the series, if present
            session.run(
                "MATCH (t:Token {uuid: $tok}), (p:Token {paragraph: $iPara, index: $pToken, source: $uuid}) MERGE (p)-[:Next]->(t)",
                uuid = srcUUID,
                tok = tokUUID,
                iPara = paraIndex,
                pToken = token.i - 1
            )

            ### PyMUSAS extra tags ###
            if token._.pymusas_tags:
                for pTag in token._.pymusas_tags:
                    sys.stdout.write( f"[{pTag}] " )
                    session.run( "MERGE (tag:Tag {class: 'pymusas', type: $tagType})", tagType=pTag )
                    session.run(
                        "MATCH (tok:Token {uuid: $tok}), (tag:Tag {class: 'pymusas', type: $tagType}) MERGE (tok)-[:Tagged]->(tag)",
                        tok = tokUUID,
                        tagType = pTag
                    )

            ### ================== ###

            # If we have an entity type, add in a link for this too
            #if token.ent_type_ != "" and (token.ent_iob == 3 or token.ent_iob == 1):
            #    session.run( "MERGE (ent:Entity {type: $entType})", entType=token.ent_type_ )
            #    session.run(
            #        "MATCH (tok:Token {paragraph: $iPara, index: $iToken, source: $uuid}), (ent:Entity {type: $entType}) MERGE (tok)<-[:Is]-(ent)",
            #        uuid = srcUUID,
            #        iPara = paraIndex,
            #        iToken = token.i,
            #        entType = token.ent_type_
            #    )

            return tokUUID
    
    

    #def print_greeting(self, message):
    #    with self.driver.session() as session:
    #        greeting = session.execute_write(self._create_and_return_greeting, message)
    #        print(greeting)

    #@staticmethod
    #def _create_and_return_greeting(tx, message):
    #    result = tx.run("CREATE (a:Greeting) "
    #                    "SET a.message = $message "
    #                    "RETURN a.message + ', from node ' + id(a)", message=message)
    #    return result.single()[0]

if __name__ == "__main__":
    (options, args) = parser.parse_args()

    if spacy.prefer_gpu():
        logger.warning( "Using GPU compute!" )

    # Default 'bare' Spacy 
    nlp = spacy.load( options.spacy_data )

    if options.doPymusas:
        logger.info( f"Using PyMUSAS with model: {options.pymusas_data}" )
        nlp = spacy.load( options.spacy_data, exclude=['parser', 'ner'] )

        # Load the English PyMUSAS rule-based tagger in a separate spaCy pipeline
        english_tagger_pipeline = spacy.load( options.pymusas_data )

        # Adds the English PyMUSAS rule-based tagger to the main spaCy pipeline
        nlp.add_pipe( 'pymusas_rule_based_tagger', source=english_tagger_pipeline )
    

    db = Spacy2Neo4j()

    if options.inputXML != None:
        with open( options.inputXML, mode='r' ) as sourceXML:
            xml = ET.fromstring( sourceXML.read() )

            for source in xml.findall( 'source' ):
                title = source.findtext( 'title' )
                url = source.findtext( 'url' )

                logger.info( f"Processing XML {sourceXML.name} - {title}: {url}" )

                srcUUID = db.update_source( title, url )

                paraIndex = -1
                for para in source.findall('para'):
                    paraIndex = paraIndex + 1
                    logger.info( f"Parsing paragraph: {paraIndex}" )
                    parsed = nlp( para.text.replace("[","").replace("]","") )

                    if options.doTokens:
                        for tok in parsed:
                            sys.stdout.write( f"{tok.text}  " )
                            sys.stdout.flush()
                            
                            db.update_token( srcUUID, paraIndex, tok )
                        sys.stdout.write( "\n" )
                        logger.info( "EOL" )

                    if options.doEntities:
                        for ent in parsed.ents:
                            db.update_entity( srcUUID, paraIndex, ent )
    
    elif options.sourceTXT != None:
        logger.warning( "NOTE: The text parser has no notion of paragraphs, so will use the entire document as paragraph = 0" )
        with open( options.inputTXT, mode='r' ) as sourceTXT:
            title = os.path.basename( sourceTXT.name )
            url = sourceTXT.name

            logger.info( "Processing TXT...", title, url )

            srcUUID = db.update_source( title, url )
            
            parsed = nlp( sourceTXT.read() )

            if options.doTokens:
                paraIndex = 0
                for tok in parsed:
                    sys.stdout.write( f"{tok.text}  " )
                    sys.stdout.flush()
                    
                    db.update_token( srcUUID, paraIndex, tok )
                sys.stdout.write( "\n" )
                logger.info( "EOL" )

            if options.doEntities:
                for ent in parsed.ents:
                    db.update_entity( srcUUID, paraIndex, ent )
            
    db.close()