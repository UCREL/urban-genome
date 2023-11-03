#!/usr/bin/env python3

from loguru import logger
from optparse import OptionParser
from neo4j import GraphDatabase
import os
import csv
from NeoBridge import NeoBridge

parser = OptionParser()
parser.add_option( "-i", "--input", dest="input", help="Read from a source .csv", metavar="FILE" )

class IPN2Neo4j(NeoBridge):
    
    def __init__( self ):
        super().__init__( name="ipn2neo4j", version="1.0.0" )

        with self.driver.session() as session:
            session.run( "CREATE INDEX placeIndex IF NOT EXISTS FOR (p:Place) ON (p.id)" )
        
        self.addIndexedUUID( "Place" )
        

    def updateRow( self, header, row ):
        logger.info( f"Adding {getByColumn( 'PLACEID', header, row )}..." )

        with self.driver.session() as session:
            # Update the base node
            session.run(
                "MERGE (:Place {type: $type, lat: $lat, lon: $lon, id: $id, code: $code, name: $name, toolchain: $uuid})",
                uuid=self.uuid,
                type=getByColumn( 'DESCNM', header, row ),
                lat=getByColumn( 'LAT', header, row ),
                lon=getByColumn( 'LONG', header, row ),
                id=getByColumn( 'PLACEID', header, row ),
                code=getByColumn( 'PLACE22CD', header, row ),
                name=getByColumn( 'PLACE22NM', header, row )
            )

            # DataSet meta element (for timestamping the geo data)
            session.run(
                "MERGE (:DataSet {year: $year, toolchain: $uuid})",
                uuid=self.uuid,
                year=2023,
                toolchain="ipn"
            )
            session.run(
                "MATCH (pnd:DataSet {year: $year, toolchain: $uuid}), (p:Place {id:$id, toolchain: $uuid}) MERGE (p)-[:PartOf]->(pnd)",
                uuid=self.uuid,
                id=getByColumn( 'PLACEID', header, row ),
                year=2023,
            )

            # Place Name Descriptor
            session.run(
                "MERGE (:PlaceNameDescriptor {code: $code, toolchain: $uuid})",
                uuid=self.uuid,
                code=getByColumn( 'DESCNM', header, row ),
            )
            session.run(
                "MATCH (pnd:PlaceNameDescriptor {code: $code, toolchain: $uuid}), (p:Place {id:$id, toolchain: $uuid}) MERGE (pnd)-[:Describes]->(p)",
                uuid=self.uuid,
                id=getByColumn( 'PLACEID', header, row ),
                code=getByColumn( 'DESCNM', header, row ),
            )

            # Country Name
            session.run(
                "MERGE (:Country {name: $name, toolchain: $uuid})",
                uuid=self.uuid,
                name=getByColumn( 'CTRY22NM', header, row ),
            )
            session.run(
                "MATCH (pnd:Country {name: $name, toolchain: $uuid}), (p:Place {id:$id, toolchain: $uuid}) MERGE (pnd)-[:Describes]->(p)",
                uuid=self.uuid,
                id=getByColumn( 'PLACEID', header, row ),
                name=getByColumn( 'CTRY22NM', header, row ),
            )

            # Historic county name
            if getByColumn( 'CTYHISTNM', header, row ) != "":
                self.addProperty(
                    _prop = "County",
                    _id   = getByColumn( 'PLACEID', header, row ),
                    _set  = "historic",
                    _name = getByColumn( 'CTYHISTNM', header, row )
                )

            # 1961 county name
            if getByColumn( 'CTY61NM', header, row ) != "":
                self.addProperty(
                    _prop = "County",
                    _id   = getByColumn( 'PLACEID', header, row ),
                    _set  = "1961",
                    _name = getByColumn( 'CTY61NM', header, row )
                )

            # 1991 county name
            if getByColumn( 'CTY91NM', header, row ) != "":
                self.addProperty(
                    _prop = "County",
                    _id   = getByColumn( 'PLACEID', header, row ),
                    _set  = "1991",
                    _name = getByColumn( 'CTY91NM', header, row )
                )

            # Lieutenancy County Name
            if getByColumn( 'CTYLTNM', header, row ) != "":
                self.addProperty(
                    _prop = "County",
                    _id   = getByColumn( 'PLACEID', header, row ),
                    _set  = "lieutenancy",
                    _name = getByColumn( 'CTYLTNM', header, row )
                )

            # 1961 Local Authority District Name
            if getByColumn( 'LAD61NM', header, row ) != "":
                self.addProperty(
                    _prop = "LocalAuthorityDistrict",
                    _id   = getByColumn( 'PLACEID', header, row ),
                    _set  = "1961",
                    _name = getByColumn( 'LAD61NM', header, row )
                )

            # 1991 Local Authority District Name
            if getByColumn( 'LAD91NM', header, row ) != "":
                self.addProperty(
                    _prop = "LocalAuthorityDistrict",
                    _id   = getByColumn( 'PLACEID', header, row ),
                    _set  = "1991",
                    _name = getByColumn( 'LAD91NM', header, row )
                )
    
    def addProperty( self, _prop, _id, _set, _name, _propName = "name" ):
        with self.driver.session() as session:
            session.run(
                "MERGE (:" +_prop+ " {" +_propName+ ": $name, set: $set, toolchain: $uuid})",
                uuid=self.uuid,
                set=_set,
                name=_name
            )
            session.run(
                "MATCH (prop:" +_prop+ " {" +_propName+ ": $name, set: $set, toolchain: $uuid}), (p:Place {id:$id}) MERGE (prop)-[:Describes]->(p)",
                uuid=self.uuid,
                id=_id,
                set=_set,
                name=_name
            )
        
    

def getByColumn( field, headings, row, default = None ):
    field = field.lower()
    if field not in headings:
        if default == None:
            raise f"Unable to find heading '{field}', and no default set"
        return default

    return row[headings.index(field)]


if __name__ == "__main__":
    (options, args) = parser.parse_args()

    db = IPN2Neo4j()

    logger.info( "ID for IPN Tooling: " + str(db.uuid) )

    with open(options.input, newline='', encoding='latin-1') as sourceCSV:
        ipnReader = csv.reader( sourceCSV )

        header = []
        header = next( ipnReader )

        logger.info( header )

        for row in ipnReader:
            db.updateRow( header, row )