import re
import json
from neo4j import GraphDatabase

url = 'bolt://localhost:7687'
with open("config.json") as f:
    neo4j_conf = json.load(f)
neo4j_auth_id = neo4j_conf['neo4j_auth_id']
neo4j_auth_pw = neo4j_conf['neo4j_auth_pw']
auth = (neo4j_auth_id, neo4j_auth_pw)

with open("SiwooFamily.json") as f:
    siwoo_fam_data = json.load(f)
with open("Tools.json") as f:
    tools = json.load(f)

with open("Cities.json") as f:
    cities = json.load(f)

def add_character(driver, name, character_config):
    query = """
    MERGE (a:Character {name: $name, gender: $gender, age: $age, hobby: $hobby})
    """
    driver.run(
        query,
        name=name,
        gender=character_config['gender'],
        age=character_config['age'],
        hobby=character_config['hobby']
    )

def add_city(driver, name, city_config):
    query = """
    MERGE (a:City {name:$name, alias:$alias, country:$country, main_citizen_type:$main_citizen_type weather:$weather, average_temperature:$average_temperature, latitude:$latitude, longitude:$longitude, altitude_m:$altitude, area_km2:$area})
    """
    driver.run(
        query,
        name=name,
        alias=city_config['alias'],
        country=city_config['country'],
        main_citizen_type=city_config['main_citizen_type'],
        weather=city_config['weather'],
        average_temperature=city_config['average_temperature'],
        latitude=city_config['latitude'],
        longitude=city_config['longitude'],
        altitude=city_config['altitude'],
        area=city_config['area']        
    )

def add_citizen_type(driver, name, citizen_config):
    query = """
    MERGE (a:CitizenTypeConfig {name:$name, class:$classes, definition:$definition})
    """
    driver.run(
        query,
        name=name,
        classes=citizen_config['classes'],
        
        definition=citizen_config['definition']
    )

def char_city_relationships(driver, char_name, city_name):
    if city_name is not None:
        query = """
        MATCH (a:Character {name:$char_name}), (b:City {name:$city_name})
        MERGE (a) -[:LIVES_IN]-> (b)
        """
        driver.run(
            query,
            char_name=char_name,
            city_name=city_name
        )

# def add_country(driver, country_name, country_config):
#     query = """
#     MERGE (a:Country {name:$name, cities:$cities, ruler:$ruler, })
#     """

def char_relationships(driver, name, character_config):
    relationships = [
        ("IS_SON_OF", "Child", "Parent"),
        ("IS_DAUGHTER_OF", "Child", "Parent"),
        ("IS_SPOUSE_OF", "Spouse", "Spouse"),
        ("IS_BROTHER_OF", "Sibling", "Sibling"),
        ("IS_SISTER_OF", "Sibling", "Sibling"),
        ("IS_FATHER_OF", "Parent", "Child"),
        ("IS_MOTHER_OF", "Parent", "Child"),
        ("LIVES_IN", "Resident", "City")
    ]

    for rel, source, target in relationships:
        if character_config.get(rel):
            if rel == "LIVES_IN":
                query = """
                MATCH (a:Character {name: $name}), (b:City {name: $target})
                MERGE (a)-[:LIVES_IN]->(b)
                """
                driver.run(
                    query,
                    name=name,
                    target=character_config[rel]
                )
            else:
                for target_name in character_config[rel]:
                    query = f"""
                    MATCH (a:Character {{name: $name}}), (b:Character {{name: $target_name}})
                    MERGE (a)-[:{rel}]->(b)
                    """
                    driver.run(
                        query,
                        name=name,
                        target_name=target_name
                    )

def add_tools(driver, name, tool_config):
    query = """
    MERGE (a:Tools {name: $name, type: $type, alias: $alias, explanation: $explanation, how_to: $how_to})
    """
    driver.run(
        query,
        name=name,
        type=tool_config['type'],
        alias=tool_config['alias'],
        explanation=tool_config['explanation'],
        how_to=tool_config['how_to']
    )

def tools_requirements(driver, tool_name, tool_config):
    if tool_config['REQUIRES'] is not None:
        for required_tool in tool_config['REQUIRES']:
            query = """
            MATCH (a: Tools {name:$tool_name}), (b: Tools {name:$required_tool})
            MERGE (a) -[:REQUIRES]-> (b)
            """
            driver.run(
                query,
                tool_name=tool_name,
                required_tool=required_tool
            )

def char_tools_relationship(driver, char_name, char_config, tools_config):
    query = '''
    MATCH (a:Character {name: $name}), (b:Tools {name: $tool_name})
    MERGE (a)-[:USES]->(b)
    '''
    if char_config['hobby'] is not None:
        for hobby in char_config['hobby']:
            hobby = hobby.split()[0]
            if hobby in tools_config:
                driver.run(
                    query,
                    name=char_name,
                    tool_name=hobby
                )
            

driver = GraphDatabase.driver(url, auth=auth)

with driver.session() as session:
    for name, config in siwoo_fam_data.items():
        add_character(session, name, config)

    for name, config in siwoo_fam_data.items():
        char_relationships(session, name, config)

    for name, config in cities.items():
        add_city(session, name, config)

    for name, config in tools.items():
        add_tools(session, name, config)

    for name, config in siwoo_fam_data.items():
        char_tools_relationship(session, name, config, tools)

    for name, config in siwoo_fam_data.items():
        char_city_relationships(session, name, config['LIVES_IN'])
    
    for tool_name, tool_config in tools.items():
        tools_requirements(session, tool_name, tool_config)