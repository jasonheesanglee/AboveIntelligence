import re
import json
from neo4j import GraphDatabase

url = 'bolt://localhost:7687'
with open("config.json") as f:
    neo4j_conf = json.load(f)
neo4j_auth_id = neo4j_conf['neo4j_auth_id']
neo4j_auth_pw = neo4j_conf['neo4j_auth_pw']
auth = (neo4j_auth_id, neo4j_auth_pw)

with open("Characters.json") as f:
    characters = json.load(f)
with open("Tools.json") as f:
    tools = json.load(f)
with open("Cities.json") as f:
    cities = json.load(f)
with open("Countries.json") as f:
    countries = json.load(f)
with open("CitizenType.json") as f:
    citizen_type = json.load(f)

def add_INITIAL(driver):
    query = """
    MERGE (a: Rank)
    MERGE (b: Human)
    MERGE (c: Cultiva)
    """
    driver.run(query)

def add_citizen_type(driver, name, citizen_config):
    query = """
    MERGE (b:CitizenType {name:$name, class:$class, definition:$definition})
    """
    driver.run(
        query,
        name=name,
        classes=citizen_config['class'],
        definition=citizen_config['definition']
    )

def rank_citizen_type(driver, name):
    query = """
    MATCH (a: Rank), (b: CitizenType {name:$name})
    MERGE (a) - (b)
    """
    driver.run(
        query,
        name=name
        )

def add_character(driver, name, character_config):
    query = """
    MERGE (a:Character {name: $name, gender: $gender, age: $age, citizen_type=$citizen_type, hobby: $hobby, characteristics:$characteristics})
    """
    driver.run(
        query,
        name=name,
        gender=character_config['gender'],
        age=character_config['age'],
        citizen_type=character_config['citizen_type'],
        hobby=character_config['hobby'],
        characteristics=character_config['characteristics']
    )

def character_species(driver, name, character_config):
    human_query = """
    MATCH (a: Human), (b: Character {name:$name})
    MERGE (a) - (b)
    """
    cultiva_query = """
    MATCH (a: Cultiva), (b: Character {name:$name})
    MERGE (a) - (b)
    """

    if character_config['citizen_type'] == '인간':
        driver.run(
            human_query,
            name=name
        )
    else:
        driver.run(
            cultiva_query,
            name=name
        )

def add_country(driver, name, country_config):
    query = """
    MERGE (a: Country {name:$name, alias:$alias, latitude:$latitude, longitude:$longitude, capital:$capital, characteristics:$characteristics, population:$population})
    """
    driver.run(
        query,
        name=name,
        alias=country_config['alias'],
        latitude=country_config['latitude'],
        longitude=country_config['longitude'],
        capital=country_config['capital'],
        population=country_config['population'],
        government=country_config['government']
    )

def add_city(driver, name, city_config):
    query = """
    MERGE (a:City {name:$name, alias:$alias, country:$country, main_citizen_type:$main_citizen_type weather:$weather, geography:$geography, characteristics:$characteristics, average_temperature:$average_temperature, latitude:$latitude, longitude:$longitude, altitude_m:$altitude, area_km2:$area})
    """
    driver.run(
        query,
        name=name,
        alias=city_config['alias'],
        country=city_config['country'],
        main_citizen_type=city_config['main_citizen_type'],
        weather=city_config['weather'],
        geography=city_config['geography'],
        characteristics=city_config['characteristics'],
        average_temperature=city_config['average_temperature'],
        latitude=city_config['latitude'],
        longitude=city_config['longitude'],
        altitude=city_config['altitude'],
        area=city_config['area']
    )

def country_city_relationships(driver, city_config):
    capital_query = """
    MATCH (a: Country {name:$name}), (b:City {name:$city_name})
    MERGE (b) -[:IS_CAPITAL_OF]-> (a)
    """
    non_capital_query = """
    MATCH (a: Country {name:$name}), (b:City {name:$city_name})
    MERGE (b) -[:IS_CITY_OF]-> (a)
    """
    if city_config['IS_CAPITAL_OF']:
        driver.run(
            capital_query,
            name=city_config['IS_CITY_OF'],
            city_name=city_config['name']
            )
    else:
        driver.run(
            non_capital_query,
            name=city_config['IS_CITY_OF'],
            city_name=city_config['name']
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
    add_INITIAL(driver)
    for name, config in citizen_type.items():
        add_citizen_type(session, name, config)
        rank_citizen_type(session, name)

    for name, config in characters.items():
        add_character(session, name, config)
        character_species(session, name, config)

    for name, config in characters.items():
        char_relationships(session, name, config)

    for name, config in countries.items():
        add_country(session, name, config)

    for name, config in cities.items():
        add_city(session, name, config)
        country_city_relationships(session, config)

    for name, config in tools.items():
        add_tools(session, name, config)

    for name, config in characters.items():
        char_tools_relationship(session, name, config, tools)

    for name, config in characters.items():
        char_city_relationships(session, name, config['LIVES_IN'])
    
    for tool_name, tool_config in tools.items():
        tools_requirements(session, tool_name, tool_config)



