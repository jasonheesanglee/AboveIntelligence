import re
import json
from neo4j import GraphDatabase

url = 'bolt://localhost:7687'
with open("config.json") as f:
    neo4j_conf = json.load(f)
neo4j_auth_id = neo4j_conf['neo4j_auth_id']
neo4j_auth_pw = neo4j_conf['neo4j_auth_pw']
auth = (neo4j_auth_id, neo4j_auth_pw)


with open("InitialNode.json") as f:
    initial_node = json.load(f)
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

def add_INITIAL(driver, name, config):
    query = """
    MERGE (a: InitialNode {name:$name, alias:$alias, rank:$rank, definition:$definition})
    """
    driver.run(
        query,
        name=name,
        alias=config['alias'],
        rank=config['rank'],
        definition=config['definition']
        )


def add_citizen_type(driver, name, citizen_config):
    query = """
    MERGE (b:CitizenType {name:$name, alias:$alias, class:$classes, definition:$definition})
    """
    driver.run(
        query,
        name=name,
        alias=citizen_config['alias'],
        classes=citizen_config['class'],
        definition=citizen_config['definition']
    )

def connect_convergent(driver, char_name, char_config):
    if char_config['citizen_type'] != "인간":
        query = """
        MATCH (a: InitialNode {name:"컨버전트"}), (b: Character {name:$name})
        MERGE (b) -[:IS]-> (a)
        """
        driver.run(
            query,
            name=char_name
            )

def rank_citizen_type(driver, name):
    query = """
    MATCH (a: InitialNode {name:"계급"}), (b: CitizenType {name:$name})
    MERGE (b) -[:IS_PART_OF]-> (a)
    """
    driver.run(
        query,
        name=name
        )

def add_character(driver, name, character_config):
    query = """
    MERGE (a:Character {name:$name, alias:$alias, gender:$gender, age:$age, citizen_type:$citizen_type, hobby:$hobby, characteristics:$characteristics})
    """
    driver.run(
        query,
        name=name,
        alias=character_config['alias'],
        gender=character_config['gender'],
        age=character_config['age'],
        citizen_type=character_config['citizen_type'],
        hobby=character_config['hobby'],
        characteristics=character_config['characteristics']
    )

def character_species(driver, name, character_config):
    if character_config['citizen_type'] == "인간":
        query = """
        MATCH (a: InitialNode {name:$node_name}), (b: Character {name:$name})
        MERGE (b) -[:IS]-> (a)
        """

    else:
        query = """
        MATCH (a: CitizenType {name:$node_name}), (b: Character {name:$name})
        MERGE (b) -[:IS]-> (a)
        """
    
    driver.run(
        query,
        node_name=character_config['citizen_type'],
        name=name
    )

def char_relationships(driver, name, character_config):
    relationships = [
        ("IS_SON_OF", "Child", "Parent"),
        ("IS_DAUGHTER_OF", "Child", "Parent"),
        ("IS_SPOUSE_OF", "Spouse", "Spouse"),
        ("IS_BROTHER_OF", "Sibling", "Sibling"),
        ("IS_SISTER_OF", "Sibling", "Sibling"),
        ("IS_FATHER_OF", "Parent", "Child"),
        ("IS_MOTHER_OF", "Parent", "Child")
    ]

    for rel, _, _ in relationships:
        if character_config.get(rel):
            if rel == "LIVES_IN":
                pass
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

    if character_config["IS_FRIEND_OF"]:
        for friend in character_config["IS_FRIEND_OF"]:
            query = """
            MATCH (a: Character {name:$name}), (b: Character {name:$target})
            MERGE (a) <-[:IS_FRIENDS_OF]-> (b)
            """
            driver.run(
                query,
                name=name,
                target=friend
            )
    
    if character_config['IS_PRINCIPAL'] == 'TRUE':
        query = """
        MATCH (a:Character {name:$name}), (b:CitizenType {name:"현자"})
        MERGE (a) -[:IS]->(b)
        """
        driver.run(
            query,
            name=name
            )

    if character_config['LIVES_IN']:
        for city in character_config['LIVES_IN']:
            if "노마드" in city:
                query = """
                MATCH (a:Character {name:$name}), (b:Country {name:$target_name})
                MERGE (a) -[:LIVES_IN]-> (b)
                """
            else:
                query = """
                MATCH (a:Character {name:$name}), (b:City {name:$target_name})
                MERGE (a) -[:LIVES_IN]-> (b)
                """
            driver.run(
                query,
                name=name,
                target_name=city
                )

def add_country(driver, name, country_config):
    query = """
    MERGE (a: Country {name:$name, alias:$alias, area:$area, latitude:$latitude, longitude:$longitude, capital:$capital, characteristics:$characteristics, population_approx:$population})
    """
    driver.run(
        query,
        name=name,
        alias=country_config['alias'],
        area=country_config['area'],
        latitude=country_config['latitude'],
        longitude=country_config['longitude'],
        capital=country_config['capital'],
        characteristics=country_config['characteristics'],
        population=country_config['population'],

    )

def add_city(driver, name, city_config):
    query = """
    MERGE (a:City {name:$name, alias:$alias, country:$country, main_citizen_type:$main_citizen_type, weather:$weather, geography:$geography, characteristics:$characteristics, average_temperature:$average_temperature, latitude:$latitude, longitude:$longitude, altitude_m:$altitude, area_km2:$area})
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
        average_temperature=str(city_config['average_temperature']),
        latitude=city_config['latitude'],
        longitude=city_config['longitude'],
        altitude=city_config['altitude'],
        area=city_config['area']
    )

def country_city_relationships(driver, name, city_config):
    if city_config['IS_CAPITAL_OF']=="TRUE":
        query = """
        MATCH (a: Country {name:$name}), (b:City {name:$city_name})
        MERGE (b) -[:IS_CAPITAL_OF]-> (a)
        """
    else:
        query = """
        MATCH (a: Country {name:$name}), (b:City {name:$city_name})
        MERGE (b) -[:IS_CITY_OF]-> (a)
        """
    if city_config['IS_CAPITAL_OF']:
        driver.run(
            query,
            name=city_config['IS_CITY_OF'],
            city_name=name
            )

# --------------------------------------------

# def char_city_relationships(driver, char_name, char_config):
#     if char_config['LIVES_IN'] is not None:
#         query = """
#         MATCH (a:Character {name:$char_name}), (b:City {name:$city_name})
#         MERGE (a) -[:LIVES_IN]-> (b)
#         """
#         driver.run(
#             query,
#             char_name=char_name,
#             city_name=char_config['LIVES_IN']
#         )

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
        how_to=tool_config['how_to'],
        made_by=tool_config['made_by']
    )

def tools_requirements(driver, tool_name, tool_config):
    query = """
            MATCH (a: Tools {name:$tool_name}), (b: Tools {name:$required_tool})
            MERGE (a) -[:REQUIRES]-> (b)
            """
    if tool_config['REQUIRES'] is not None:
        for required_tool in tool_config['REQUIRES']:
            driver.run(
                query,
                tool_name=tool_name,
                required_tool=required_tool
            )
    made_by_query = """
                    MATCH (c:Tools {name: $tool_name}), (d:Character {name: $name})
                    MERGE (c)-[:IS_MADE_BY]->(d)
                    """
    if tool_config['made_by'] is not None:
        driver.run(
            made_by_query,
            name=tool_config['made_by'],
            tool_name=tool_name
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
    for name, config in initial_node.items():
        add_INITIAL(session, name, config)

    for name, config in citizen_type.items():
        add_citizen_type(session, name, config)
        if name != "노마드" :
            rank_citizen_type(session, name)
        
    for name, config in characters.items():
        add_character(session, name, config)
        character_species(session, name, config)

    for name, config in countries.items():
        add_country(session, name, config)

    for name, config in cities.items():
        add_city(session, name, config)
        country_city_relationships(session, name, config)

    for name, config in characters.items():
        char_relationships(session, name, config)
        connect_convergent(session, name, config)

    # for name, config in characters.items():
    #     char_city_relationships(session, name, config)

    for name, config in tools.items():
        add_tools(session, name, config)

    for name, config in characters.items():
        char_tools_relationship(session, name, config, tools)
    
    for tool_name, tool_config in tools.items():
        tools_requirements(session, tool_name, tool_config)



