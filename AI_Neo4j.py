import re
import json
from neo4j import GraphDatabase

url = 'bolt://localhost:7687'
with open("config.json") as f:
    neo4j_auth_id = json.load(f)['neo4j_auth_id']
    neo4j_auth_pw = json.load(f)['neo4j_auth_pw']
auth = (neo4j_auth_id, neo4j_auth_pw)

with open("SiwooFamily.json") as f:
    siwoo_fam_data = json.load(f)
with open("Tools.json") as f:
    tools = json.load(f)

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

def add_relationships(driver, name, character_config):
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

def tools_requirements(driver, tools_config):
    for tool_name, tool_info in tools_config.items():
        if tool_info['REQUIRES'] is not None:
            for required_tool in tool_info['REQUIRES']:
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
        add_relationships(session, name, config)

    for name, config in tools.items():
        add_tools(session, name, config)

    for name, config in siwoo_fam_data.items():
        char_tools_relationship(session, name, config, tools)

    tools_requirements(session, tools)