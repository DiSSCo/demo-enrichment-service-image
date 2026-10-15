"""Extract structured distance context information from DiSSCo specimen records."""
import re

def extract_distance(specimen):
    if specimen.get("locality"):
        text = specimen.get("locality")
        distance_in_text = False
        if re.search(r'\b(km|kilometer|kilometers|mi|mile|miles|m|meter|meters)\b', text, re.IGNORECASE):
            distance_in_text = True
        coordinates_in_text = True if specimen.get("latitude") and specimen.get("longitude") else False
        if not coordinates_in_text and distance_in_text: # Coordinates with N/S/E/W may be confused with distance
            units_and_values = re.search(r'(\d+(\.\d+)?)\s*(km|kilometer|kilometers|mi|mile|miles|m|meter|meters)', text, re.IGNORECASE)
            if units_and_values:
                distance_value, _, distance_unit = units_and_values.groups()
                specimen["distance"] = {
                    "value": float(distance_value),
                    "unit": distance_unit.lower()
                }
            direction_match_pattern = r'\b(north|south|east|west|northeast|northwest|southeast|southwest|' + \
                                     r'n|s|e|w|ne|nw|se|sw|north-east|north-west|south-east|south-west\b' + \
                                     r'wsw|wnw|ene|ese|sse|ssw|nne|nnw|sww|nww\b' + \
                                    r'nördlich|südlich|östlich|westlich|nordöstlich|nordwestlich|südöstlich|südwestlich\b' + \
                                    r'|nord|süd|ost|west|nordost|nordwest|südost|südwest)\b'
            direction_match = re.search(direction_match_pattern, text, re.IGNORECASE)
            if direction_match:
                direction = direction_match.group(0).lower()
                direction_mapping = {
                    'n': 'north',
                    's': 'south',
                    'e': 'east',
                    'w': 'west',
                    'ne': 'northeast',
                    'nw': 'northwest',
                    'se': 'southeast',
                    'sw': 'southwest',
                    'wsw': 'west-southwest',
                    'wnw': 'west-northwest',
                    'ene': 'east-northeast',
                    'ese': 'east-southeast',
                    'sse': 'south-southeast',
                    'ssw': 'south-southwest',
                    'nne': 'north-northeast',
                    'nnw': 'north-northwest',
                    'sww': 'southwest-west',
                    'nww': 'northwest-west',
                    'north-east': 'northeast',
                    'north-west': 'northwest',
                    'south-east': 'southeast',
                    'south-west': 'southwest',
                    'nördlich': 'north',
                    'südlich': 'south',
                    'östlich': 'east',
                    'westlich': 'west',
                    'nordöstlich': 'northeast',
                    'nordwestlich': 'northwest',
                    'südöstlich': 'southeast',
                    'südwestlich': 'southwest',
                    'nord': 'north',
                    'süd': 'south',
                    'ost': 'east',
                    'west': 'west',
                    'nordost': 'northeast',
                    'nordwest': 'northwest',
                    'südost': 'southeast',
                    'südwest': 'southwest'
                }
                normalized_direction = direction_mapping.get(direction, direction)
                specimen["direction"] = normalized_direction  
            # TODO: Add further direction patterns or use the translation service depending on the language detected
    return specimen
