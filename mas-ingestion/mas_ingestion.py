from typing import Dict, Any, Tuple
import os
import requests
import logging
import json

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

HAS_IDENTIFIER = "$['ods:hasIdentifier']"
HAS_EVENT = "$['ods:hasEvent']"
ODS_TYPE = "$['ods:type']"
SPECIMEN_TYPE = 'https://doi.org/21.T11148/894b1e6cad57e921764e'
MEDIA_TYPE = 'https://doi.org/21.T11148/bbad8c4e101e8af01115'


def build_secret(name: str, secret_key_ref: str) -> Dict[str, str]:
    return {
        "schema:name": name,
        "ods:secretKeyRef": secret_key_ref
    }


def build_attributes(name: str, description: str, image: str, tag: str,
                     target_filter: Dict[str, Any], batching: bool,
                     secrets=None) -> \
        Dict[str, Any]:
    if secrets is None:
        secrets = []
    request = {
        'data': {
            'type': 'ods:MachineAnnotationService',
            'attributes': {
                'schema:name': name,
                'schema:description': description,
                'ods:containerImage': image,
                'ods:containerTag': tag,
                'ods:TargetDigitalObjectFilter': target_filter,
                'schema:codeRepository': 'https://github.com/DiSSCo/demo-enrichment-service-image/',
                'schema:programmingLanguage': 'python',
                'schema:license': 'https://www.apache.org/licenses/LICENSE-2.0',
                'ods:batchingPermitted': batching,
                'ods:hasEnvironmentalVariable': [],
                'ods:hasSecretVariable': secrets
            }
        }
    }
    return request


def bold() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/72M-NDX-140'
    name = 'bold-linkage'
    description = 'Links specimen to an entry in the Barcode of Life Data System (BOLD)'
    image = 'public.ecr.aws/dissco/bold-linkage'
    tag = 'latest'
    target_filter = {
        HAS_IDENTIFIER: ['*'],
        ODS_TYPE: [SPECIMEN_TYPE]
    }
    batching = False
    secrets = [
        build_secret('API_USER', 'bold-api-user'),
        build_secret('API_PASSWORD', 'bold-api-password')
    ]
    return build_attributes(name, description, image, tag, target_filter,
                            batching, secrets), test_id


def ena() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/FSJ-G4M-L47'
    name = 'ena-linkage'
    description = 'Links specimen to an entry in the European Nucleotide Archive (ENA)'
    image = 'public.ecr.aws/dissco/ena-linkage'
    tag = 'latest'
    target_filter = {
        HAS_IDENTIFIER: ['*'],
        HAS_EVENT: ['*'],
        ODS_TYPE: [SPECIMEN_TYPE]
    }
    batching = False
    return build_attributes(name, description, image, tag, target_filter,
                            batching, None), test_id


def gbif() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/KV5-4SJ-384'
    name = 'gbif-linkage'
    description = 'Links specimen to an occurrence in Global Biodiversity Information Facility (GBIF)'
    image = 'public.ecr.aws/dissco/gbif-occurrence-linkage'
    tag = 'latest'
    target_filter = {
        HAS_IDENTIFIER: ['*'],
        "$['dwc:basisOfRecord']": ['*'],
        ODS_TYPE: [SPECIMEN_TYPE]
    }
    batching = False
    return build_attributes(name, description, image, tag, target_filter,
                            batching, None), test_id


def geocase() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/37R-AG5-6KV'
    name = 'geocase-linkage'
    description = 'Links specimen to an entity in  Geoscience Collections Access Service (GeoCASe)'
    image = 'public.ecr.aws/dissco/geocase-linkage'
    tag = 'latest'
    target_filter = {
        HAS_IDENTIFIER: ['*'],
        ODS_TYPE: [SPECIMEN_TYPE]
    }
    batching = False
    return build_attributes(name, description, image, tag, target_filter,
                            batching, None), test_id


def plant_organ() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/BEE-FF5-41X'
    name = 'herbarium-sheet-plant-organ-detection'
    description = 'Uses machine learning classifier to identify plant organs on herbarium sheets'
    image = 'public.ecr.aws/dissco/geocase-linkage'
    tag = 'latest'
    target_filter = {
        "$['ac:accessURI']": ['*'],
        ODS_TYPE: [MEDIA_TYPE]
    }
    batching = False
    return build_attributes(name, description, image, tag, target_filter,
                            batching, None), test_id


def image_metadata() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/QNH-4YQ-ZD4'
    name = 'image-metadata-addition'
    description = 'Uses the Python Imaging Library to add additional image metadata (size, format, etc.) to digital media'
    image = 'public.ecr.aws/dissco/image-metadata-addition'
    tag = 'latest'
    target_filter = {
        "$['ac:accessURI']": ['*'],
        ODS_TYPE: [MEDIA_TYPE]
    }
    batching = False
    return build_attributes(name, description, image, tag, target_filter,
                            batching, None), test_id


def mindat() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/3VL-80Q-8NK'
    name = 'mindat-georeferencing'
    description = 'Uses the Mindat georeferencing API to add Georeference coordinates'
    image = 'public.ecr.aws/dissco/mindat-georeferencing'
    tag = 'latest'
    target_filter = {
        HAS_EVENT: ['*'],
        ODS_TYPE: [SPECIMEN_TYPE]
    }
    batching = True
    secrets = [
        build_secret('API_KEY', 'mindat-api-key')
    ]
    return build_attributes(name, description, image, tag, target_filter,
                            batching, secrets), test_id


def osm() -> Tuple[Dict[str, Any], str]:
    test_id = 'TEST/L9G-HB0-YPS'
    name = 'osm-geopick-georeferencing'
    description = 'Uses the OSM Georeferencing tool to get coordinates and the Geopick API to get the center of the polygon to apply Georeferencing coordinates'
    image = 'public.ecr.aws/dissco/osm-georeferencing'
    tag = 'latest'
    target_filter = {
        HAS_EVENT: ['*'],
        ODS_TYPE: [SPECIMEN_TYPE]
    }
    batching = True
    secrets = [
        build_secret('GEOPICK_USER', 'geopick-user'),
        build_secret('GEOPICK_PASSWORD', 'geopick-password')
    ]
    return build_attributes(name, description, image, tag, target_filter,
                            batching, secrets), test_id


def get_token() -> str:
    url = f"{os.environ.get('server')}auth/realms/{os.environ.get('realm')}/protocol/openid-connect/token"
    data = f"grant_type={os.environ.get('grantType')}&client_id={os.environ.get('clientId')}&client_secret={os.environ.get('clientSecret')}&scope=roles"
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(url=url, data=data, headers=header)
    return response.json().get('access_token')

def update(request_json: Dict[str, Any], test_id: str) -> None:
    header = {
        'Authorization': 'Bearer ' + get_token()
    }
    url = f"https://dev-orchestration.dissco.tech/api/v1/mas/{test_id}"
    response = requests.patch(url=url, json=request_json, headers=header)
    response.raise_for_status()
    logging.info(json.dumps(response.json(), indent=2))

def post(request_json: Dict[str, Any]) -> None:
    header = {
        'Authorization': 'Bearer ' + get_token()
    }
    url = 'https://dev-orchestration.dissco.tech/api/v1/mas/'
    response = requests.post(url=url, json=request_json, headers=header)
    response.raise_for_status()
    logging.info(json.dumps(response.json(), indent=2))


if __name__ == '__main__':
    request_json, test_id = gbif()
    update(request_json, test_id)

