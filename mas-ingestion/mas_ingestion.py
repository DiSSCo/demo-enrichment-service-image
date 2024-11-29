from typing import Dict, Any
import os
import requests
import logging
import json

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)

HAS_IDENTIFIER = "$['ods:hasIdentifiers']"
HAS_EVENT = "$['ods:hasEvents']"
ODS_FDO_TYPE = "$['ods:fdoType']"
AC_URI = "$['ac:accessURI']"
SPECIMEN_TYPE = "https://doi.org/21.T11148/894b1e6cad57e921764e"
MEDIA_TYPE = "https://doi.org/21.T11148/bbad8c4e101e8af01115"

BOLD_TEST = ""
BOLD_ACC = ""
ENA_TEST = ""
ENA_ACC = ""
GBIF_TEST = "TEST/X47-D32-7FW"
GBIF_ACC = ""
GEOCASE_TEST = ""
GEOCASE_ACC = ""
PLANT_ORGAN_TEST = ""
PLANT_ORGAN_ACC = ""
IMAGE_METADATA_TEST = ""
IMAGE_METADATA_ACC = ""
MINDAT_TEST = ""
MINDAT_ACC = ""
OSM_TEST = ""
OSM_ACC = ""
SENCK_TEST = "TEST/V1Z-0JJ-GX7"
SENCK_ACC = "SANDBOX/F28-90S-SQX"

def build_secret(name: str, secret_key_ref: str) -> Dict[str, str]:
    return {"schema:name": name, "ods:secretKeyRef": secret_key_ref}


def build_attributes(
    name: str, description: str, image: str, tag: str, target_filter: Dict[str, Any], batching: bool, secrets=None
) -> Dict[str, Any]:
    if secrets is None:
        secrets = []
    request = {
        "data": {
            "type": "ods:MachineAnnotationService",
            "attributes": {
                "schema:name": name,
                "schema:description": description,
                "ods:containerImage": image,
                "ods:containerTag": tag,
                "ods:hasTargetDigitalObjectFilter": target_filter,
                "schema:codeRepository": "https://github.com/DiSSCo/demo-enrichment-service-image/",
                "schema:programmingLanguage": "python",
                "schema:license": "https://www.apache.org/licenses/LICENSE-2.0",
                "ods:batchingPermitted": batching,
                "ods:hasEnvironmentalVariables": [],
                "ods:hasSecretVariables": secrets,
            },
        }
    }
    return request


def bold() -> Dict[str, Any]:
    name = "bold-linkage"
    description = "Links specimen to an entry in the Barcode of Life Data System (BOLD)"
    image = "public.ecr.aws/dissco/bold-linkage"
    tag = "latest"
    target_filter = {HAS_IDENTIFIER: ["*"], ODS_FDO_TYPE: [SPECIMEN_TYPE]}
    batching = False
    secrets = [build_secret("API_USER", "bold-api-user"), build_secret("API_PASSWORD", "bold-api-password")]
    return build_attributes(name, description, image, tag, target_filter, batching, secrets)

def ena() -> Dict[str, Any]:
    name = "ena-linkage"
    description = "Links specimen to an entry in the European Nucleotide Archive (ENA)"
    image = "public.ecr.aws/dissco/ena-linkage"
    tag = "latest"
    target_filter = {HAS_IDENTIFIER: ["*"], HAS_EVENT: ["*"], ODS_FDO_TYPE: [SPECIMEN_TYPE]}
    batching = False
    return build_attributes(name, description, image, tag, target_filter, batching, None)


def gbif() -> Dict[str, Any]:
    name = "gbif-linkage"
    description = "Links specimen to an occurrence in Global Biodiversity Information Facility (GBIF)"
    image = "public.ecr.aws/dissco/gbif-occurrence-linkage"
    tag = "latest"
    target_filter = {HAS_IDENTIFIER: ["*"], "$['dwc:basisOfRecord']": ["*"], ODS_FDO_TYPE: [SPECIMEN_TYPE]}
    batching = False
    return build_attributes(name, description, image, tag, target_filter, batching, None)


def geocase() -> Dict[str, Any]:
    name = "geocase-linkage"
    description = "Links specimen to an entity in  Geoscience Collections Access Service (GeoCASe)"
    image = "public.ecr.aws/dissco/geocase-linkage"
    tag = "latest"
    target_filter = {HAS_IDENTIFIER: ["*"], ODS_FDO_TYPE: [SPECIMEN_TYPE]}
    batching = False
    return build_attributes(name, description, image, tag, target_filter, batching, None)


def plant_organ() -> Dict[str, Any]:
    name = "herbarium-sheet-plant-organ-detection"
    description = "Uses machine learning classifier to identify plant organs on herbarium sheets"
    image = "public.ecr.aws/dissco/geocase-linkage"
    tag = "latest"
    target_filter = {AC_URI: ["*"], ODS_FDO_TYPE: [MEDIA_TYPE]}
    batching = False
    return build_attributes(name, description, image, tag, target_filter, batching, None)


def image_metadata() -> Dict[str, Any]:
    name = "image-metadata-addition"
    description = (
        "Uses the Python Imaging Library to add additional image metadata (size, format, etc.) to digital media"
    )
    image = "public.ecr.aws/dissco/image-metadata-addition"
    tag = "latest"
    target_filter = {AC_URI: ["*"], ODS_FDO_TYPE: [MEDIA_TYPE]}
    batching = False
    return build_attributes(name, description, image, tag, target_filter, batching, None)


def mindat() -> Dict[str, Any]:
    name = "mindat-georeferencing"
    description = "Uses the Mindat georeferencing API to add Georeference coordinates"
    image = "public.ecr.aws/dissco/mindat-georeferencing"
    tag = "latest"
    target_filter = {HAS_EVENT: ["*"], ODS_FDO_TYPE: [SPECIMEN_TYPE]}
    batching = True
    secrets = [build_secret("API_KEY", "mindat-api-key")]
    return build_attributes(name, description, image, tag, target_filter, batching, secrets)


def osm() -> Dict[str, Any]:
    name = "osm-geopick-georeferencing"
    description = "Uses the OSM Georeferencing tool to get coordinates and the Geopick API to get the center of the polygon to apply Georeferencing coordinates"
    image = "public.ecr.aws/dissco/osm-georeferencing"
    tag = "latest"
    target_filter = {HAS_EVENT: ["*"], ODS_FDO_TYPE: [SPECIMEN_TYPE]}
    batching = False
    secrets = [build_secret("GEOPICK_USER", "geopick-user"), build_secret("GEOPICK_PASSWORD", "geopick-password")]
    return build_attributes(name, description, image, tag, target_filter, batching, secrets)

def senck() -> Dict[str, Any]:
    name = "plant-organ-segmentation"
    description = "Herbarium sheet plant organ segmenter developed by Senckenberg Natural History Museum"
    image = "public.ecr.aws/dissco/herbarium-sheet-plant-organ-segmentation"
    tag = "sha-276a7fb" # or late
    target_filter = {AC_URI: ["*"], ODS_FDO_TYPE: [MEDIA_TYPE]}
    batching = True
    secrets = [build_secret("PLANT_ORGAN_SEGMENTATION_USER", "plant-organ-segmentation-user"), build_secret("PLANT_ORGAN_SEGMENTATION_PASSWORD", "plant-organ-segmentation-password")]
    return build_attributes(name, description, image, tag, target_filter, batching, secrets)


def get_token() -> str:
    url = f"{os.environ.get('server')}auth/realms/{os.environ.get('realm')}/protocol/openid-connect/token"
    data = f"grant_type={os.environ.get('grantType')}&client_id={os.environ.get('clientId')}&client_secret={os.environ.get('clientSecret')}&scope=roles"
    header = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url=url, data=data, headers=header)
    return response.json().get("access_token")


def update(request_json: Dict[str, Any], test_id: str) -> None:
    header = {"Authorization": "Bearer " + get_token()}
    url = f"https://dev-orchestration.dissco.tech/api/v1/mas/{test_id}"
    response = requests.patch(url=url, json=request_json, headers=header)
    response.raise_for_status()
    logging.info(json.dumps(response.json(), indent=2))


def post(request_json: Dict[str, Any]) -> None:
    header = {"Authorization": "Bearer " + get_token()}
    url = "https://orchestration.dissco.tech/api/v1/mas"
    response = requests.post(url=url, json=request_json, headers=header)
    response.raise_for_status()
    logging.info(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    request_json = senck()
    post(request_json)
