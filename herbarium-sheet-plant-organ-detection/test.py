from detectron2.config import get_cfg
from detectron2.engine.defaults import DefaultPredictor
from detectron2 import model_zoo

from main import run_object_detection

cfg = get_cfg()
cfg.merge_from_file(
    model_zoo.get_config_file("PascalVOC-Detection/faster_rcnn_R_50_FPN.yaml")
)
cfg.merge_from_file("config/custom_model_config.yaml")
cfg.freeze()
predictor = DefaultPredictor(cfg)

test_object = {
    "id": "test_object",
    "content": {
        "ods:images": [
            {
                "ods:imageURI": "https://search.senckenberg.de/pix/images/sesam/32/64133.jpg"
            }
        ]
    },
}

result = run_object_detection(
    test_object["content"]["ods:images"][0]["ods:imageURI"], predictor
)
print("Result: ", result)
