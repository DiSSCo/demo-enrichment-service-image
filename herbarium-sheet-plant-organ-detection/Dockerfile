# set base image (host OS)
FROM python:3.9
# cannot use the slim image because of missing dependencies for pycocotools

# set the working directory in the container
WORKDIR /code

# Create new user with UID
RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

# Set user to newly created user
USER 1001

#RUN apt-get -y update && apt-get -y install git ffmpeg libsm6 libxext6

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

RUN wget https://github.com/2younis/plant-organ-detection/releases/download/v1.0/model_final.pth -P data/

RUN python -m pip install 'git+https://github.com/facebookresearch/detectron2.git'

# copy the content of the local src directory to the working directory
COPY main.py .
COPY config/custom_model_config.yaml ./config/custom_model_config.yaml

# command to run on container start
CMD [ "python", "main.py" ]
