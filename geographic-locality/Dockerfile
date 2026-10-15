# set base image (host OS)
FROM python:3.12-slim

# set the working directory in the container
WORKDIR /code

# Create new user with UID
RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

# copy the dependencies file to the working directory
COPY geographic-locality/requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# copy the MAS (main.py, value_service.py, pointer_annotations.py, indigenous_names.py)
COPY geographic-locality/*.py ./

# copy shared directory
COPY shared/ shared/

# Set user to newly created user
USER 1001

# command to run on container start
CMD [ "python", "main.py" ]
