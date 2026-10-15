# set base image (host OS)
FROM python:3.12-slim

# set the working directory in the container
WORKDIR /code

# Create new user with UID
RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

# copy the dependencies file to the working directory
COPY geographic-locality/requirements.txt .

# install dependencies
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt

# copy the MAS (main.py, value_service.py, community_annotations.py)
COPY geographic-locality/*.py ./
COPY geographic-locality/geographic_locality_mas/ ./geographic_locality_mas/
COPY geographic-locality/data/reference/ ./data/reference/

# copy the vendored team modules and their data (community context, name registers)
COPY geographic-locality/geographic_locality_mas/ geographic_locality_mas/

# copy shared directory
COPY shared/ shared/

# Set user to newly created user
USER 1001

# command to run on container start
CMD [ "python", "main.py" ]
