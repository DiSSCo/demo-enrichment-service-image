# set base image (host OS)
FROM python:3.8-alpine

# set the working directory in the container
WORKDIR /code

# Create new user with UID
RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

# Set user to newly created user
USER 1001

# copy the dependencies file to the working directory
COPY image-metadata-addition/requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY image-metadata-addition/main.py .

# copy shared directory
COPY shared/ shared/

# command to run on container start
CMD [ "python", "main.py" ]