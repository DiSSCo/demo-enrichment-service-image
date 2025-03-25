# set base image (host OS)
FROM python:3.11-slim

# set the working directory in the container
WORKDIR /code

# Create new user with UID
RUN adduser --disabled-password --gecos '' --system --uid 1001 python && chown -R python /code

# copy the dependencies file to the working directory
COPY bold-linkage/requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY bold-linkage/main.py .

# copy shared directory
COPY shared/ shared/

# Set user to newly created user
USER 1001

# command to run on container start
CMD [ "python", "main.py" ]