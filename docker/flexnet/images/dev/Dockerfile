# python dev environment
FROM python:3.12.2-slim

RUN apt update && apt install -y build-essential swig pkg-config git

COPY ./images/dev/requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY ./testing-library /monad/testing-library
RUN pip3 install /monad/testing-library
