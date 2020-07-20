FROM "gcr.io/..../nbrun-base:2.0"

ENV ADDITIONAL_PYTHON_DEPS="\
implicit \
lightfm \
"

RUN pip3 install ${ADDITIONAL_PYTHON_DEPS}