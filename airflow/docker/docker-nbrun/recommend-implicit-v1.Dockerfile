FROM "nbrun-base:1.0"

ENV ADDITIONAL_PYTHON_DEPS="\
implicit \
"

RUN pip3 install ${ADDITIONAL_PYTHON_DEPS}