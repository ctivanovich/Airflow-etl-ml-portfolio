FROM "nbrun-base:1.0"

ENV ADDITIONAL_PYTHON_DEPS="\
torch \
torchvision \
Pillow \
asyncio \
aiohttp \
facenet_pytorch \
h5py \
"

RUN pip3 install ${ADDITIONAL_PYTHON_DEPS}