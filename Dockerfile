FROM nvcr.io/nvidia/deepstream:7.1-gc-triton-devel

RUN apt update -y 

RUN apt-get update && apt-get install -y \
        libglib2.0 libglib2.0-dev libssl-dev \
        libopencv-dev \
        libspdlog-dev \
        wget \
        python3-pip \
        python3-gi python3-dev python3-gst-1.0\
        libgstreamer1.0-0 \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        gstreamer1.0-tools \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        libjson-glib-dev \
        libcairo2-dev \
        librdkafka1 librdkafka-dev \
        libgstrtspserver-1.0-0 gstreamer1.0-rtsp\
        libgirepository1.0-dev \
        gobject-introspection gir1.2-gst-rtsp-server-1.0 \
        libjansson4 libjansson-dev protobuf-compiler\
    && apt-get remove -y gstreamer1.0-plugins-ugly \
    && rm -rf /var/lib/apt/lists/*
    

RUN wget https://github.com/NVIDIA-AI-IOT/deepstream_python_apps/releases/download/v1.2.0/pyds-1.2.0-cp310-cp310-linux_x86_64.whl \
    && pip3 install pyds-1.2.0-cp310-cp310-linux_x86_64.whl

RUN apt purge -y gstreamer1.0-plugins-ugly

RUN pip3 install --upgrade pip

WORKDIR /home/test/deepstream_rdx_sample

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

ENV PYTHONPATH=/opt/nvidia/deepstream/deepstream/lib

ENV OPENBLAS_CORETYPE=ARMV8

ENV PYTHONUNBUFFERED=1
ENV GST_DEBUG=3
CMD [ "python3", "deepstream_template.py" ]
