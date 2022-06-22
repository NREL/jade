#
# Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Copied and modified this file from https://nvidia.github.io/spark-rapids/docs/get-started/Dockerfile.cuda.
# Needed to make some modifications to get all apt install commands to work and to get all Nvidia
# software correctly installed.

# Usage:
# 1. Download getGpuResources.sh from https://nvidia.github.io/spark-rapids/docs/get-started/getting-started-on-prem.html#install-the-gpu-discovery-script
#    to your local directory.
# 2. Download these two jar files to your local directory.
#    wget https://repo1.maven.org/maven2/ai/rapids/cudf/22.04.0/cudf-22.04.0-cuda11.jar
#    wget https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.04.0/rapids-4-spark_2.12-22.04.0.jar
# 3. Detach from the VPN. You may get a certificate error while downloading spark.
# 4. docker build --tag nvidia_spark --build-arg VERSION=<x.y.z> .

# This container can be converted to a Singularity container on Eagle with these commands:
# Save and upload the docker image to Eagle.
# $ docker save -o nvidia_spark_v<x.y.z>.tar nvidia_spark
# $ scp nvidia_spark<x.y.z>.tar <username>@eagle.hpc.nrel.gov:/scratch/<username>/
# Acquire a compute node.
# $ export SINGULARITY_TMPDIR=/tmp/scratch
# $ module load singularity-container
# Create writable image for testing and development or read-only image for production.
# Writable
# $ singularity build --sandbox nvidia_spark docker-archive://nvidia_spark_v<x.y.z>.tar
# Read-only
# $ singularity build nvidia_spark_v<x.y.z>.sif docker-archive://nvidia_spark_v<x.y.z>.tar

FROM nvidia/cuda:11.0-devel-ubuntu20.04
#ARG spark_uid=185

ARG VERSION
ARG SPARK_VERSION=3.2.1
ARG HADOOP_VERSION=3.2
ARG FULL_STR=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

RUN if [ -z "$VERSION" ]; then echo "VERSION must be specified"; exit 1; fi
ENV CONTAINER_VERSION ${VERSION}

# Install java dependencies 
RUN rm /etc/apt/sources.list.d/cuda.list
#RUN rm /etc/apt/sources.list.d/nvidia-ml.list
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
	&& apt-get install -y --no-install-recommends openjdk-8-jdk openjdk-8-jre wget vim jq nano

ENV SPARK_HOME /opt/${FULL_STR}
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV PATH $PATH:/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/bin:/usr/lib/jvm/java-1.8.0-openjdk-amd64/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV SPARK_RAPIDS_PLUGIN_DIR /opt/sparkRapidsPlugin
ENV SPARK_CUDF_JAR ${SPARK_RAPIDS_PLUGIN_DIR}/cudf-22.04.0-cuda11.jar
ENV SPARK_GPU_DISCOVERY_SCRIPT=${SPARK_RAPIDS_PLUGIN_DIR}/getGpusResources.sh
ENV SPARK_RAPIDS_PLUGIN_JAR ${SPARK_RAPIDS_PLUGIN_DIR}/rapids-4-spark_2.12-22.04.0.jar

RUN mkdir /nopt
RUN mkdir /projects
RUN mkdir /scratch
RUN mkdir /lustre
RUN mkdir ${SPARK_RAPIDS_PLUGIN_DIR}

# Before building the docker image, first either download Apache Spark 3.0+ from
# http://spark.apache.org/downloads.html or build and make a Spark distribution following the
# instructions in http://spark.apache.org/docs/3.0.2/building-spark.html (see
# https://nvidia.github.io/spark-rapids/docs/download.html for other supported versions).  If this
# docker file is being used in the context of building your images from a Spark distribution, the
# docker build command should be invoked from the top level directory of the Spark
# distribution. E.g.: docker build -t spark:3.0.2 -f kubernetes/dockerfiles/spark/Dockerfile .

WORKDIR /opt
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${FULL_STR}.tgz \
	&& tar -xzf ${FULL_STR}.tgz \
	&& rm ${FULL_STR}.tgz \
	&& cp /opt/${FULL_STR}/conf/spark-defaults.conf.template /opt/${FULL_STR}/conf/spark-defaults.conf \
	&& cp /opt/${FULL_STR}/conf/spark-env.sh.template /opt/${FULL_STR}/conf/spark-env.sh \
	&& chmod +x /opt/${FULL_STR}/conf/spark-env.sh

RUN set -ex && \
    ln -s /lib /lib64 && \
    mkdir -p /opt/sparkRapidsPlugin && \
    touch $SPARK_HOME/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd

COPY rapids-4-spark_2.12-22.04.0.jar ${SPARK_RAPIDS_PLUGIN_DIR}/
COPY cudf-22.04.0-cuda11.jar ${SPARK_RAPIDS_PLUGIN_DIR}/
COPY getGpusResources.sh ${SPARK_RAPIDS_PLUGIN_DIR}/

# This is a workaround documented in Nvidia online forums.
RUN apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/opt/ubuntu2004/x86_64/3bf863cc.pub
RUN apt-get update && \
    apt install -y python3 python3-pip && \
    # We remove ensurepip since it adds no functionality since pip is
    # installed on the image and it just takes up 1.6MB on the image
    pip install --upgrade pip setuptools && \
    # You may install with python3 packages by using pip3.6
    # Removed the .cache to save space
    rm -r /root/.cache && rm -rf /var/cache/apt/*

#ENV TINI_VERSION v0.18.0
#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
#COPY tini /usr/bin/tini
#RUN chmod +rx /usr/bin/tini

# Install the CUDA toolkit.
RUN wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin \
    && mkdir -p /etc/apt/preferences.d/cuda-repository-pin-600 \
    && mv cuda-ubuntu2004.pin /etc/apt/preferences.d/cuda-repository-pin-600/
RUN wget https://developer.download.nvidia.com/compute/cuda/11.0.3/local_installers/cuda-repo-ubuntu2004-11-0-local_11.0.3-450.51.06-1_amd64.deb \
    && dpkg -i cuda-repo-ubuntu2004-11-0-local_11.0.3-450.51.06-1_amd64.deb \
    && rm cuda-repo-ubuntu2004-11-0-local_11.0.3-450.51.06-1_amd64.deb
RUN apt-key add /var/cuda-repo-ubuntu2004-11-0-local/7fa2af80.pub \
    && apt-get update \
    && apt-get -y install cuda \
    && rm -rf /root/.cache && rm -rf /var/cache/apt/*

CMD [ "bash" ]
# Disabled because we will start the spark processes ourselves.
#ENTRYPOINT [ "/opt/spark-3.2.1-bin-hadoop3.2/kubernetes/dockerfiles/spark/entrypoint.sh" ]

# Specify the User that the actual main process will run as
#USER ${spark_uid}
USER root
