FROM ubuntu:20.04

LABEL maintainer="Adriano Lange"
LABEL email="alange0001@gmail.com"

RUN apt-get update && \
    apt-get install -y wget bzip2 curl smartmontools fio && \
    apt-get install -y libgflags2.2 libsnappy1v5 zlib1g libbz2-1.0 liblz4-1 libzstd1 libaio1 poppler-utils && \
    apt-get install -y openjdk-16-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -fr /opt && \
    cp -a /root /home/user && \
    chmod 777 /home/user && \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    sh /tmp/miniconda.sh -b -p /usr/local/conda && \
    rm -f /tmp/miniconda.sh && \
    ln -s /usr/local/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh


ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/storiks/bin:/opt/performancemonitor:/opt/YCSB/bin:/opt/rocksdb/bin:/usr/local/conda/bin
ENV LD_LIBRARY_PATH /opt/rocksdb/lib
ENV PYTHONPATH /opt/storiks/lib
ENV YCSB_PATH /opt/YCSB

RUN \
    conda install -y IPython numpy matplotlib pandas seaborn natsort psutil jupyter && \
    conda install -y -c conda-forge pdf2image

ENV STORIKS_PATH /opt/storiks/bin:/opt/performancemonitor:/opt/YCSB/bin:/opt/rocksdb/bin:/usr/local/conda/bin


# DEVELOPMENT ONLY:
# RUN \
#     apt update && \
#     apt install -y vim

COPY opt /opt

CMD ["bash"]
