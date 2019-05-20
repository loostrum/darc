FROM tensorflow/tensorflow:1.12.0-gpu-py3

RUN apt-get update && apt-get upgrade -y
RUN DEBIAN_FRONTEND='noninteractive' apt-get install -y git vim openssh-server \
        apache2 gfortran python-qpid qpidd qpid-client liblog4cplus-dev cmake \
        gcc g++ libboost-regex-dev libboost-python-dev python-psutil \
        python-iso8601 autogen libblitz0-dev castxml python-pip binutils-dev \
        netcat
RUN apt-get clean all

RUN ssh-keygen -A
RUN a2enmod userdir

# Add apertif software stack
# Install py++ through pip to avoid it pulling in gccxml
RUN pip2 install pyplusplus pygccxml==1.8.0
ARG BRANCH=ARTS-BusyWeek-May2019
ARG BUILDDIR=build/gnucxx11_debug
COPY $BRANCH /root/$BRANCH
RUN cd /root/$BRANCH && mkdir -p $BUILDDIR && cd $BUILDDIR && \
        cmake -DBUILD_PACKAGES=ARTSCLUSTER ../../ && make -j && make install && \
        ln -s $(pwd)/installed /opt/apertif

# Install DARC
COPY . /root/darc/
RUN mkdir /root/.ssh
RUN mv /root/darc/docker/authorized_keys /root/.ssh/
RUN mv /root/darc/docker/id_rsa /root/.ssh
RUN chmod go-rwx /root/.ssh/id_rsa

RUN pip3 install --upgrade pip
RUN pip3 install /root/darc

ENTRYPOINT ["/root/darc/docker/entrypoint.sh"]
