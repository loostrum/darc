FROM tensorflow/tensorflow:1.12.0-gpu-py3

RUN apt-get update && apt-get upgrade -y
RUN DEBIAN_FRONTEND='noninteractive' apt-get install -y git vim openssh-server \
        apache2 netcat libbz2-dev libreadline-dev libssl-dev libfftw3-dev \
        build-essential libgtest-dev opencl-headers ocl-icd-opencl-dev \
        autoconf libtool && \
        apt-get clean all

RUN ssh-keygen -A
RUN a2enmod userdir

# Install python 3.6 with pyenv
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
ENV HOME=/root/
ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH
RUN pyenv install 3.6.8 && pyenv global 3.6.8

# Install cmake from source (package manager version is too old)
WORKDIR /opt/cmake
RUN wget https://github.com/Kitware/CMake/releases/download/v3.19.1/cmake-3.19.1.tar.gz && \
    tar xzvf cmake-3.19.1.tar.gz && cd cmake-3.19.1 && ./bootstrap && make -j 8 && make install

# Install Google Test
WORKDIR /usr/src/gtest/build
RUN cmake .. && make -j 8 && cp *.a /usr/lib

# Install psrdada
RUN git clone git://git.code.sf.net/p/psrdada/code /opt/psrdada
WORKDIR /opt/psrdada
RUN autoreconf --install --force && ./configure --enable-shared && make -j 8 && make install

# Install AMBER with PSRDADA support
ENV PSRDADA=1
ENV CPLUS_INCLUDE_PATH=/usr/local/cuda/targets/x86_64-linux/include
ENV LIBRARY_PATH=/usr/local/cuda/targets/x86_64-linux/lib:$LIBRARY_PATH
WORKDIR /opt/amber
RUN git clone https://github.com/TRASAL/AMBER_setup.git
ENV SOURCE_ROOT=/opt/amber/src
ENV INSTALL_ROOT=/usr/local
RUN /opt/amber/AMBER_setup/amber.sh install

# Install dadafilterbank
RUN git clone https://github.com/TRASAL/dadafilterbank.git /opt/dadafilterbank
WORKDIR /opt/dadafilterbank/_build
RUN cmake .. && make && make install

# Copy files and install ssh keys
COPY . /opt/darc/
RUN mkdir /root/.ssh &&  mv /opt/darc/docker/authorized_keys /root/.ssh/ &&  mv /opt/darc/docker/id_rsa /root/.ssh && chmod go-rwx /root/.ssh/id_rsa

# Install python packages including DARC
# need pybind11 before installing sigpyproc
# need sigpyproc before installing DARC
RUN pip3 install --upgrade pip && pip3 install pybind11 && \
    pip3 install git+https://github.com/FRBs/sigpyproc3 && pip3 install -e /opt/darc


ENTRYPOINT ["/opt/darc/docker/entrypoint.sh"]
