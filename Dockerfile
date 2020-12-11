FROM nvidia/cuda:9.0-cudnn7-devel-ubuntu16.04

RUN apt-get update && apt-get upgrade -y
RUN DEBIAN_FRONTEND='noninteractive' apt-get install -y git vim openssh-server \
        apache2 netcat libbz2-dev libreadline-dev libssl-dev libfftw3-dev \
        build-essential libgtest-dev opencl-headers ocl-icd-opencl-dev \
        autoconf libtool curl && \
        apt-get clean all

RUN ssh-keygen -A
RUN a2enmod userdir

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

# set some env vars
ENV PSRDADA=1
ENV CPLUS_INCLUDE_PATH=/usr/local/cuda/targets/x86_64-linux/include
ENV PATH=/usr/local/cuda/bin:$PATH
ENV LIBRARY_PATH=/usr/local/cuda/targets/x86_64-linux/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=/usr/local/cuda/targets/x86_64-linux/lib:/usr/local/cuda/targets/x86_64-linux/lib/stubs:$LD_LIBRARY_PATH
# tensorflow checks for libcuda.so.1 explicitly, symlink to libcuda.so
RUN ln -s /usr/local/cuda/targets/x86_64-linux/lib/stubs/libcuda.so /usr/local/cuda/targets/x86_64-linux/lib/stubs/libcuda.so.1

# Install AMBER with PSRDADA support
WORKDIR /opt/amber
RUN git clone https://github.com/TRASAL/AMBER_setup.git
ENV SOURCE_ROOT=/opt/amber/src
ENV INSTALL_ROOT=/usr/local
RUN /opt/amber/AMBER_setup/amber.sh install

# Install dadafilterbank
RUN git clone https://github.com/TRASAL/dadafilterbank.git /opt/dadafilterbank
WORKDIR /opt/dadafilterbank/_build
RUN cmake .. && make && make install

# create local user
#RUN useradd -u 1006 -ms /bin/bash arts
RUN useradd -u 1000 -ms /bin/bash arts
ENV HOME=/home/arts
USER arts
WORKDIR $HOME
 
# Install python 3.6 with pyenv
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/bin:$PYENV_ROOT/shims:$PATH
ENV PYENV_VERSION=3.6.8
RUN pyenv install $PYENV_VERSION && pyenv global $PYENV_VERSION

# Install python packages
RUN pip3 install --upgrade pip && pip3 install pybind11 && \ 
    pip3 install git+https://github.com/FRBs/sigpyproc3 tensorflow-gpu==1.12

# Copy files and install ssh keys
COPY . /opt/darc/
RUN mkdir $HOME/.ssh &&  cp /opt/darc/docker/authorized_keys $HOME/.ssh/ &&  cp /opt/darc/docker/id_rsa $HOME/.ssh && chmod go-rwx $HOME/.ssh/id_rsa

# Install darc
RUN pip3 install /opt/darc

# Add pyenv and cuda libs to arts bashrc
RUN echo 'export PATH=/home/arts/.pyenv/bin:/home/arts/.pyenv/shims:$PATH' >> .bashrc && \
    echo 'export PATH=/usr/local/cuda/bin:$PATH' >> .bashrc && \
    echo 'export LD_LIBRARY_PATH=/usr/local/cuda/targets/x86_64-linux/lib:/usr/local/cuda/targets/x86_64-linux/lib/stubs:$LD_LIBRARY_PATH' >> .bashrc && \
    echo 'eval "$(pyenv init -)"' >> .bashrc

# Run entrypoint as root because it starts services
# entrypoint runs CMD as arts
USER root
ENTRYPOINT ["/opt/darc/docker/entrypoint.sh"]
CMD ["/home/arts/.pyenv/shims/darc_service"]
