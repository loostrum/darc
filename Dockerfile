FROM tensorflow/tensorflow:1.12.0-gpu-py3

RUN apt-get update && apt-get upgrade -y
RUN DEBIAN_FRONTEND='noninteractive' apt-get install -y git vim openssh-server \
        apache2 netcat
RUN apt-get clean all

RUN ssh-keygen -A
RUN a2enmod userdir

# Install DARC
COPY . /root/darc/
RUN mkdir /root/.ssh
RUN mv /root/darc/docker/authorized_keys /root/.ssh/
RUN mv /root/darc/docker/id_rsa /root/.ssh
RUN chmod go-rwx /root/.ssh/id_rsa

RUN pip3 install --upgrade pip
RUN pip3 install /root/darc

ENTRYPOINT ["/root/darc/docker/entrypoint.sh"]
