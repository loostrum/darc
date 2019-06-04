FROM tensorflow/tensorflow:1.12.0-gpu-py3

RUN apt-get update && apt-get upgrade -y
RUN DEBIAN_FRONTEND='noninteractive' apt-get install -y git vim openssh-server \
        apache2 netcat
RUN apt-get clean all

RUN ssh-keygen -A
RUN a2enmod userdir

# Install ssh keys and DARC
COPY . /root/darc/
RUN mkdir /root/.ssh &&  mv /root/darc/docker/authorized_keys /root/.ssh/ &&  mv /root/darc/docker/id_rsa /root/.ssh && chmod go-rwx /root/.ssh/id_rsa
RUN pip3 install --upgrade pip; pip3 install /root/darc

ENTRYPOINT ["/root/darc/docker/entrypoint.sh"]
