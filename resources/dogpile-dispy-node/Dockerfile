FROM python:3.8.17-bookworm
MAINTAINER jason.a.diamond@gmail.com

RUN /usr/bin/apt-get -y update

RUN /usr/bin/apt-get -y install tzdata
ENV TZ="America/Vancouver"

#debug and testing tools
RUN /usr/bin/apt-get -y install nano telnet netbase dnsutils netcat-traditional

RUN /usr/bin/apt-get -y upgrade

#clean up apt
RUN /usr/bin/apt-get clean && rm -rf /var/lib/apt/lists

RUN /usr/local/bin/python -V

RUN /usr/sbin/useradd -m dispy

USER dispy

# image installs python 3.8.6 to /usr/local/bin/

# venv for our user
RUN /usr/local/bin/python -m venv /home/dispy/venv_dispy

# dispy and dogpile dependencies
RUN /home/dispy/venv_dispy/bin/python -m pip install --upgrade pip
RUN /home/dispy/venv_dispy/bin/python -m pip install wheel
RUN /home/dispy/venv_dispy/bin/python -m pip install dispy==4.15.2 pycos==4.12.2 pillow psutil 
RUN /home/dispy/venv_dispy/bin/python -m pip install numpy numba

# set up the endpoint
USER root

COPY start.sh /home/dispy/start.sh
RUN /bin/chmod +x /home/dispy/start.sh
RUN /bin/chown dispy:dispy /home/dispy/start.sh

USER dispy

ENTRYPOINT /home/dispy/start.sh
