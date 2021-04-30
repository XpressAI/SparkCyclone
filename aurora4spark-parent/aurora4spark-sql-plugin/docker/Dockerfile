FROM centos:8.2.2004

RUN yum install -y gcc gcc-c++ gcc-gfortran boost-devel
RUN yum install -y python3-pip python3-scipy python36-devel
RUN yum install -y numactl-devel java-1.8.0-openjdk-devel
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN rpm -i https://github.com/frovedis/frovedis/releases/download/v0.9.9/frovedis-0.9.9-rh8.x86_64.rpm
RUN yum install -y man glibc-common glibc-locale-source glibc-langpack-en && localedef -i en_US -f UTF-8 en_US.UTF-8
RUN yum install -y https://www.hpc.nec/repos/TSUBASA-soft-release-2.3-1.noarch.rpm
RUN yum update || true
RUN yum group install -y ve-devel
