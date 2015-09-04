FROM debian:jessie

RUN apt-get update && \
    apt-get install -y beanstalkd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

CMD ["/usr/bin/beanstalkd", "-f", "60000", "-b", "/data"]
