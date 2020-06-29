FROM prom/prometheus
ADD ./config.yml /etc/prometheus/config.yml
CMD [ "--config.file=/etc/prometheus/config.yml", "--web.listen-address=:9990" ]