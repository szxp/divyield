FROM haproxy:latest-divyield

ARG STATE
ARG VIP

USER root

COPY haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg

COPY keepalived-$STATE.conf /usr/local/etc/keepalived/keepalived.conf

RUN set -eux; \
	sed -i -e "s;\$VIP;${VIP};g" /usr/local/etc/keepalived/keepalived.conf; \
	chmod -x /usr/local/etc/keepalived/keepalived.conf;

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD

