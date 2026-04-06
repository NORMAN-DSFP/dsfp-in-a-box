FROM alpine:latest

RUN apk add --no-cache curl python3 unzip

COPY init-elasticsearch.sh /init-elasticsearch.sh
COPY reload-compounds.sh /reload-compounds.sh
COPY convert_bulk.py /convert_bulk.py
COPY mappings.json /mappings.json
COPY compounds-mappings.json /compounds-mappings.json

RUN sed -i 's/\r$//' /init-elasticsearch.sh
RUN sed -i 's/\r$//' /reload-compounds.sh
RUN chmod +x /init-elasticsearch.sh
RUN chmod +x /reload-compounds.sh
RUN chmod +x /convert_bulk.py

CMD ["/init-elasticsearch.sh"]
