FROM alpine

RUN apk add --update \
    python \
    py-pip \
  && rm -rf /var/cache/apk/*

COPY clean_empty.py /usr/bin
COPY requirements.txt requirements.txt
RUN pip install -r /requirements.txt
CMD ["python", "/usr/bin/clean_empty.py"]
