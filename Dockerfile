FROM alpine

RUN apk add --update \
    python3 \
  && easy_install-3.5 pip \
  && rm -rf /var/cache/apk/*

WORKDIR /
COPY clean_empty.py /
COPY requirements.txt requirements.txt
RUN pip install -r /requirements.txt
ENTRYPOINT ["python", "/clean_empty.py"]
