FROM --platform=linux/amd64 public.ecr.aws/sam/build-python3.10:latest

WORKDIR /tmp

COPY event_bridge/runtime /tmp/event_bridge

RUN pip install -r /tmp/event_bridge/requirements.txt -t /asset
RUN pip install --upgrade pip   
RUN pip install "psycopg[binary,pool]" -t /asset
RUN rm -rf /tmp/event_bridge

# Reduce package size and remove useless files
RUN cd /asset && find . -type f -name '*.pyc' | while read f; do n=$(echo $f | sed 's/__pycache__\///' | sed 's/.cpython-[2-3][0-9]//'); cp $f $n; done;
RUN cd /asset && find . -type d -a -name '__pycache__' -print0 | xargs -0 rm -rf
# RUN cd /asset && find . -type f -a -name '*.py' -print0 | xargs -0 rm -f
RUN find /asset -type d -a -name 'tests' -print0 | xargs -0 rm -rf

COPY event_bridge/runtime/handler.py /asset/handler.py

CMD ["echo", "hello world"]
