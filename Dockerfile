FROM ubuntu:oracular

# Update and install dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get -yq install python3-pip python3-venv

# Set up working directory
WORKDIR /app

# Copy necessary files and directories
COPY requirements.txt /app/
ADD system /app/system

# remove imports below when running on kubernetes
# ADD data /app/data
# ADD tests /app/tests

RUN python3 -m venv /app/venv && /app/venv/bin/pip install -r /app/requirements.txt

ENV MLLP_ADDRESS='host.docker.internal:8440'
ENV PAGER_ADDRESS='host.docker.internal:8441'
ENV HISTORY_PATH='/app/data/history.csv'
ENV DB_PATH='/app/data/hospital_aki.db'
ENV PYTHONUNBUFFERED=1

# Expose necessary ports
EXPOSE 8440
EXPOSE 8441
# for Prometheus metrics
EXPOSE 8000

WORKDIR /app/system

ENTRYPOINT ["/app/venv/bin/python3", "/app/system/main.py"]
CMD ["--database_path=${DB_PATH}", "--history=${HISTORY_PATH}", "--mllp_address=${MLLP_ADDRESS}", "--pager_address=${PAGER_ADDRESS}"]