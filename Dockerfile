# Start with the OpenJDK image
FROM openjdk:11-jre-slim AS java-build

# Switch to the Python image
FROM python:3.11-slim-buster

# Copy Java runtime from the OpenJDK image
COPY --from=java-build /usr/local/openjdk-11/ /usr/local/openjdk-11/
ENV PATH="/usr/local/openjdk-11/bin:${PATH}"
ENV JAVA_HOME="/usr/local/openjdk-11"

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "single_site_report_app.py"]