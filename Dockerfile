# Start with the OpenJDK image
FROM openjdk:11-jre-slim AS java-build

# Switch to the Python image
FROM python:3.11-slim-buster

# Copy Java runtime from the OpenJDK image
COPY --from=java-build /usr/local/openjdk-11/ /usr/local/openjdk-11/
ENV PATH="/usr/local/openjdk-11/bin:${PATH}"
ENV JAVA_HOME="/usr/local/openjdk-11"

WORKDIR /app

# Some extra steps required for proper fonts in plotly
# Install fontconfig
RUN apt-get update && \
    apt-get install -y fontconfig && \
    rm -rf /var/lib/apt/lists/*

# Copy the custom font into the container
COPY branding/fonts/AtlasGrotesk*.ttf /usr/share/fonts/truetype/custom/

# Refresh the font cache
RUN fc-cache -fv

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "single_site_report_app.py"]