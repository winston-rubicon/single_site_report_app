# Use an official Python runtime as a base image
FROM python:3.11-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Upgrade pip before proceeding
RUN pip install --upgrade pip
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8501 available to the world outside this container
EXPOSE 8501

# Define environment variable (optional)
# ENV NAME=World

# Run the Streamlit app
CMD ["streamlit", "run", "single_site_report_app.py"]