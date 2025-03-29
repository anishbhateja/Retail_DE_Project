# Use an official Python image as the base image
FROM python:3.7-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for AWS credentials (optional, for local testing)
ENV AWS_PROFILE=dev_user_anish
ENV AWS_REGION=ap-south-1

# Command to run your script
CMD ["python", "test_data_generator.py"]
