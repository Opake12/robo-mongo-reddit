# Use the official Python image as the base image
FROM python:3.10-slim

# Set the working directory
WORKDIR /app

# Install the required dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev

# Copy requirements.txt into the container
COPY requirements.txt .


# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Run the app.py script when the container launches
CMD ["python", "app.py"]
