# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install any needed packages specified in requirements.txt
COPY /app/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

#copy all the other main files
COPY /app/*.py /app/

# Make port 8084 available to the world outside this container
EXPOSE 8084

# Run the gas monitoring script when the container launches
CMD ["python", "linde_manager.py", "--path", "/etc/linde", "--notify", "--port", "8084", "--debug"]
