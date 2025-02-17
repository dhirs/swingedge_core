# Use the Bitnami Spark image as a base
FROM bitnami/spark:3.1.2

# Switch to root user to install dependencies
USER root

# Install system dependencies (including git, PostgreSQL development libraries)
RUN mkdir -p /var/lib/apt/lists/partial && apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    wget \
    python3-pip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (including PySpark)
COPY requirements.txt ./ 
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the working directory inside the container
WORKDIR /var/swingedge

# Copy the rest of the files (including your Python code)
COPY . ./ 

# Set environment variables for Java and Spark (ensure Spark is available in PATH)
ENV JAVA_HOME=/opt/bitnami/java
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$PATH:/opt/bitnami/spark/bin
ENV PYTHONPATH="/var/swingedge"

# Command to run the script
CMD ["python3", "load_candles.py"]
