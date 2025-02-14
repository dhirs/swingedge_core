FROM dhirs2020/swingedge:latest

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create the directory 
RUN mkdir -p /var/swingedge

# Set the working directory inside the container
WORKDIR /var/swingedge

# Set the python path for all module imports
ENV PYTHONPATH = "/var/swingedge"

# Copy the requirements file and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest files
COPY . ./

# Command to run the script
CMD ["python", "load_candles.py"]