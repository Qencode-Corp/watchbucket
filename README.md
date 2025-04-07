# Watchbucket

Watchbucket is a Python-based service designed to interact with the Qencode API by launching transcoding jobs for each new videos found in a specific S3 folder.

## Installation

To install the required dependencies, run the following command:

```bash
pip install -r watchbucket/requirements.txt
```

## Configuration

Before running the service, you need to configure the necessary settings:

1. **Python Executable Path**  
   Specify the path to your Python executable in the `run_service.sh` script:
   ```bash
   PYTHON=/usr/bin/python3
   ```

2. **Repository Path**  
   Define the path to your clone of this repository in the `run_service.sh` script:
   ```bash
   ROOT_DIR=/home/user/watchbucket
   ```

3. **Qencode API Key**  
   Enter your Qencode API Key in `watchbucket/settings/qencode.py`:
   ```python
   QENCODE_API_KEY = 'abcd12345678'
   ```

4. **API Query JSON**  
   Customize your API query JSON in `watchbucket/query/query.json`. The following placeholders are available within the `query.json` template:
   - `{source_url}`: The URL to the source video file.
   - `{file_name}`: The file name of the source video, excluding path and extension.

5. **S3 configuration**
   Provide your S3 host, bucket and and credentials in `watchbucket/settings/s3.py`. 
   ```python
   S3_HOST = 's3.us-east-1.amazonaws.com'
   S3_BUCKET = 'bucketname'
   S3_KEY = '123456789'
   S3_SECRET = '123456789'
   ```

   Specify the folder to monitor in `watchbucket/settings/system.py`:
   ```python
   INPUT_PATH = 'input' # S3 folder with input videos
   PROCESSED_PATH = 'processed' # Contains data for files from input folder which were already processed.
   ```

## Running the Service

To run the service, execute the `run_service.sh` script from the command line:

```bash
./service.sh start
```

You can stop the service using the following command:
```bash
./service.sh stop
```

To restart:
```bash
./service.sh restart
```


Ensure that you have the necessary permissions to execute the script.
