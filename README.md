# dynamodb_full_extraction
This Repo provides the script for converting dynamodb s3 export data  to normalised parquet format using python.

## Requirements
* Python 3.11
* Python Library Requirements
   * boto3
   * pandas
   * gzip
   * dynamodb_json
   * swifter
  
## Setup
Ensure you have Python installed

* Install libraries

  ```
  pip install -r requirements.txt
  ```

* Update Aws Credentials , S3 source and target bucket in **script.py**  {line number 12,13,14,15}

## Usage

* Run **script.py** 
 
Note : Uncomment **'os.remove'** code from '**script.py**' based on your requirements


## Contact

If you have any questions, need help, or want to discuss this project, feel free to reach out:

[Varun Bainsla](https://www.linkedin.com/in/varunbainsla/)
