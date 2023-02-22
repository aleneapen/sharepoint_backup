# sharepoint_backup
Script to backup sharepoint files/folders to an AWS S3 bucket

## Usage
1. Rename secrets_example.json to secrets.json and add correct configuration to the file. 
    1. This script depends on the Office365-REST-Python-Client module. See this page on getting credentials for the app: https://github.com/vgrem/Office365-REST-Python-Client#Installation
    2. Remember to set the AWS credentials
2. Install dependencies `pip install -r requirements.txt`
3. Run the script `python main.py PATH_TO_SECRETS_JSON`

The conditions for backup are: 

The file size is above a threshold (current default: 250Mb) AND the file’s last modification time is older than 90days (both values can be changed in secrets.json). 

OR: the file name starts with TOBACKUPAWS_ (can be changed in secrets.json) 

The backed up file will be moved to the recycle bin in Sharepoint. There will be a new file in the place of the backed up file with the following naming convention: INAWS_{old_file_name} (the prefix can be changed in secrets.json). The new file is a shortcut to view the file in AWS console (in browser).