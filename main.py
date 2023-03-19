from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.files.file import File

import time
import os
import shutil
import datetime
import boto3
import json
import sys
import urllib.parse


SETTINGS = {}

if len(sys.argv) < 2:
    raise RuntimeError("You need to provide a settings (secrets) json file")
else:
    secrets_filepath = sys.argv[1]
    with open(secrets_filepath, "r") as f:
        SETTINGS = json.load(f)

if __name__ == "__main__":

    if not SETTINGS:
        raise RuntimeError("Settings file could not be found. Try again.")

    ### VARIABLES TO SET FOR THE SCRIPT
    
    BACKUP_DIR = SETTINGS.get("BACKUP_DIR")

    backup_criteria_settings = SETTINGS.get("BACKUP_CRITERIA", {})
    DAYS_BEFORE_FILE_SEND = backup_criteria_settings.get("DAYS_BEFORE_FILE_SEND")
    FILESIZE_CUTOFF_BYTES = backup_criteria_settings.get("FILESIZE_CUTOFF_BYTES")

    sharepoint_settings = SETTINGS.get("SHAREPOINT_SETTINGS", {})
    PREFIX_FOR_BACKUP = sharepoint_settings.get("PREFIX_FOR_BACKUP")
    PREFIX_AFTER_BACKUP = sharepoint_settings.get("PREFIX_AFTER_BACKUP")
    PREFIX_AFTER_BACKUP_FOLDER = sharepoint_settings.get("PREFIX_AFTER_BACKUP_FOLDER")
    SP_CLIENT_ID = sharepoint_settings.get("CLIENT_ID")
    SP_CLIENT_SECRET = sharepoint_settings.get("CLIENT_SECRET")
    SITE_URL = sharepoint_settings.get("SITE_URL")
    ROOT_FOLDER_NAME = sharepoint_settings.get("ROOT_FOLDER_NAME")
    
    aws_settings = SETTINGS.get("AWS_SETTINGS", {})
    BACKUP_BUCKET_NAME = aws_settings.get("BACKUP_BUCKET_NAME")
    AWS_CLIENT_ID = aws_settings.get("AWS_CLIENT_ID")
    AWS_SECRET_ACCESS_KEY = aws_settings.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION_NAME = aws_settings.get("AWS_REGION_NAME")

    SECONDS_BEFORE_RETRY = 5
    RETRY_AMOUNT = 10

    ### END OF VARIABLES




    
    client_credentials = ClientCredential(SP_CLIENT_ID,SP_CLIENT_SECRET)
    ctx = ClientContext(SITE_URL).with_credentials(client_credentials)
    
    
    root_folder_path = "/" + "/".join(SITE_URL.split("sharepoint.com/")[1:]) + ROOT_FOLDER_NAME
    s3 = boto3.resource('s3', aws_access_key_id = AWS_CLIENT_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    



    # ctx.load(folders)

    def make_tarfile(output_filename, source_dir):
        return shutil.make_archive(output_filename, 'zip', source_dir)

    def process_folder(folder: Folder, curr_backup_folder):
        curr_backup_folder = f"{curr_backup_folder}/{folder.get_property('name')}"
        if not os.path.exists(curr_backup_folder):
            os.mkdir(curr_backup_folder)
        return curr_backup_folder


    

    def send_to_s3(file_path, bucket_name, aws_file_name):
        s3.meta.client.upload_file(file_path,BACKUP_BUCKET_NAME,aws_file_name)
        url_first = f"https://s3.console.aws.amazon.com/s3/object/{bucket_name}?"
        params = {
            "region": AWS_REGION_NAME,
            "prefix": aws_file_name
        }
        url_last = urllib.parse.urlencode(params)
        new_sp_file_content = f"[InternetShortcut]\nURL={url_first}{url_last}"
        return new_sp_file_content
    

    def file_download(file, f, file_path):
        success = False

        def print_download_progress(offset):
            print("Downloaded '{}' bytes... of: {}".format(offset, file_path))

        try_number = 1
        while (not success and try_number <= RETRY_AMOUNT):
            try:    
                file.download_session(f, print_download_progress)
                ctx.execute_query()
                success = True
            except Exception as e:
                print(f"Error when executing query to download file retrying in {SECONDS_BEFORE_RETRY} seconds")
                success = False
                try_number += 1
                time.sleep(SECONDS_BEFORE_RETRY)

    def process_file(file: File, curr_backup_path, ctx: ClientContext, sharepoint_folder: Folder, transfer_folder_ongoing = False):
        transfer_file: bool = False

        converted_file_name: str = file.name.replace(".","_").replace(" ","_").replace("+","") if file.name else ""

        if converted_file_name == "":
            return

        if converted_file_name.startswith(PREFIX_FOR_BACKUP):
            transfer_file = True
            converted_file_name = converted_file_name.replace(PREFIX_FOR_BACKUP,"")

        file_name = f"{PREFIX_AFTER_BACKUP}{converted_file_name}.url"

        if transfer_folder_ongoing:
            file_path = f"{curr_backup_path}/{file.name}"
        else:
            file_path = f"{curr_backup_path}/{converted_file_name}"
        
        timediff = datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.strptime(file.time_created,"%Y-%m-%dT%H:%M:%S%z")


        if (file.length > FILESIZE_CUTOFF_BYTES and timediff.days > DAYS_BEFORE_FILE_SEND):
            transfer_file = True
        
        

        if transfer_folder_ongoing:
            with open(file_path,"wb+") as f:
                file_download(file,f,file_path)
                
        elif transfer_file:
            filesize_gb = file.length/1000000000
            print(f"file: {file.name} file size: {filesize_gb} GB")
            with open("transferred_files.txt", "a+") as f:
                f.write(f"file: {file.resource_url} file size: {filesize_gb} GB\n")

            with open(file_path,"wb+") as f:
                file_download(file,f,file_path)

            # Upload to AWS
            aws_file_name = file.name.replace(" ","").replace("+","") if file.name else ""

            aws_file_name = f"{file.unique_id}_{aws_file_name}"

            success = False
            try_number = 1
            while (not success and try_number <= RETRY_AMOUNT):
                try:
                    new_sp_file_content = send_to_s3(file_path,BACKUP_BUCKET_NAME,aws_file_name)

                    # Add link to SP
                    sharepoint_folder.upload_file(file_name,bytes(new_sp_file_content, 'utf-8'))

                    # Recycle file
                    file.recycle()
                    ctx.execute_query()
                    success = True
                except Exception as e:
                    print(f"Error {e}")
                    print(f"Error when executing query to upload dummy and recycle old file. Retrying in {SECONDS_BEFORE_RETRY} seconds")
                    success = False
                    try_number += 1
                    time.sleep(SECONDS_BEFORE_RETRY)

            # Remove local copy
            os.remove(file_path)
    


    # upload_folders = SETTINGS.get("UPLOAD_FOLDERS", {})
    should_backup_root_files = SETTINGS.get("BACKUP_ROOT_FILES", True)
    # should_zip_folders = SETTINGS.get("ZIP_FODLERS", True)

    def recursive_process(ctx: ClientContext, root_folder_name, backup_path, root_folder_path = None, transfer_folder_ongoing = False):
        root_folder = ctx.web.get_folder_by_server_relative_url(root_folder_name).expand(["Files","Folders"])

        success = False
        try_number = 1
        while (not success and try_number <= RETRY_AMOUNT):
            try:
                ctx.load(root_folder)
                ctx.execute_query()
                success = True
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)
                try_number += 1
                if try_number > RETRY_AMOUNT:
                    print("Can't load root folder, stopping at this folder")
                    return

        # TODO: change
        if transfer_folder_ongoing:
            backup_path = process_folder(root_folder,backup_path)


        # # Get all files with the prefix
        # if root_folder_path:
        #     all_files = root_folder.get_files(recursive=True)
        #     for each_file in all_files:
        #         process_file(each_file, backup_path, ctx, each_file.get_property("fol"))

        file: File
        for file in root_folder.get_property("files"):
            if root_folder_path and not should_backup_root_files:
                break
            process_file(file, backup_path, ctx, root_folder, transfer_folder_ongoing)

        folder: Folder
        for folder in root_folder.get_property("folders"):
            
            # If a folder, make a folder in the folder directory
            new_root_folder = f"{root_folder_name}/{folder.get_property('name')}"

            if transfer_folder_ongoing == False and folder.get_property('name').startswith(PREFIX_FOR_BACKUP):
                print(f"folder to zip and send: {new_root_folder}")
                recursive_process(ctx, new_root_folder, backup_path, None,transfer_folder_ongoing=True)
                tar_file_name = f'{BACKUP_DIR}/{folder.unique_id}'

                folder_backup_path = f"{backup_path}/{folder.get_property('name')}"

                output_filename = make_tarfile(tar_file_name, folder_backup_path)
                aws_file_name = f"FOLDER_{folder.unique_id}.zip"
                new_sp_file_content = send_to_s3(output_filename,BACKUP_BUCKET_NAME,aws_file_name)

                file_name = folder.get_property('name').replace(PREFIX_FOR_BACKUP,"").replace(" ", "_")
                 # Add link to SP
                folder.parent_folder.upload_file(f"{PREFIX_AFTER_BACKUP_FOLDER}{file_name}.url",bytes(new_sp_file_content, 'utf-8'))

                # Recycle folder
                folder.recycle()
                ctx.execute_query()

                # Remove local copy
                shutil.rmtree(folder_backup_path)
                os.remove(output_filename)

                transfer_folder_ongoing=False
            else:
                print(f"Processing folder: {new_root_folder} transfer status {transfer_folder_ongoing}")
                recursive_process(ctx, new_root_folder, backup_path, None,transfer_folder_ongoing=transfer_folder_ongoing)
        
    recursive_process(ctx,ROOT_FOLDER_NAME,BACKUP_DIR, root_folder_path)