import os
import shutil
import logging
from datetime import datetime

# Configure logging
log_file = "/path/to/your/logfile.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def create_folder(folder_path, permissions=0o755):
    try:
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
            os.chmod(folder_path, permissions)
            logging.info(f"Folder '{folder_path}' created with permissions '{oct(permissions)}'.")
        else:
            logging.warning(f"Folder '{folder_path}' already exists.")
    except Exception as e:
        logging.error(f"Failed to create folder '{folder_path}': {e}")

def copy_files(source_files, destination_folder):
    try:
        for file in source_files:
            if os.path.isfile(file):
                shutil.copy(file, destination_folder)
                logging.info(f"File '{file}' copied to '{destination_folder}'.")
            else:
                logging.warning(f"File '{file}' does not exist and was not copied.")
    except Exception as e:
        logging.error(f"Failed to copy files to '{destination_folder}': {e}")

def main():
    folder_path = "/path/to/your/new/folder"  # Replace with your folder path
    permissions = 0o755  # Folder permissions

    source_files = [
        "/path/to/source/file1.txt",
        "/path/to/source/file2.txt"
    ]  # List of files to copy

    # Create folder and assign permissions
    create_folder(folder_path, permissions)

    # Copy files to the folder
    copy_files(source_files, folder_path)

if __name__ == "__main__":
    main()

