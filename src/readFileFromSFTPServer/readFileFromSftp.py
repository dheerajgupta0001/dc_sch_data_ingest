import paramiko
import os
import datetime as dt
import logging
import pandas as pd
# logging.basicConfig(level=logging.DEBUG)

def readSftpFie(sftphost, sftpRow, targetMonth: dt.datetime, isDC: bool, isSch: bool, isDayAhead: bool):
    # SFTP server details
    hostname = sftphost
    # hostname = '10.2.100.171'
    username = sftpRow['sftp_username']
    # username = 'chatt'
    password = sftpRow['sftp_password']
    port = 22  # Default SFTP port

    # Remote file path
    if isDayAhead:
        remote_dir = sftpRow['remote_day_ahead_dir']
    else:
        remote_dir = sftpRow['remote_dir']
    # generate file name
    targetDateStr = ''
    if not pd.isna(sftpRow['format']):
        targetDateStr = dt.datetime.strftime(targetMonth, sftpRow['format'])

    if isDC:
        remote_file = sftpRow['filename'].replace('{{dt}}', targetDateStr)
    if isSch:
        remote_file = sftpRow['sch_filename'].replace('{{dt}}', targetDateStr)
    if isDayAhead:
        remote_file = sftpRow['day_ahead_filename'].replace('{{dt}}', targetDateStr)
    # remote_file = '21102024_DC.csv'
    # full_remote_path = os.path.join(remote_dir, remote_file)
    full_remote_path = remote_dir+'/'+remote_file

    # Local file path (where you want to save the file on your Windows PC)
    if isDayAhead:
        local_dir = sftpRow['local_day_ahead_dir']
    else:
        local_dir = sftpRow['local_dir']
    # local_file = os.path.join(os.path.expanduser('~'), 'Desktop', '21102024_DC.csv')
    local_file = os.path.join(os.path.expanduser('~'), local_dir, remote_file)

    # Create an SSH client
    ssh = paramiko.SSHClient()

    # Automatically add the server's host key
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the SFTP server
        ssh.connect(hostname, port, username, password)
        
        # Create an SFTP client object
        sftp = ssh.open_sftp()
        
        # Get current working directory
        print(f"Current working directory: {sftp.getcwd()}")
        
        # List contents of the home directory
        # print("\nContents of home directory:")
        # for entry in sftp.listdir('.'):
        #     print(entry)
        
        # List contents of the Chatt_Intraday_DC_Sch_Files directory
        # print(f"\nContents of {remote_dir}:")
        try:
            dir_contents = sftp.listdir(remote_dir)
            # for entry in dir_contents:
            #     print(entry)
            
            # Check if file exists in the directory listing
            if remote_file in dir_contents:
                print(f"\nFile {remote_file} exists in the directory listing.")
                
                # Try to get file attributes
                # try:
                    # file_attr = sftp.stat(full_remote_path)
                    # print(f"File size: {file_attr.st_size} bytes")
                    # print(f"Last modified: {datetime.fromtimestamp(file_attr.st_mtime)}")
                    
                # Try to download the file'Chatt_Intraday_DC_Sch_Files\\18102024_DC.csv'
                try:
                    sftp.get(full_remote_path, local_file)
                    # sftp.get('Chatt_Intraday_DC_Sch_Files/21102024_DC.csv', local_file)
                    print(f"File downloaded successfully to {local_file}")
                except IOError as e:
                    print(f"Error downloading file: {str(e)}")
                    # Try to open and read the file
                    try:
                        with sftp.open(full_remote_path, 'r') as f:
                            print("First few lines of the file:")
                            for _ in range(5):
                                print(f.readline().strip())
                    except IOError as e:
                        print(f"Error reading file: {str(e)}")
            else:
                print(f"\nFile {remote_file} is not in the directory listing.")
        except IOError as e:
            print(f"Error accessing {remote_dir}: {str(e)}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the SFTP session and SSH connection
        if 'sftp' in locals():
            sftp.close()
        ssh.close()
