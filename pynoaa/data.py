import os
import logging
import threading
import gzip
import shutil
from ftplib import FTP
from ftplib import error_perm, error_reply

import ish

SERVER_URL = "ftp.ncdc.noaa.gov"
USER = "anonymous"
PASSWORD = ""
NOAA_BASE_DIR = "/pub/data/noaa/"
LOCAL_DATA = "./data/raw/"
LOCAL_DATA_RAW_DIR = "raw/"
LOCAL_DATA_DECOMPRESS = "decompress/"
LOCAL_DATA_OUTPUT = "../../output/"
LOCAL_DATA_OUTPUT_ISH = "../../output-ish/"

MAX_NUM_JOBS = 4
NUM_RETRIES = 3

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
pool_semaphore = threading.BoundedSemaphore(value=MAX_NUM_JOBS)


class YearDataError(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return self.code


class YearData(threading.Thread):
    def __init__(self, year, ish=True):
        super(YearData, self).__init__()
        self.year = year
        self.ish = ish
        self.name = "year:{0}".format(year)
        self.raw_data_dir = LOCAL_DATA + str(year) + "/" + LOCAL_DATA_RAW_DIR
        self.raw_data_uncompressed_dir = LOCAL_DATA + str(year) + "/" + LOCAL_DATA_DECOMPRESS
        self.output_data_dir = LOCAL_DATA + str(year) + "/" + LOCAL_DATA_OUTPUT
        self.output_ish_data_dir = LOCAL_DATA + str(year) + "/" + LOCAL_DATA_OUTPUT_ISH
        self.remote_year_path = NOAA_BASE_DIR + str(year) + "/"
        self.output_file = None
        self.output_file_ish = None
        self.ftp = None
        self.remote_files = dict()
        self.remote_files_total_size = 0
        self.remote_files_total_num = 0
        self.pending_files = dict()
        self.pending_files_total_size = 0
        self.pending_files_total_num = 0
        self.files = dict()
        self.files_decompressed = list()
        self.files_not_downloaded = list()

    def run(self):
        with pool_semaphore:
            try:
                self.connect()

                # create directories
                self.create_directory(self.raw_data_dir)
                self.create_directory(self.raw_data_uncompressed_dir)
                self.create_directory(self.output_data_dir)
                self.create_directory(self.output_ish_data_dir)

                attempt = 0
                while True:
                    self.get_list_pending_files()
                    if self.is_all_data_downloaded():
                        logger.info("All files has been downloaded")
                        break
                    elif 0 < attempt < NUM_RETRIES:
                        logger.warning(
                            "Not all files have been downloaded, retrying missing. Attempt number: {0}".format(attempt))
                    elif attempt == NUM_RETRIES:
                        err_text = "Aborting after {0} attempts, {1} files missing".format(attempt,
                                                                                           self.pending_files_total_num)
                        logger.error(err_text)
                        raise YearDataError(err_text)
                    self.download_files()
                    attempt += 1

                # decompress files
                self.decompress()
                # merge files
                self.merge()
                # convert to ish
                if self.ish:
                    logger.info("Building ish output")
                    self.output_file_ish = self.output_ish_data_dir + str(self.year) + "_ish"
                    ish.convert(self.output_file, self.output_file_ish)
            except YearDataError as err:
                logger.error(err)
            finally:
                self.disconnect()

    def connect(self):
        try:
            self.ftp = FTP(host=SERVER_URL)
            self.ftp.login(user=USER, passwd=PASSWORD, )
            logger.info("Login to FTP successfully")
        except error_perm:
            logger.error("Login to FTP failed")
            return 1

    def disconnect(self):
        try:
            self.ftp.quit()
            logger.info("Disconnected from FTP successfully")
        except error_reply:
            self.ftp.close()

    def get_list_remote_files(self):
        # change directory
        self.ftp.sendcmd(cmd="CWD " + self.remote_year_path)
        # get the list of all remote files
        remote_list = self.ftp.mlsd(path=self.remote_year_path)
        # clear previous list
        self.remote_files.clear()
        self.remote_files_total_size = 0
        self.remote_files_total_num = 0
        for filename, metadata in remote_list:
            if metadata['type'] == 'file':
                self.remote_files[filename] = metadata
                self.remote_files_total_size += int(metadata["size"])
                self.remote_files_total_num += 1

    def get_list_pending_files(self):
        # get the list of files from remote server
        try:
            self.get_list_remote_files()
        except error_perm as err:
            err_text = "Error while getting remote list directory: {0}".format(err)
            logger.error(err_text)
            raise YearDataError(err_text)

        self.pending_files = self.remote_files.copy()
        self.pending_files_total_num = self.remote_files_total_num
        self.pending_files_total_size = self.remote_files_total_size

        # exclude downloaded files and build a list containing only pending files to be downloaded
        try:
            for file in os.listdir(self.raw_data_dir):
                if file in self.remote_files and int(self.remote_files[file]["size"]) == int(
                        os.stat(self.raw_data_dir + file).st_size):
                    self.pending_files_total_size -= int(self.remote_files[file]["size"])
                    self.pending_files_total_num -= 1
                    self.pending_files.pop(file)  # file already downloaded
                    if file not in self.files.keys():
                        self.files[file] = self.remote_files[file]
        except error_perm as err:
            err_test = "Error while getting list of pending files: {0}".format(err)
            logger.error(err_test)
            raise YearDataError(err_test)

    def download_files(self):
        logger.info("Ready for downloading {0} files, {1} bytes".format(self.pending_files_total_num,
                                                                        self.pending_files_total_size))
        for file, metadata in self.remote_files.items():
            try:
                with open(self.raw_data_dir + file, "wb") as f:
                    cmd = 'RETR {fname}'.format(fname=file)
                    self.ftp.retrbinary(cmd, f.write)
                self.files[file] = metadata
            except error_perm as err:
                logger.error("Error downloading file: {0}".format(err))
                self.files_not_downloaded.append((file, metadata))

    def is_all_data_downloaded(self):
        if not os.path.exists(self.raw_data_dir):
            return False

        if self.pending_files is None or len(list(self.pending_files.keys())) == 0:
            return True
        else:
            return False

    def decompress(self):
        logger.info("Decompressing files")
        for file, _ in self.files.items():
            new_filename = str(self.raw_data_uncompressed_dir + file).replace(".gz", "")
            with gzip.open(self.raw_data_dir + file, 'rb') as fr, open(new_filename, 'wb') as fw:
                fw.write(fr.read())
            self.files_decompressed.append(new_filename)

    def merge(self):
        logger.info("Merging decompressed files")
        self.output_file = self.output_data_dir + str(self.year)
        with open(self.output_file, 'wb') as fw:
            for file in self.files_decompressed:
                with open(file, 'rb') as fr:
                    shutil.copyfileobj(fr, fw)

    @staticmethod
    def create_directory(directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.debug("Creating new directory {0}".format(directory))
        except PermissionError as err:
            err_text = "Error creating directory: {0}".format(err)
            logger.error(err_text)
            raise YearDataError(err_text)


def main():
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    jobs = list()
    for i in range(1901, 1904):
        y = YearData(i, ish=True)
        y.start()
        jobs.append(y)

    for j in jobs:
        j.join()


if __name__ == "__main__":
    main()