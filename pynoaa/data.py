import os
import logging
import threading
import gzip
import shutil
import socket
from datetime import date
from ftplib import FTP
from ftplib import error_perm, error_reply

import ish

SERVER_URL = "ftp.ncdc.noaa.gov"
SERVER_PORT = 21
USER = "anonymous"
PASSWORD = ""
NOAA_BASE_DIR = "/pub/data/noaa/"
LOCAL_DATA = "./data/raw/"
LOCAL_DATA_RAW_DIR = "raw/"
LOCAL_DATA_DECOMPRESS = "decompress/"
LOCAL_DATA_OUTPUT = "../../output/"
LOCAL_DATA_OUTPUT_ISH = "../../output-ish/"

MAX_NUM_JOBS = 4             # number of parallel tasks (this is not the number of concurrent downloads)
MAX_NUM_FTP_CONNECTIONS = 1  # limited by NOAA server to only 1
NUM_RETRIES = 3              # retries for trying to retrieve all data from a given year
FTP_CONN_TIMEOUT = 1        # ftp connection timeout in seconds

pool_semaphore = threading.BoundedSemaphore(value=MAX_NUM_JOBS)
ftp_semaphore = threading.BoundedSemaphore(value=MAX_NUM_FTP_CONNECTIONS)

logger = logging.getLogger(__name__)
formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)


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
                        logger.info("All files have been downloaded")
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
                # finished downloading files, free ftp connection
                self.disconnect()
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
                if self.ftp is not None:
                    self.disconnect()

    def connect(self):
        try:
            ftp_semaphore.acquire(blocking=True)
            self.ftp = FTP(timeout=None)
            self.ftp.connect(host=SERVER_URL, port=SERVER_PORT, timeout=FTP_CONN_TIMEOUT)
            self.ftp.login(user=USER, passwd=PASSWORD, )
            self.ftp.set_pasv(False)
            logger.info("Login to FTP successfully")
        except socket.timeout as err:
            self.ftp = None
            ftp_semaphore.release()
            err_test = "Can't connect to server: {0}".format(err)
            logger.error(err_test)
            raise YearDataError(err_test)
        except error_perm as err:
            err_test = "Login to FTP failed: {0}".format(err)
            logger.error(err_test)
            self.disconnect()
            raise YearDataError(err_test)

    def disconnect(self):
        try:
            self.ftp.quit()
            logger.info("Disconnected from FTP successfully")
        except (error_reply, OSError):
            self.ftp.close()
        finally:
            self.ftp = None
            ftp_semaphore.release()

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
        except (error_perm, socket.timeout, OSError) as err:
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
            new_file = self.raw_data_dir + file
            try:
                with open(new_file, "wb") as f:
                    cmd = 'RETR {fname}'.format(fname=file)
                    self.ftp.retrbinary(cmd, f.write)
                self.files[file] = metadata
            except error_perm as err:
                logger.error("Error downloading file: {0}".format(err))
                self.files_not_downloaded.append((file, metadata))
            except socket.timeout as err:
                logger.warning("Timeout downloading file: {0}".format(err))
                try:
                    os.remove(new_file)
                except OSError as err:
                    logger.warning("Couldn't delete file {0}: {1}".format(new_file, err))

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


def get_all():
    get_interval(1901, 1910)


def get_interval(from_year, to_year):

    if to_year < from_year or from_year < 1901 or to_year > date.today().year + 1:
        logger.error("Bad year interval, only valid: ({0}, {1})".format(1901, date.today().year))
        exit(1)

    jobs = list()
    for i in range(from_year, to_year + 1):
        y = YearData(i, ish=True)
        y.start()
        jobs.append(y)

    for j in jobs:
        j.join()


def get_year(year):
    y = YearData(year, ish=True)
    y.start()
    y.join()
