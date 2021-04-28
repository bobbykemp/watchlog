#!/usr/bin/python3.8 -u
import argparse
import concurrent.futures
import datetime
from hashlib import md5
import os
from pathlib import Path
import sys
import tempfile
import zipfile

import pyinotify as watch

watched_events = watch.IN_MOVED_TO

watchman = watch.WatchManager()

parser = argparse.ArgumentParser(description='A simple file watcher and unzipper')
parser.add_argument('-w', '--watch_paths', nargs='+', help='<Required> Set flag', required=True)
parser.add_argument('-e', '--extract_to', nargs='+', help='<Required> Set flag', required=True)

class WatchLog(watch.ProcessEvent):
    def __init__(self):
        super().__init__()

        args = parser.parse_args()

        self.file_ext_whitelist = [
            '.zip'
        ]
        self.extracted_file_names = {}

        # dict of paths to watch with corresponding directories to extract to of the form
        # <watch_path> : <extract_to_path>
        self.path_data = {}

        try:
            assert len(args.watch_paths) == len(args.extract_to)
        except AssertionError as e:
            print('Number of path args must equal number of name args')
            exit(1)

        e_fnames_file = open('.prev_extracted', 'w')

        for x in zip(args.watch_paths, args.extract_to):
            self.path_data[Path( x[0] )] = Path( x[1] )

        # all extract-to directories
        for dir_ in self.path_data.values():
            pre_file_count = 0
            self.extracted_file_names[dir_] = []
            for file_ in os.listdir(dir_):
                name, ext = os.path.splitext(file_)
                fname_no_ext = os.path.basename(name)
                dirname = os.path.dirname(os.path.join(dir_, file_))
                # NOTE: we are assuming that when the program extracts a file to the
                # target directory, that extracted file will be correct/fully extracted
                if os.path.isfile(os.path.join(dir_, file_)) and ext == '.txt':
                    # add the filename of this file to the list of files
                    # that we don't need to extract again
                    # print("Adding {} to list of already extracted files".format(name))
                    self.extracted_file_names[dir_].append(fname_no_ext)
                    e_fnames_file.writelines(dirname + fname_no_ext + '\n')
                    pre_file_count += 1
            print("Found {} previously-existing files in directory {} on startup".format(pre_file_count, dir_))

        # all watched directories
        for dir_ in self.path_data.keys():
            corresponding_extract_dir = self.path_data[dir_]
            for file_ in os.listdir(dir_):
                name, ext = os.path.splitext(file_)
                fname_no_ext = os.path.basename(name)
                full_path = os.path.join(dir_, file_)
                # perform extraction logic if file not already in processed list
                if fname_no_ext not in self.extracted_file_names[corresponding_extract_dir]:
                    print("{} not in list".format(full_path))
                    self.do_extract(full_path)

    def atomic_write(self, out_path, fname_no_ext, extracted_file):
        # create a new temp file for atomic io operation
        txt_path = str(out_path) + '/' + fname_no_ext + '.txt'
        temp_path = txt_path + '.tmp'
        temp_file = open(temp_path, 'w+b')
        temp_file.write(extracted_file.read())
        # flush tempfile contents to disk
        temp_file.flush()
        os.fsync(temp_file.fileno())
        temp_file.close()
        # temp_file now written fully to disk
        os.rename(temp_path, txt_path)
        return txt_path

    def do_extract(self, pathname):
        name, ext = os.path.splitext(pathname)
        dirname = os.path.dirname(pathname)
        fname_no_ext = os.path.basename(name)
        out_path = self.path_data[Path(dirname)]
        list_name = os.path.join(out_path, fname_no_ext)

        if ext in self.file_ext_whitelist \
            and os.path.isfile(pathname):
            # open zip archive
            with zipfile.ZipFile(pathname, 'r') as zref:
                # for each file in the archive
                for archived_file in zref.infolist():
                    # open the archived file for reading
                    with zref.open(archived_file) as extracted_file:
                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(self.atomic_write, out_path, fname_no_ext, extracted_file)
                            concurrent.futures.wait([future])
                            txt_path = future.result()
                            print("+++ adding new file: {}".format(
                                txt_path
                            ))
                            self.extracted_file_names[out_path].append(list_name)
                        extracted_file.seek(0)
                        with open(txt_path, 'rb') as file_on_disk:
                            try:
                                assert (md5(extracted_file.read()).hexdigest()) \
                                ==     (md5(file_on_disk.read()).hexdigest())
                            except AssertionError as e:
                                print("!!! HEX DIGESTS DIFFER FOR ARCHIVED FILE AND FILE ON DISK: {}".format(txt_path))

    # this is how I've observed rsync copying the log files to the directory:
    # copy a prelim dot file and write to that
    # when done writing, move the contents of the dot file to the final file name
    def process_IN_MOVED_TO(self, event):
        name, ext = os.path.splitext(event.pathname)
        dirname = os.path.dirname(event.pathname)
        fname_no_ext = os.path.basename(name)
        # perform extraction logic if file not already in processed list
        if fname_no_ext not in self.extracted_file_names[self.path_data[Path(dirname)]]:
            self.do_extract(event.pathname)

if __name__ == '__main__':
    handler = WatchLog()
    notifier = watch.Notifier(watchman, handler)

    paths = tuple([str(key) for key in handler.path_data.keys()])

    for path in paths:
        wdd = watchman.add_watch(path, watched_events, rec=False)

    watches = [watch.path for watch in watchman.watches.values()]

    print("watching: {}".format(watches))

    notifier.loop()
