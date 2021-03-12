#!/usr/bin/python3.8 -u
import datetime
import os
import tempfile
import pathlib
import sys
import zipfile
import argparse

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
        self.extracted_file_names = []

        # dict of paths to watch with corresponding directories to extract to of the form
        # <watch_path> : <extract_to_path>
        self.path_data = {}

        try:
            assert len(args.watch_paths) == len(args.extract_to)
        except AssertionError as e:
            print('Number of path args must equal number of name args')
            exit(1)

        for x in zip(args.watch_paths, args.extract_to):
            self.path_data[pathlib.Path( x[0] )] = pathlib.Path( x[1] )

        pre_file_count = 0
        # all extract-to directories
        for dir_ in self.path_data.values():
            for file_ in os.listdir(dir_):
                name, ext = os.path.splitext(file_)
                fname_no_ext = os.path.basename(name)
                # NOTE: we are assuming that when the program extracts a file to the
                # target directory, that extracted file will be correct/fully extracted
                if os.path.isfile(os.path.join(dir_, file_)) and ext == '.txt':
                    # add the filename of this file to the list of files
                    # that we don't need to extract again
                    # print("Adding {} to list of already extracted files".format(name))
                    self.extracted_file_names.append(fname_no_ext)
                    pre_file_count += 1
            print("Found {} previously-existing files in directory {} on startup".format(pre_file_count, dir_))

        # all watched directories
        for dir_ in self.path_data.keys():
            for file_ in os.listdir(dir_):
                name, ext = os.path.splitext(file_)
                fname_no_ext = os.path.basename(name)
                # perform extraction logic if file not already in processed list
                if fname_no_ext not in self.extracted_file_names:
                    self.do_extract(os.path.join(dir_, file_))

    def do_extract(self, pathname):
        name, ext = os.path.splitext(pathname)
        dirname = os.path.dirname(pathname)
        fname_no_ext = os.path.basename(name)
        if ext in self.file_ext_whitelist \
            and os.path.isfile(pathname):
            # extraction logic
            with zipfile.ZipFile(pathname, 'r') as zref:
                with tempfile.TemporaryDirectory() as tdir:
                    zref.extractall(tdir)
                    # this should extract exactly ONE text document
                    print("+++ adding new file: {}".format(str(self.path_data[pathlib.Path(dirname)]) + '/' + fname_no_ext + '.txt'))
                    with open( (str(self.path_data[pathlib.Path(dirname)]) + '/' + fname_no_ext + '.txt'), 'w') as outfile: # outfile has same name as zip
                        # ...but just in case there is more than one text doc,
                        # we will loop over them and write their lines out to the outfile
                        for file_ in os.listdir(tdir):
                            with open(os.path.join(tdir, file_)) as tfile:
                                for line in tfile:
                                    outfile.write(line)
                        self.extracted_file_names.append(fname_no_ext)

    # this is how I've observed rsync copying the log files to the directory:
    # copy a prelim dot file and write to that
    # when done writing, move the contents of the dot file to the final file name
    def process_IN_MOVED_TO(self, event):
        fname_no_ext = os.path.basename(event.pathname)
        # perform extraction logic if file not already in processed list
        if fname_no_ext not in self.extracted_file_names:
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
