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

        # get all text files in the extracted target directories
        for dir_ in self.path_data.values():
            for file_ in os.listdir(dir_):
                name, ext = os.path.splitext(file_)
                # NOTE: we are assuming that when the program extracts a file to the
                # target directory, that extracted file will be correct/fully extracted
                if os.path.isfile(os.path.join(dir_, file_)) and ext == '.txt':
                    # add the filename of this file to the list of files
                    # that we don't need to extract again
                    print("Adding {} to list of already extracted files".format(name))
                    self.extracted_file_names.append(name)
                # this list is necessary because the files are copied over to the
                # local fs via rsync, which will has to touch all pre-existing files
                # to know not to copy over them

                # since we trigger the unzipping process via the closing of unwritable
                # file handles, we still need a way to make sure the handle being closed
                # actually needs to be unzipped
        # print("Target directory contains {} file".format(len(self.extracted_file_names)))
        # print("Checking log directory for zip files...")

        # for file_ in os.listdir(self.watch_path):
        #     name, ext = os.path.splitext(file_)
        #     if name not in self.extracted_file_names and ext in self.file_ext_whitelist:
        #         print(os.path.join(pathlib.Path().absolute()), file_)


    # this is how I've observed rsync copying the log files to the directory:
    # copy a prelim dot file and write to that
    # when done writing, move the contents of the dot file to the final file name
    def process_IN_MOVED_TO(self, event):
        name, ext = os.path.splitext(event.pathname)
        dirname = os.path.dirname(event.pathname)
        fname_no_ext = os.path.basename(name)
        if ext in self.file_ext_whitelist:
            # this file will be processed
            # only extract the file to the target directory
            # if it is not there already
            if name not in self.extracted_file_names:
                with zipfile.ZipFile(event.pathname, 'r') as zref:
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
                            self.extracted_file_names.append(name)

if __name__ == '__main__':
    handler = WatchLog()
    notifier = watch.Notifier(watchman, handler)

    paths = tuple([str(key) for key in handler.path_data.keys()])

    for path in paths:
        wdd = watchman.add_watch(path, watched_events)

    watches = [watch.path for watch in watchman.watches.values()]

    print("watching: {}".format(watches))

    notifier.loop()
