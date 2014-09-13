################################################################################
# Copyright 2014 Nils Homer
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

################################################################################
# This tool was adapted with permission from Mayank Tyagi <mtyagi@illumina.com>
# and subsequently updated by Mario Giovacchini https://github.com/mariogiov
################################################################################
from __future__ import print_function


import argparse
import os
import re
import sys

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.model.QueryParameters import QueryParameters
from ConfigParser import ConfigParser
from functools import partial


## TODO use QueryParameters to filter?
## TODO let user pick Run, File by name/id
## TODO fix logic: if user specifies both project and sample, we should search each independently
## TODO Implement separate script for listing project trees
## TODO Consider implementing separate download_basespace_<type> fns for projects, samples, etc.

print_stderr = partial(print, file=sys.stderr)

def download_basespace_files(config_file_path=None, client_key=None, client_secret=None, access_token=None,
                             project_id_list=None, project_name_list=None, sample_id_list=None, sample_name_list=None,
                             dry_run=False, output_directory=None, recreate_basespace_dir_tree=True):
    # Check input parameters / load from config file / defaults
    if not project_id_list: project_id_list = []
    if not project_name_list: project_name_list = []
    if not sample_id_list: sample_id_list = []
    if not sample_name_list: sample_name_list = []
    if not output_directory:
        output_directory = os.getcwd()
        print_stderr("Output directory not specified; using current directory ({})".format(output_directory))
    else:
        output_directory = os.path.abspath(output_directory)
    if not dry_run:
        safe_makedir(output_directory)
    config_dict = {}
    if config_file_path:
        config_parser = ConfigParser()
        config_parser.read(config_file_path)
        config_dict = config_parser._defaults
        if not client_key: client_key = config_dict.get('clientkey')
        if not client_secret: client_secret = config_dict.get('clientsecret')
        if not access_token: access_token = config_dict.get('accesstoken')
    if not (client_key and client_secret and access_token):
        missing_params = []
        if not client_key: missing_params.append("client_key")
        if not client_secret: missing_params.append("client_secret")
        if not access_token: missing_params.append("access_token")
        print_stderr('Error: Required parameters not supplied either in config '
                     'file ({}) or via arguments.'.format(config_file_path,
                                                          ', '.join(missing_params)))
        sys.exit(1)
    app_session_id = config_dict.get("appsessionid") or ""
    api_server = config_dict.get("apiserver") or "https://api.basespace.illumina.com"
    api_version = config_dict.get("apiversion") or "v1pre3"
    # Get the API connection object
    myAPI = BaseSpaceAPI(clientKey=client_key, clientSecret=client_secret,
                         apiServer=api_server, version=api_version,
                         appSessionId=app_session_id, AccessToken=access_token)

    basespace_projects = myAPI.getProjectByUser()
    user = myAPI.getUserById('current')
    # If user specified projects, get them by name or id
    project_objects = []
    if project_name_list:
        project_objects.extend(_select_from_object(filter_list=project_name_list,
                                                   search_list=basespace_projects,
                                                   key_attr="Name",
                                                   obj_type="project",
                                                   user=user))
    if project_id_list:
        digit_pattern = re.compile(r'^\d+$')
        project_filtered_id_list = []
        for project_id in project_id_list:
            if not digit_pattern.match(project_id):
                print_stderr('Error: Invalid format for user-specified project id '
                             '"{}": project ids are strictly numeric. Did you mean '
                             'to pass this as a project name?'.format(project_id))
            else:
                project_filtered_id_list.append(project_id)
        project_objects.extend(_select_from_object(filter_list=project_filtered_id_list,
                                                   search_list=basespace_projects,
                                                   key_attr="Id",
                                                   obj_type="project",
                                                   user=user))
    if not (project_name_list or project_id_list):
        # Get all projects if none are specified by user
        project_objects = basespace_projects

    basespace_samples = []
    for project_obj in project_objects:
        basespace_samples.extend(project_obj.getSamples(myAPI))
    sample_objects = []
    if sample_name_list:
        sample_objects.extend(_select_from_object(filter_list=sample_name_list,
                                                  search_list=basespace_samples,
                                                  key_attr="Name",
                                                  obj_type="sample",
                                                  user=user))
    if sample_id_list:
        digit_pattern = re.compile(r'^\d+$')
        sample_filtered_id_list = []
        for sample_id in sample_id_list:
            if not digit_pattern.match(sample_id):
                print_stderr('Error: Invalid format for user-specified sample id '
                             '"{}": sample ids are strictly numeric. Did you mean '
                             'to pass this as a sample name?'.format(sample_id))
            else:
                sample_filtered_id_list.append(sample_id)
        sample_objects.extend(_select_from_object(filter_list=sample_filtered_id_list,
                                                  search_list=basespace_samples,
                                                  key_attr="Id",
                                                  obj_type="sample",
                                                  user=user))
    if not (sample_name_list or sample_id_list):
        # Get all samples if none are specified by user
        sample_objects = basespace_samples

    files_to_download = []
    for sample_obj in sample_objects:
        files_to_download.extend(sample_obj.getFiles(myAPI))

    if files_to_download:
        print_stderr("Found {} files to download: ".format(len(files_to_download)))
        for file_obj in files_to_download:
            print_stderr("\t- {}".format(file_obj))
        print_stderr('Downloading files to output directory {}'.format(output_directory))
        if recreate_basespace_dir_tree:
            print_stderr("Recreating BaseSpace project directory tree for file.")
        if dry_run:
            print_stderr("-> Dry run: not downloading any data.")
        for i, file_obj in enumerate(files_to_download):
            print_stderr('[{}/{}] Downloading file "{}"'.format(i+1, len(files_to_download),
                                                                file_obj))
            if not dry_run:
                file_obj.downloadFile(api=myAPI, localDir=output_directory,
                                      createBsDir=recreate_basespace_dir_tree)
        print_stderr('Download completed; files are located in "{}"'.format(output_directory))
    else:
        print_stderr("Error: no files found to download.")


def _select_from_object(filter_list, search_list, key_attr, obj_type=None, user=None):
    object_attr_list = []
    object_attr_dict = { getattr(obj,key_attr): obj for obj in search_list }
    if not obj_type: obj_type = type(search_list[0])
    user_string = 'for user "{}"'.format(user) if user else ""
    for search_value in filter_list:
        try:
            object_attr_list.append(object_attr_dict[search_value])
        except KeyError:
            print_stderr('Warning: user-specified {obj_type} {key_attr} "{user_value}" '
                         'not found in {obj_type}s {user_string}'.format(obj_type=obj_type,
                                                                         key_attr=key_attr.lower(),
                                                                         user_value=search_value,
                                                                         user_string=user_string))
    return object_attr_list


def safe_makedir(dname, mode=0777):
    """Make a directory (tree) if it doesn't exist, handling concurrent race
    conditions.
    """
    if not os.path.exists(dname):
        try:
            os.makedirs(dname, mode=mode)
        except OSError:
            if not os.path.isdir(dname):
                raise
    return dname


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Navigate the byzantine corridors of Basespace and download your files to win")

    cred_group = parser.add_argument_group("Credential options (note that specifying these via '-K', '-S', and '-A' is not secure;\n\t\t    you are recommended to pass a configuration file with '-c')")
    cred_group.add_argument('-c', '--config', dest="config_file_path", default=os.path.expandvars('$HOME/.basespace.cfg'),
                        help='the path to the configuration file (default $HOME/.basespace.cfg)')
    cred_group.add_argument('-K', '--client-key', help='the developer.basespace.illumina.com client key')
    cred_group.add_argument('-S', '--client-secret', help='the developer.basespace.illumina.com client token')
    cred_group.add_argument('-A', '--access-token', help='the developer.basespace.illumina.com access token')

    query_group = parser.add_argument_group("Query arguments")
    query_group.add_argument('-s', '--sample-id', action="append", dest="sample_id_list",
            help='the sample identifier (optional); specify multiple times for multiple samples')
    query_group.add_argument('-x', '--sample-name', action="append", dest="sample_name_list",
            help='the sample name (optional); specify multiple times for multiple samples')
    query_group.add_argument('-p', '--project-id', action="append", dest="project_id_list",
            help='the project identifier (optional); specify multiple times for multiple projects')
    query_group.add_argument('-y', '--project-name', action="append", dest="project_name_list",
            help='the project name (optional); specify multiple times for multiple projects')
    ## Add RunId
    ## Add FileId
    ## Add User

    misc_group = parser.add_argument_group("Miscellaneous arguments")
    misc_group.add_argument('-d', '--dry-run', action='store_true', help='dry run; don\'t download any files')
    misc_group.add_argument('-o', '--output-directory', default=os.getcwd(), help='the directory in which to store the files')
    misc_group.add_argument('-b', '--recreate-basespace-dir-tree', action="store_false",
                     help='recreate the basespace directory structure in the output directory')

    args = vars(parser.parse_args())
    download_basespace_files(**args)
