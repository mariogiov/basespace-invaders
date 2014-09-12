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
################################################################################
from __future__ import print_function


import argparse
import os
import sys

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.model.QueryParameters import QueryParameters
from ConfigParser import ConfigParser
from urllib2 import Request, urlopen, URLError



def download_basespace_files(config_file_path=None, client_key=None, client_secret=None, access_token=None,
                             project_id_list=None, project_name_list=None, sample_id_list=None, sample_name_list=None,
                             dry_run=False, output_directory=None, recreate_basespace_dir_tree=True):

    if not project_id_list: project_id_list = []
    if not project_name_list: project_name_list = []
    if not sample_id_list: sample_id_list = []
    if not sample_name_list: sample_name_list = []
    #if not (sample_id_list or sample_name_list or project_id_list or project_name_list):
    #    print("One of the query options (sample/project) must be specified.", file=sys.stderr)
    #    sys.exit(1)

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
        print('Required parameters not supplied either in config file ({}) '
              ' or via arguments: {}'.format(",".join(missing_params)), file=sys.stderr)
        sys.exit(1)

    app_session_id = config_dict.get("appSessionId") or ""
    api_server = config_dict.get("apiServer") or "https://api.cloud-hoth.illumina.com"
    api_version = config_dict.get("apiVersion") or "v1pre3"
    # Why these limits? Should inform the user
    project_limit = 100
    sample_limit = 1024
    sample_file_limit = 1024


    import ipdb; ipdb.set_trace()
    myAPI = BaseSpaceAPI(client_key, client_secret, api_server, api_version, app_session_id, access_token)
    user = myAPI.getUserById('current')

    ## Is there an API call for this?
    # Convert names -> ids
    if project_name_list:
        remote_basespace_projects = myAPI.getProjectByUser()
        for remote_project in remote_basespace_projects:
            import ipdb; ipdb.set_trace()
            # How does this API work?
            # If the name of the project matches the user-supplied name, append to project_id_list
            pass
    # Same deal here
    for sample_name in sample_name_list:
        pass


    if not project_id_list:
        # Get all projects
        project_id_list = myAPI.getProjectByUser()
        import ipdb; ipdb.set_trace()

    files_to_download = []
    for project_id in project_id_list:
        # Get the list of files to download & append
        pass

    for i, fastq in enumerate(files_to_download):
        print("Downloading file {}/{}: {}".format(i, len(files_to_download), fastq))
        import ipdb; ipdb.set_trace()




def garbage():
    filesToDownload = []
    if None != projectId:
        filesToDownload = Samples.__get_files_to_download(myAPI, projectId, sampleId, sampleName, sampleLimit, sampleFileLimit)
    else:
        myProjects = myAPI.getProjectByUser(qp({'Limit' : projectLimit}))
        for project in myProjects:
            projectId = project.Id
            if None != projectName and project.Name != projectName:
                continue
            filesToDownload = Samples.__get_files_to_download(myAPI, projectId, sampleId, sampleName, sampleLimit, sampleFileLimit)
            if 0 < len(filesToDownload):
                break
    print("Will download {} files.".format(len(filesToDownload)))
    # FIXME
    for i in range(len(filesToDownload)):
        sampleFile = filesToDownload[i]
        print('Downloading ({}{}): {}'.format(((i+1), len(filesToDownload), str(sampleFile))))
        print("File Path: {}".format(sampleFile.Path))
        if not options.dryRun:
            sampleFile.downloadFile(myAPI, outputDirectory, createBsDir=createBsDir)
    print("Download complete.")


def get_list_of_files_to_download(myAPI, projectId, sampleId, sampleName, sampleLimit=1024, sampleFileLmit=1024):
        filesToDownload = []
        samples = myAPI.getSamplesByProject(Id=projectId, queryPars=qp({'Limit' : sampleLimit}))
        for sample in samples:
            if None != sampleId and sampleId != sample.Id:
                continue
            elif None != sampleName and sampleName != sample.Name:
                continue
            sampleFiles = myAPI.getSampleFilesById(Id=sample.Id, queryPars=qp({'Limit' : sampleFileLmit}))
            for sampleFile in sampleFiles:
                filesToDownload.append(sampleFile)
        return filesToDownload


def safe_makedir(dname, mode=0777):
    """Make a directory (tree) if it doesn't exist, handling concurrent race
    conditions.
    """
    if not os.path.exists(dname):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dname, mode=mode)
        except OSError:
            if not os.path.isdir(dname):
                raise
    return dname

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Navigate the byzantine corridors of Basespace and download your files to win")

    cred_group = parser.add_argument_group("Credential options (can also be specified via '-c', configuration file)")
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

    misc_group = parser.add_argument_group("Miscellaneous arguments")
    misc_group.add_argument('-d', '--dry-run', action='store_true', help='dry run; don\'t download any files')
    misc_group.add_argument('-o', '--output-directory',  default=os.getcwd(), help='the directory in which to store the files')
    misc_group.add_argument('-b', '--recreate-basespace-dir-tree', action="store_false",
                     help='recreate the basespace directory structure in the output directory')

    args = vars(parser.parse_args())
    download_basespace_files(**args)




class Samples:
    
    logging.basicConfig()

    @staticmethod

    @staticmethod
    def download(clientKey=None, clientSecret=None, accessToken=None, sampleId=None, projectId=None, sampleName=None, projectName=None, outputDirectory='\.', createBsDir=True):
        '''
        Downloads sample-level files.

        Project Id and project name should
        not be specified together; similarly sample Id and sample name should not be
        specified together.

        1. If only a project Id or only a project name is given, all files for all
        samples will be downloaded within that project.  If additionally a sample Id or
        sample name is given, then only the first matching sample within the project
        will be downloaded.
        2. If only a sample Id is given, then all files for that sample will be downloaded.
        3. If only a sample name is given, then all files within the first project
        containing a sample with matching name will be downloaded.
                
        :param clientKey the Illumina developer app client key
        :param clientSecret the Illumina developer app client secret
        :param accessToken the Illumina developer app access token
        :param sampleId the BaseSpace sample identifier
        :param projectId the BaseSpace project identifier
        :param sampleName the BaseSpace sample name
        :param projectName the BaseSpace project name
        :param outputDirectory the root output directory
        :param createBsDir true to recreate the path structure within BaseSpace, false otherwise
        '''
        appSessionId = ''
        apiServer = 'https://api.basespace.illumina.com/' # or 'https://api.cloud-hoth.illumina.com/'
        apiVersion = 'v1pre3'
        projectLimit = 100         
        sampleLimit = 1024         
        sampleFileLimit = 1024 

        # init the API
        if None != clientKey:
            myAPI = BaseSpaceAPI(clientKey, clientSecret, apiServer, apiVersion, appSessionId, accessToken)
        else:
            myAPI = BaseSpaceAPI(profile='DEFAULT')

        # get the current user
        user = myAPI.getUserById('current')
