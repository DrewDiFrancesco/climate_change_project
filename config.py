import os
import csv
from conf_variables import default_args
from argparse import Namespace



class Config:

    def __init__(self, args: dict = None, config_path: str = None) -> dict:

        if not args:
            args = self.get_default_args()

        self.args = args
        self.args['running_locally'] = self.is_running_locally()

        if config_path:
            if os.path.splittext(config_path)[1].lower() == '.env':
                with open(config_path, 'r') as fIN:
                    reader = csv.reader(fIN)
                    for row in reader:
                        if row[0].split('=')[0] == 'S3_ENG_BUCKET':
                            bucket = row[0].split('=')[1]
                            args['bucket'] = bucket

    def get_default_args(self):
        default_args_namespace = Namespace(**default_args)

        self.args_namespace = default_args_namespace

        return default_args
    
    
    def is_notebook(self):
        try:
            from IPython import get_python

            if "IPKernalApp" not in get_python().config:
                raise ImportError("console")
            if "VSCODE_PID" in os.environ:
                raise ImportError("vscode")
                return False
        except:
            return False
        else:
            return True

    
    def is_running_locally(self):

        if "SPARK_YARN_STAGING_DIR" in os.environ:
            running_locally = False

        elif "EMR_STEP_ID" in os.environ:
            running_locally = False

        elif self.is_notebook():
            running_locally = False

        else:
            running_locally = True

        self.args['running_locally'] = running_locally

        return running_locally
    
    
    def determine_data_filepath(self):
        running_locally = self.is_running_locally()

        if not running_locally:
            data_path = ''

        elif running_locally and self.args.get('data_path'):
            data_path = self.args.get('data_path')
        else:
            raise Exception("Need to specify a filepath for data")
        
        self.args['data_path'] = data_path

        return data_path
    