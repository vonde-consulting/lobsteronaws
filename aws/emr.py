import subprocess


class BaseEMR:
    def __init__(self, instance_group_json_file, steps_json_file, key_name, **kwargs):
        self.applications = kwargs.get("applications", [])
        self.tags = kwargs.get("tags", {})
        self.name = kwargs.get("name", "Unknown")
        self.ec2_attribute = kwargs.get("ec2_attributes", {})
        self.steps = steps_json_file
        self.instance_groups = instance_group_json_file
        self.auto_terminate = kwargs.get("auto_terminate", True)
        self.region = kwargs.get("region", "eu-west-2")
        self.key_name = key_name
        self.credential_profile = kwargs.get("credential_profile", "default")
        self.log_uri = kwargs.get("log_uri", None)

    def create(self):
        _applications = ' '.join([f'Name={app}' for app in self.applications])
        _tags = ' '.join([f"{_k}={_v}" for (_k, _v) in self.tags.items()])
        _command = "aws emr create-cluster " + \
                   f"--release-label emr-5.29.0 " + \
                   f"--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName={self.key_name} " + \
                   f"--applications {_applications} " + \
                   f"--tags '{_tags}' " + \
                   f"--instance-groups file://{self.instance_groups} " + \
                   f"--steps file://{self.steps} " + \
                   f"--auto-scaling-role EMR_AutoScaling_DefaultRole  " + \
                   f"--service-role EMR_DefaultRole " + \
                   f"--name '{self.name}'  " + \
                   f"--profile {self.credential_profile}  " + \
                   f"--region {self.region} "
        if self.auto_terminate:
            _command = _command + "--auto-terminate "
        if not (self.log_uri is None):
            _command = _command + f"--log-uri {self.log_uri} "
        print(_command)
        subprocess.run(_command, shell=True)


class LobsterEMR(BaseEMR):
    def __init__(self, instance_group_json_file, steps_json_file, key_name, name='LOBSTER_Application', **kwargs):
        _tags = {'Name': 'LOBSTER Application'}

        if 'tags' in kwargs:
            _tags = _tags.update(kwargs.get("tags"))
            del kwargs['tags']
        _apps = ['Hadoop', 'Spark', 'Ganglia']

        if 'applications' in kwargs:
            _apps = set(_apps + kwargs.get('application', []))
            del kwargs['applications']

        super(LobsterEMR, self).__init__(instance_group_json_file, steps_json_file, key_name,
                                         name=name, tags=_tags, applications=_apps, **kwargs)
