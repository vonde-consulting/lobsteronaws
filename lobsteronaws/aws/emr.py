"""
aws emr create-cluster
--applications Name=Ganglia Name=Hadoop Name=Spark
--tags 'Name=Ruihong Test'
--ec2-attributes '{"KeyName":"ec2_t2_ruihong","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-7206953f","EmrManagedSlaveSecurityGroup":"sg-37c23b5f","EmrManagedMasterSecurityGroup":"sg-63c33a0b","AdditionalMasterSecurityGroups":["sg-0c5e7dfece90390ff"]}' --release-label emr-5.29.0 --log-uri 's3n://aws-logs-017077188059-eu-west-2/elasticmapreduce/'
--steps '[{"Args":["spark-submit","--deploy-mode","client","--driver-memory","6G","--driver-cores","3","--executor-memory","11G","--executor-cores","3","--class","com.lobsterdata.app.ConstructBook","s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar","s3://demo-ordermessage-lobsterdata-com/NASDAQ100-2019-12-30.txt","s3://demo-ordermessage-lobsterdata-com","s3://ruihong-testing-bucket/demo","parquet","10"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]'
--instance-groups '[{"InstanceCount":1,"BidPrice":"10.000","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":30,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"c4.xlarge","Name":"Master - 1"},{"InstanceCount":2,"BidPrice":"10","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":64,"VolumeType":"gp2"},"VolumesPerInstance":4}]},"InstanceGroupType":"CORE","InstanceType":"m5.4xlarge","Name":"Core - 2"}]'
--auto-terminate
--auto-scaling-role EMR_AutoScaling_DefaultRole
--ebs-root-volume-size 10
--service-role EMR_DefaultRole
--name 'Ruihong Test Auto Scale'
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION
--region eu-west-2
"""
import subprocess

class BaseEMR:
    def __init__(self, **kwargs):
        self.applications = kwargs.get("applications", [])
        self.tags = kwargs.get("tags", {})
        self.name = kwargs.get("name", "Unknown")
        self.ec2_attribute = kwargs.get("ec2_attributes", {})
        self.steps = kwargs.get("steps", None)
        self.instance_groups = kwargs.get("instance_groups")
        self.auto_terminate = kwargs.get("auto_terminate", True)
        self.auto_scaling_role = kwargs.get("auto_scaling_role", "EMR_AutoScaling_DefaultRole")
        self.ebs_root_volume_size = kwargs.get("ebs_root_volume_size", 10)
        self.service_role = kwargs.get("service_role", "EMR_DefaultRole")
        self.scale_down_behavior = kwargs.get("scale_down_behavior", "TERMINATE_AT_TASK_COMPLETION")
        self.region = kwargs.get("region", "eu-west-2")

    def create(self):
        _applications = ' '.join(['Name='+app for app in self.applications])
        _tags = ' '.join([f"{_k}={_v}" for (_k, _v) in self.tags])
        _command = "aws emr create-cluster " + \
            f"--applications {_applications} " + \
            f"--tags '{_tags}' "



class LobsterEMR(BaseEMR):
    def __init__(self, name='LOBSTER Application', **kwargs):
        _tags = {'Name': 'LOBSTER Application'}

        if 'tags' in kwargs:
            _tags = _tags.update(kwargs.get("tags"))
            del kwargs['tags']
        _apps = ['Hadoop' 'Spark']

        if 'applications' in kwargs:
            _apps = set(_apps + kwargs.get('application', []))
            del kwargs['applications']
        super(LobsterEMR, self).__init__(name=name, tags=_tags, applications=_apps, **kwargs)

