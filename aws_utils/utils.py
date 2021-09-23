import configparser
import json
import time

import boto3
import boto3.session
import pandas as pd


# Parse config
config = configparser.ConfigParser()
with open('./aws_utils/aws.cfg', 'r') as cfg:
    config.read_file(cfg)

KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

# Create boto3 session
boto3_session = boto3.session.Session(
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET  
)


def create_cluster():
    """Create a redshift cluster with values pulled from aws.cfg"""

    # Create redshift client
    redshift = boto3_session.client('redshift')

    # Check role exists
    iam = boto3_session.client('iam')
    try:
        iam.get_role(RoleName=DWH_IAM_ROLE_NAME)
    except iam.exceptions.NoSuchEntityException:
        print(f'IAM role {DWH_IAM_ROLE_NAME} does not exist, creating now')
        create_iam_role()


    # Create cluster
    try:
        redshift.create_cluster(        
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[_get_iam_role_arn(DWH_IAM_ROLE_NAME)]  
        )
    except Exception as e:
        print(e)

    # Print props as dataframe
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(_pretty_redshift_props(cluster_props))

    # Wait for cluster to become available
    _wait_for_cluster()

    # Open incoming TCP port on security group
    _open_tcp_port(cluster_props)
    

def _open_tcp_port(props):
    """Open incoming TCP port on security group to access cluster"""

    # Create EC2 resource
    ec2 = boto3_session.resource('ec2')

    # Get default security group from cluster's VPC
    vpc = ec2.Vpc(id=props['VpcId'])
    default_security_group = list(vpc.security_groups.all())[0]

    # Open TCP port
    try:
        default_security_group.authorize_ingress(
        GroupName=default_security_group.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
    except Exception as e:
        print(e)


def _wait_for_cluster():
    """Waits for cluster to become available"""

    redshift = boto3_session.client('redshift')
    MAX_TIMEOUT = 600
    t0 = time.time()
    cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    while cluster_props['ClusterStatus'] != 'available':
        print(f"Cluster status: {cluster_props['ClusterStatus']}")
        time.sleep(10)
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if (time.time() - t0) > MAX_TIMEOUT:
            print('Max timeout (10 mins) reached and cluster not available')
            break
    print('Cluster available')


def delete_cluster():
    """Deletes cluster skipping final snapshot"""

    # Create redshift client
    redshift = boto3_session.client('redshift')

    # Delete cluster
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  
        SkipFinalClusterSnapshot=True
    )


def _get_iam_role_arn(iam_role_name):
    """Returns an IAM role's arn"""

    # Create iam client and return arn
    iam = boto3_session.client('iam')
    return iam.get_role(RoleName=iam_role_name)['Role']['Arn']


def create_iam_role():
    """Create an IAM role to allow redshift cluster S3 read access"""

    # Creat IAM client
    iam = boto3_session.client('iam')

    # Create the role
    try:
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

    # Attach S3 ReadOnly policy
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )

    # Get and print IAM role ARN
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(f'IAM role ARN: {role_arn}')


def _pretty_redshift_props(props):
    """Important redshift properties as pandas dataframe"""

    print(props)
    pd.set_option('display.max_colwidth', None)
    keys_to_show = [
        "ClusterIdentifier", 
        "NodeType", 
        "ClusterStatus", 
        "MasterUsername", 
        "DBName", 
        "NumberOfNodes", 
        'VpcId'
    ]
    x = [(k, v) for k,v in props.items() if k in keys_to_show]
    endpoint = props.get('Endpoint', {}).get('Address', '')
    arn = props['IamRoles'][0]['IamRoleArn']
    x.extend([('Endpoint', endpoint), ('IamRoleArn', arn)])

    return(pd.DataFrame(data=x, columns=["Key", "Value"]))
