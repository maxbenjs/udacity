import configparser
import boto3
import json


def create_redshift_cluster(redshift_client, config):
    """Function to create the Redshift cluster, using the redshift client
    Arguments as inputs include:
    - Redshift client
    - Config file to fetch the arguments required to create the redshift cluster via the client
    """
    try:    
        response = redshift_client.create_cluster(        
              ClusterType=config.get('CLUSTER', 'dwh_cluster_type')
            , NodeType=config.get('CLUSTER', 'dwh_node_type')
            , NumberOfNodes=int(config.get('CLUSTER', 'dwh_num_nodes'))
            , DBName=config.get('CLUSTER','db_name')
            , ClusterIdentifier=config.get('CLUSTER','dwh_cluster')
            , MasterUsername=config.get('CLUSTER','db_user')
            , MasterUserPassword=config.get('CLUSTER','db_password')
            , IamRoles=[config.get('IAM_ROLE','arn')] 
        )
    
    except Exception as e:
        print(e)
    


def main():
    """
    - Main function which fetches arguments from the config file
    - Then create the redshift client before calling the 'create_redshift_cluster' function to create the cluster
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg')) 
    arn_role = config.get('IAM_ROLE','role_name')
    
    redshift_client = boto3.client('redshift'
                        , region_name = 'us-west-2'
                        , aws_access_key_id = config.get('AWS','KEY')
                        , aws_secret_access_key = config.get('AWS','SECRET'))
    
    create_redshift_cluster(redshift_client, config)
        
    
    
if __name__ == "__main__":
    main()
    
    
    