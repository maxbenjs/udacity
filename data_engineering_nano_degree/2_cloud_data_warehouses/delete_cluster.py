import configparser
import boto3


def delete_redshift_cluster(redshift_client, config):
    try:
        redshift_client.delete_cluster(ClusterIdentifier=config.get('CLUSTER','dwh_cluster'), SkipFinalClusterSnapshot=True)
    
    except Exception as e:
        print(e)
    


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg')) 
    arn_role = config.get('IAM_ROLE','role_name')
    
    redshift_client = boto3.client('redshift'
                        , region_name = 'us-west-2'
                        , aws_access_key_id = config.get('AWS','KEY')
                        , aws_secret_access_key = config.get('AWS','SECRET')
                    )
    

    delete_redshift_cluster(redshift_client, config)
        
    
    
if __name__ == "__main__":
    main()
    