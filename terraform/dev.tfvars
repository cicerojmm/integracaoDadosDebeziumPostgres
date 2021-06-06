service_name = "DEBEZIUM-KAFKA-STACK"
environment  = "dev"


instances_params = {
  kafka_delta = {
    keypair_name  = "debezium"
    subnet_id     = "subnet-ab5ae79a"
    vpc_id        = "vpc-0e09b373"
    instance_type = "t2.large"
  }
}

lambda_environment =  [
  {
    function_name        = "EMR-DELTALAKE"
    keypair_name         = "debezium"
    master_instance_type = "r5.xlarge"
    core_instance_type   = "r5.xlarge"
    instance_count       = 2
    ec2_master_name      = "EMR-DELTALAKE-MASTER"
    ec2_core_name        = "EMR-DELTALAKE-CORE"
    ebs_size_gb          = 50
    ec2_subnet_id        = "subnet-dc9a2bba"
    time_interval        = 144000
  },
]


