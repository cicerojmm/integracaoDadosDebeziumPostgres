# Integracao Dados com do Postgres para o S3 com Debezium 
Integração de dados em tempo real com Debezium, Postgres e S3 com deploy utilizando o Terraform.

### Arquitetura
![alt text](https://github.com/cicerojmm/integracaoDadosDebeziumPostgres/blob/main/images/arquitetura.png?raw=true)


### Comandos Terraform

#### Iniciar e construir a infraestrutura
```sh
terraform init
```
```sh
terraform apply --var-file="dev.tfvars" --var-file="table_conf_vars/produtos.tfvars"
```

#### Destruir toda a infraestrutura
```sh
terraform destroy --var-file="dev.tfvars" --var-file="table_conf_vars/produtos.tfvars"
```
