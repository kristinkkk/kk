import yaml

#read yaml file:
with open('ac_shopping_crm.yml') as config_file:
    crm_config= yaml.full_load(config_file)
 #   print(crm_config) #返回的是一个dictionary


#get yaml attributes:
print(crm_config.get('dag_name'))
print(crm_config['dag_name'])