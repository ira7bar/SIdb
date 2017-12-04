import ruamel.yaml as yaml

dict = {'input_port' :'3030', 'output_port':'3031', 'ttl_time':20 }
dict_ymal_encoded = yaml.dump(dict)

with open('config_yaml.yml','w') as open_file:
    open_file.write(dict_ymal_encoded)