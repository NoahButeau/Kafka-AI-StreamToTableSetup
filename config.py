####################################

# This file is meant to be configured with your information

####################################
#Config with your Cluster Information information 
config = {
     'bootstrap.servers': '<Server>',     
     'security.protocol': 'SASL_SSL',
     'sasl.mechanisms': 'PLAIN',
     'sasl.username': '<api>', 
     'sasl.password': '<api_secret>'}

#Config with your Schema Registry information 
sr_config = {
    'url': '<url>',
    'basic.auth.user.info':'<api>:<api_secret>'
}

#Config with your mySQL account information
mySQL_config = {
    'host': '<localhost>',
    'user': '<user>',
    'password': "<password>"
}
