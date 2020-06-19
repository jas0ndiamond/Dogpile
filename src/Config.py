import json
import os

class Config:
    def __init__(self, file):


        #check file exists

        if(os.path.exists(file) == False):
            raise Exception("Could not find config file: %s: " % file)


        self.file = file
        self.config = None

        self.nodes_key = "nodes"
        self.secret_key = "secret"
        self.dependencies_key = "dependencies"
        self.loglevel_key = "loglevel"
        self.client_ip_key = "client_ip"
        self.pulse_interval_key = "pulse_interval"


        self._load()
        self._check()

    def _load(self):
        json_data=open(self.file).read()

        self.config = json.loads(json_data)

    def _check(self):
        #assume load has been called already

        #check
        if(self.config.get(self.nodes_key) == None):
            raise Exception("Missing nodes list in config")
        if(self.config.get(self.secret_key) == None):
            raise Exception("Missing secret in config")
        if(self.config.get(self.dependencies_key) == None):
            #key not present => no dependencies
            self.config[self.dependencies_key] = []
        if(self.config.get(self.loglevel_key) == None):
            #key not found => default loglevel
            self.config[self.loglevel_key] = 3
        if(self.config.get(self.client_ip_key) == None):
            raise Exception("Missing client ip in config")
        if(self.config.get(self.pulse_interval_key) == None):
            #key not found => default pulse interval
            self.config[self.pulse_interval_key]  = 300

    def get_nodes(self):
        return self.config[self.nodes_key]

    def get_secret(self):
        return self.config[self.secret_key]

    def get_dependencies(self):
        return self.config[self.dependencies_key]

    def get_loglevel(self):
        return self.config[self.loglevel_key]

    def get_client_ip(self):
        return self.config[self.client_ip_key]

    def get_pulse_interval(self):
        return self.config[self.pulse_interval_key]

    def dump(self):
        print("Config:\n%s" % json.dumps(self.config, indent=4) )
