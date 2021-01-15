import dispy

import dispy.config
from dispy.config import MsgTimeout

from Config import Config
from Grayscaler import Grayscaler

class ClusterFactory:
    def __init__(self, config):
        self.setConfig(config)

        #TODO set this correctly
        dispy.config.MsgTimeout = 1200

    def setConfig(self, config):
        self.config = config

    def buildCluster(self, runFunction, status_callback=None):

        cluster_nodes = self.config.get_nodes()
        client_ip = self.config.get_client_ip()
        pulse_interval = self.config.get_pulse_interval()
        node_secret = self.config.get_secret()
        cluster_dependencies = self.config.get_dependencies()
        loglevel = self.config.get_loglevel()
        loglevel_dispy = self.config.get_disy_loglevel()

    # def __init__(self, computation, nodes=None, depends=[], callback=None, cluster_status=None,
    #              ip_addr=None, dispy_port=None, ext_ip_addr=None,
    #              ipv4_udp_multicast=False, dest_path=None, loglevel=logger.INFO,
    #              setup=None, cleanup=True, ping_interval=None, pulse_interval=None,
    #              poll_interval=None, reentrant=False, secret='', keyfile=None, certfile=None,
    #              recover_file=None):


        return dispy.JobCluster(runFunction,
            cluster_status=status_callback,
            nodes=cluster_nodes,
            depends=cluster_dependencies,
            loglevel=loglevel_dispy,
            ip_addr=client_ip,
            pulse_interval=pulse_interval,
            secret=node_secret)
