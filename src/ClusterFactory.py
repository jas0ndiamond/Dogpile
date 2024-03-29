import dispy
import dispy.config
import logging
from dispy.config import MsgTimeout

from Config import Config
from DefaultClusterStatusCallback import DefaultClusterStatusCallback

class ClusterFactory:
    def __init__(self, config):
        
        self.setConfig(config)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.DEBUG )

        #TODO set this correctly
        dispy.config.MsgTimeout = 1200

    def setConfig(self, config):
        self.config = config

    def getConfig(self):
        return self.config

    # build a dispy cluster with the default cluster status callback and a specified job status callback
    def buildCluster(self, runFunction, cluster_status_callback=DefaultClusterStatusCallback.callback, job_status_callback=None):

        #if the local machine is a node, then it has to be specified by ip address. it doesn't seem to work with hostname or fqdn
        cluster_nodes = self.config.get_nodes()

        self.logger.debug("Using cluster nodes: %s" % cluster_nodes)

        client_ip = self.config.get_client_ip()
        pulse_interval = self.config.get_pulse_interval()
        node_secret = self.config.get_secret()
        cluster_dependencies = self.config.get_dependencies()
        loglevel = self.config.get_loglevel()
        loglevel_dispy = self.config.get_disy_loglevel()

        return dispy.JobCluster(runFunction,
            cluster_status=cluster_status_callback,
            job_status=job_status_callback,
            nodes=cluster_nodes,
            depends=cluster_dependencies,
            loglevel=loglevel_dispy,
            ip_addr=client_ip,
            pulse_interval=pulse_interval,
            ping_interval=300,
            reentrant=True,
            secret=node_secret)
