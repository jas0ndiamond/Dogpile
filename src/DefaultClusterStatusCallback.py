import dispy
import logging

class DefaultClusterStatusCallback(object):
    
    _logger = logging.getLogger(__name__)
    
    def __init__(self):
        pass

    #debug
    def _logtest():
        
        print("in log test")

        DefaultClusterStatusCallback._logger.debug("static method debug")
        DefaultClusterStatusCallback._logger.info("static method info")
        
    def callback(status, node, job):

        DefaultClusterStatusCallback._logger.debug("============= DefaultClusterStatusCallback invoked ===========")
        
        if status == dispy.DispyNode.Initialized:
            DefaultClusterStatusCallback._logger.info ("node %s with %s CPUs available" % (node.ip_addr, node.avail_cpus))
        elif status == dispy.DispyNode.Closed:
            DefaultClusterStatusCallback._logger.info ("node %s closing" % node.ip_addr)
        elif status == dispy.DispyJob.Created or status == dispy.DispyJob.Running or status == dispy.DispyJob.Finished:
            #inherited from job statuses. normal operation. ignore
            pass
        else: 
            DefaultClusterStatusCallback._logger.warn("Unexpected node status: %d" % status )
    
        DefaultClusterStatusCallback._logger.debug("============= DefaultClusterStatusCallback returning===========")
