from ClusterJobResult import ClusterJobResult


class ChessBoard(ClusterJobResult):
        
    def __init__(self, xdim, ydim):
        super().__init__()
        self.xdim = xdim
        self.ydim = ydim
        
        pass

    def toString(self):
        return "I am a Chess Board %d x %d" % (self.xdim, self.ydim)
