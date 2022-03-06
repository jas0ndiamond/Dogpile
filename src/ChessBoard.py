from ClusterJobResult import ClusterJobResult


class ChessBoard(ClusterJobResult):
        
    def __init__(self, xdim, ydim):
        super().__init__()
        self.xdim = xdim
        self.ydim = ydim
        
        self.boardState = "0" * ( self.xdim * self.ydim)
        
    def getBoardState(self):
        return self.boardState;
        
    def setBoardState(self, newState):
        #TODO: dimension check
        self.boardState = newState

    def getSpace(self, x, y):
        pass

    def toString(self):
        #return "I am a Chess Board %d x %d" % (self.xdim, self.ydim)
        state = ""
        
        for y in range(self.ydim):
            
            state += "\n|"
            
            row = ""
            for x in range(self.xdim):
                row += " " + self.getSpace(x,y) + " |"
                
            state += "\n"
            
            
