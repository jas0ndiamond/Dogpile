import csv
import json
import hashlib

from ClusterJobResult import ClusterJobResult


class ChessBoard(ClusterJobResult):

    STANDARD_BOARD_DIM = 8
        
    INVALID_POS_X = -1
    INVALID_POS_Y = -1
        
    DEAD = "X"
    BLANK = "O"  # letter 'O' is for Open
    
    BOARD_SPACE_DELIM = ","
    
    PAWN = "P"
    ROOK = "R"
    KNIGHT = "K"
    BISHOP = "B"
    QUEEN = "Q"
    KING = "M" # M is for Monarch
        
    def __init__(self, xdim, ydim, turnCount=0):
        super().__init__()
        
        self.setXDim(xdim)
        self.setYDim(ydim)
        
        self.boardState = [[ChessBoard.BLANK for x in range(xdim)] for y in range(ydim)]
        
        # defaults
        self.parentState = None
        self.turnCount = turnCount
        
    def getXDim(self):
        return self.xdim
        
    def getYDim(self):
        return self.ydim
        
    ##############
    # must be set coherently with board state
    def setXDim(self, dim):
        self.xdim = dim
        
    def setYDim(self, dim):
        self.ydim = dim
    ###############
        
    def setTurnCount(self, turnCount):
        self.turnCount = turnCount
        
    def getTurnCount(self):
        return self.turnCount
        
    def setParentState(self, state):
        self.parentState = state
        
    def getParentState(self):
        return self.parentState
        
    def getBoardState(self):
        return self.boardState;
        
    def setBoardState(self, newState):
        
        #matrix copy
        
        #TODO: dimension check
        self.boardState = newState

    #create a board from a csv string
    #TODO: consistent naming
    def setBoardStateFromString(self, newStateString):

        #empty board
        self.boardState = [[ChessBoard.BLANK for x in range(self.xdim)] for y in range(self.ydim)]
        
        #set spaces
        
        #csv module seems to be built primarily for files and not strings and causes all sorts of problems
        xiter = yiter = 0
        
        for space in next(csv.reader(newStateString.splitlines(), delimiter=ChessBoard.BOARD_SPACE_DELIM, strict=True)):
            self.boardState[xiter][yiter] = space
            
            xiter += 1
            if(xiter == self.xdim):
                xiter = 0
                yiter += 1


    def getSpace(self, x, y):
        #TODO: bounds check
        
        return self.boardState[x][y]
        
    def setSpace(self, x, y, val):
        self.boardState[x][y] = val

    def getBoardStateStr(self):
        
        board = self.getBoardState()
        
        #add to single dim array, then parse into csv string
        retval = []
        for y in range(self.ydim):
            for x in range(self.xdim):
                retval.append(str(board[x][y]))
                
        return ChessBoard.BOARD_SPACE_DELIM.join(retval)
        
    def getChildBoard(self):
        currentBoardState = self.getBoardState()
        
        newBoard = ChessBoard(self.xdim, self.ydim)
        
        newBoard.setParentState(currentBoardState)
        
        #expect the new board state to change, but the parent state not to change
        newBoard.setBoardStateFromString(self.getBoardStateStr())
        
        newBoard.setTurnCount(self.getTurnCount() + 1)
        
        return newBoard
        
    # possibly move these to ClusterJobResult and require override
    # easy way to determine board uniqueness
    def getHashCode(self):
        #call hexdigest otherwise the result is a undefined memory address for string comparisons
        return hashlib.md5(self.toJson().encode()).hexdigest()
        
    def fromJson(self, jsonStr):
        pass
        
    def toJson(self):
        
        #TODO find a way to incorporate parent state
        
        # skip hashcode as toJson calls this function
        return """
        {
            "boardState": "%s",
            "turnCount:" %d,
            "xDim": %d,
            "yDim": %d
        }
        """ % (
            self.getBoardStateStr(),
            self.getTurnCount(),
            self.getXDim(),
            self.getYDim()
        )
        
    def toDict(self):
        return {
            "hashCode": self.getHashCode(),
            "boardState": self.getBoardStateStr(),
            "turnCount": self.getTurnCount(),
            "xDim": self.getXDim(),
            "yDim": self.getYDim(),
        }
        
    def dump(self):
        #print( "\nI am a Chess Board %d x %d\n%s" % (self.xdim, self.ydim, self.getBoardStateStr()))
        
        state = ""
        
        for y in range(self.ydim):
            
            state += "\n%s|" % y
            
            row = ""
            for x in range(self.xdim):
                row += " " + str(self.getSpace(x,y)) + " |"
                
            state += row
            
        return ("----------------------------------%s\n----------------------------------\n" % state )
        
    ##################################################
    @classmethod
    def serializeFromDict(cls, other):
    
        #TODO: require override?
        
        # create a new board copied from 'other'
        
        # used in serialization of job objects by job managers that store job object data in various data structures
        
        newBoard = ChessBoard( other["xDim"], other["yDim"], other[ 'turnCount' ])
        
        newBoard.setBoardStateFromString( other['boardState'] )
        
        return newBoard
