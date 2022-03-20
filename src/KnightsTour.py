import logging
import random

from ChessBoard import ChessBoard

logFile = "knightstour.log"

class KnightsTour(object):
    
    DEFAULT_KNIGHT_POS_X = -1
    DEFAULT_KNIGHT_POS_Y = -1
    
    def __init__(self, boardString, xdim, ydim, turnCount=0):

        #TODO: not seeing logging on the node
        logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)

        self.logger.setLevel( logging.DEBUG )

        #convert board string into N x M array for easier moves and bounds checking
        self.xdim = xdim
        self.ydim = ydim
        self.xdimMin = 0
        self.ydimMin = 0
        
        self.knightPosX = KnightsTour.DEFAULT_KNIGHT_POS_X
        self.knightPosY = KnightsTour.DEFAULT_KNIGHT_POS_Y
        
        self.board = ChessBoard(self.xdim, self.ydim)
        self.board.setTurnCount(turnCount)
        self.board.setBoardStateFromString(boardString)

    #dispy cluster task
    #compute the expansion of self.board if any are possible
    def expandBoard(self):

        try:
            expandedBoards = []
            
            #where is the knight?
            #TODO: fail gracefully if not found
            #could do this in the constructor but we want the work for 
            #this done on the cluster node
            self.findKnightPostion()
            
            #major parts of a knight's move, +/- 2 in x,y directions
            #cached for funzies
            moveLeftMajor = self.knightPosX - 2
            moveUpMajor = self.knightPosY - 2
            moveRightMajor = self.knightPosX + 2
            moveDownMajor = self.knightPosY + 2
            
            #minor parts of a knight's move, +/- 1 in x,y directions
            #cached for funzies
            moveLeftMinor = self.knightPosX - 1
            moveUpMinor = self.knightPosY - 1
            moveRightMinor = self.knightPosX + 1
            moveDownMinor = self.knightPosY + 1
            
            #bad case of arrow pattern but it makes testing and future logging easier
            
            #left 2
            if(moveLeftMajor >= self.xdimMin):
                #up 1
                if(moveUpMinor >= self.ydimMin):
                    if(self._canProgress(moveLeftMajor, moveUpMinor)):
                        expandedBoards.append(self._getExpansionBoard(moveLeftMajor, moveUpMinor))
                
                #down 1
                if(moveDownMinor < self.ydim):
                    if(self._canProgress(moveLeftMajor, moveDownMinor)):
                        expandedBoards.append(self._getExpansionBoard(moveLeftMajor, moveDownMinor))
            #########
            
            #up 2
            if(moveUpMajor >= self.ydimMin):
                #left 1
                if(moveLeftMinor >= self.xdimMin):
                    if(self._canProgress(moveLeftMinor, moveUpMajor)):
                        expandedBoards.append(self._getExpansionBoard(moveLeftMinor, moveUpMajor))
                        
                #right 1    
                if(moveRightMinor < self.xdim):
                    if(self._canProgress(moveRightMinor, moveUpMajor)):
                        expandedBoards.append(self._getExpansionBoard(moveRightMinor, moveUpMajor))
            #########
            
            #right 2
            if(moveRightMajor < self.xdim):
                #up 1
                if(moveUpMinor >= self.ydimMin):
                    if(self._canProgress(moveRightMajor, moveUpMinor)):
                        expandedBoards.append(self._getExpansionBoard(moveRightMajor, moveUpMinor))
                        
                #down 1
                if(moveDownMinor < self.ydim):
                    if(self._canProgress(moveRightMajor, moveDownMinor)):
                        expandedBoards.append(self._getExpansionBoard(moveRightMajor, moveDownMinor))
            #########
            
            #down 2
            if(moveDownMajor < self.ydim):
                #right 1
                if(moveRightMinor < self.xdim):
                    if(self._canProgress(moveRightMinor, moveDownMajor)):
                        expandedBoards.append(self._getExpansionBoard(moveRightMinor, moveDownMajor))
            
                #left 1
                if(moveLeftMinor >= self.xdimMin):
                    if(self._canProgress(moveLeftMinor, moveDownMajor)):
                        expandedBoards.append(self._getExpansionBoard(moveLeftMinor, moveDownMajor))
            #########

        finally:
            pass

        self.logger.info("Expansion produced %d new boards" % len(expandedBoards) )

        return expandedBoards

    def findKnightPostion(self):
        
        #TODO: possibly inefficient because it may search each the knight's row twice. better impl
        for i, x in enumerate(self.board.getBoardState()):
            if ChessBoard.KNIGHT in x:
                self.knightPosX = i
                self.knightPosY = x.index(ChessBoard.KNIGHT)
                break
                
        self.logger.info("Found knight at %d,%d" % (self.knightPosX, self.knightPosY) )
    
    def getKnightPosition(self):
        #call findKnightPostion before invoking
        return (self.knightPosX, self.knightPosY)
    
    def _canProgress(self, targetx, targety):
        #given the stored board state, can the knight progress to the target coordinates?
        #assume legal knight move, and on board
        
        #is the target space blank?
        return self.board.getSpace(targetx, targety) == ChessBoard.BLANK
        
    #create a new board with the given x and y as the new move
    def _getExpansionBoard(self, targetx, targety):
        #transaction to create a new board with the moved knight 
        #assume this is a legal tour move
        
        #childboard increments the turncount
        newBoard = self.board.getChildBoard()
        
        #set the new knight position
        newBoard.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        #set the old knight position as the turn count for the progression history
        newBoard.setSpace(self.knightPosX, self.knightPosY, newBoard.getTurnCount())
        
        #debug
        #print("New board:\n%s" % newBoard.dump() )
        
        return newBoard
