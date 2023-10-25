import logging
import random

from ChessBoard import ChessBoard

logFile = "knightstour.log"

class KnightsTour(object):
    
    DEFAULT_KNIGHT_POS_X = -1
    DEFAULT_KNIGHT_POS_Y = -1
    
    def __init__(self, boards):

        #TODO check boards is an array of chessboards

        #TODO: not seeing logging on the node
        logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)

        self.logger.setLevel( logging.DEBUG )

        self.boards = boards

    def expandBoards(self):           
        return self._expandBoards(self.boards)

    def _expandBoards(self, boards):
        expandedBoards = []
        for board in boards:
            for expandedBoard in self._expandBoard(board):
                expandedBoards.append(expandedBoard)
            
        return expandedBoards

    #dispy cluster task
    #compute the expansion of self.board if any are possible
    def _expandBoard(self, board):

        try:
            boardXDimMin = 0
            boardYDimMin = 0
            boardXDim = board.getXDim()
            boardYDim = board.getYDim()
            
            expandedBoards = []
            
            #where is the knight?
            #TODO: fail gracefully if not found ==> no expansion possible for kt
            #could do this in the constructor but we want the work for 
            #this done on the cluster node
            #stores result in self.knightPosX and self.knightPosY
            (kXPos, kYPos) = self.findKnightPosition(board)
            
            if(kXPos == ChessBoard.INVALID_POS_X or kYPos == ChessBoard.INVALID_POS_Y):
                self.logger.error("Could not find knight on board. Bailing.")
                #TODO: return None instead? throw exception? since this runs on a node it will have to signal the server
                return []
            
            #major parts of a knight's move, +/- 2 in x,y directions
            #cached for funzies
            moveLeftMajor = kXPos - 2
            moveUpMajor = kYPos - 2
            moveRightMajor = kXPos + 2
            moveDownMajor = kYPos + 2
            
            #minor parts of a knight's move, +/- 1 in x,y directions
            #cached for funzies
            moveLeftMinor = kXPos - 1
            moveUpMinor = kYPos - 1
            moveRightMinor = kXPos + 1
            moveDownMinor = kYPos + 1
            
            #bad-ish case of arrow pattern but it makes testing and future logging easier
            
            #left 2
            if(moveLeftMajor >= boardXDimMin):
                #up 1
                if(moveUpMinor >= boardYDimMin):
                    if(self._canProgress(board, moveLeftMajor, moveUpMinor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveLeftMajor, moveUpMinor))
                
                #down 1
                if(moveDownMinor < boardYDim):
                    if(self._canProgress(board, moveLeftMajor, moveDownMinor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveLeftMajor, moveDownMinor))
            #########
            
            #up 2
            if(moveUpMajor >= boardYDimMin):
                #left 1
                if(moveLeftMinor >= boardXDimMin):
                    if(self._canProgress(board, moveLeftMinor, moveUpMajor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveLeftMinor, moveUpMajor))
                        
                #right 1    
                if(moveRightMinor < boardXDim):
                    if(self._canProgress(board, moveRightMinor, moveUpMajor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveRightMinor, moveUpMajor))
            #########
            
            #right 2
            if(moveRightMajor < boardXDim):
                #up 1
                if(moveUpMinor >= boardYDimMin):
                    if(self._canProgress(board, moveRightMajor, moveUpMinor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveRightMajor, moveUpMinor))
                        
                #down 1
                if(moveDownMinor < boardYDim):
                    if(self._canProgress(board, moveRightMajor, moveDownMinor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveRightMajor, moveDownMinor))
            #########
            
            #down 2
            if(moveDownMajor < boardYDim):
                #right 1
                if(moveRightMinor < boardXDim):
                    if(self._canProgress(board, moveRightMinor, moveDownMajor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveRightMinor, moveDownMajor))
            
                #left 1
                if(moveLeftMinor >= boardXDimMin):
                    if(self._canProgress(board, moveLeftMinor, moveDownMajor)):
                        expandedBoards.append(self._getExpansionBoard(board, kXPos, kYPos, moveLeftMinor, moveDownMajor))
            #########

        finally:
            pass

        self.logger.debug("Expansion produced %d new boards" % len(expandedBoards) )

        return expandedBoards

    def findKnightPosition(self, board):
        
        xPos = ChessBoard.INVALID_POS_X
        yPos = ChessBoard.INVALID_POS_Y
        
        #TODO: possibly inefficient because it may search each the knight's row twice. better impl
        for i, x in enumerate(board.getBoardState()):
            if ChessBoard.KNIGHT in x:
                xPos = i
                yPos = x.index(ChessBoard.KNIGHT)
                break
                
        self.logger.info("Found knight at %d,%d" % (xPos, yPos) )
        
        return (xPos, yPos)
    
    def _canProgress(self, board, targetx, targety):
        #given the stored board state, can the knight progress to the target coordinates?
        #assume legal knight move, and on board
        
        #is the target space blank?
        return board.getSpace(targetx, targety) == ChessBoard.BLANK
        
    #create a new board with the given x and y as the new move
    def _getExpansionBoard(self, board, currentX, currentY, targetX, targetY):
        #transaction to create a new board with the moved knight 
        #assume this is a legal tour move
        
        #childboard increments the turncount
        newBoard = board.getChildBoard()
        
        #set the new knight position
        newBoard.setSpace(targetX, targetY, ChessBoard.KNIGHT)
        
        #set the old knight position as the turn count for the progression history
        newBoard.setSpace(currentX, currentY, newBoard.getTurnCount())
        
        return newBoard
