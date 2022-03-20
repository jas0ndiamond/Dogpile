import unittest

from ChessBoard import ChessBoard

class TestChessBoard(unittest.TestCase):
    def test_blank_board(self):
        
        xdim = ydim = 8
        
        board = ChessBoard(xdim, ydim)
        
        self.assertEqual(board.getBoardStateStr().replace(ChessBoard.BOARD_SPACE_DELIM, ""), ChessBoard.BLANK * (xdim * ydim))
        
    def test_knights_tour_start(self):
        #knight at row 0, col 7
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        #check that a knight landed at the start space
        self.assertEqual(board.getSpace(targetx,targety), ChessBoard.KNIGHT)
        
        #check the board state
        boardState = board.getBoardStateStr()
        
        #print("\nBoardState:\n%s" % boardState)
        
        self.assertEqual("0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0", boardState)

        
        #self.assertTrue( boardState.startswith( ChessBoard.BLANK * (targetx) + ChessBoard.KNIGHT) )
        #self.assertTrue( boardState.endswith( ChessBoard.BLANK * ((xdim * ydim) - targetx - 1 ) ))        
        
    def test_board_serialization(self):
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        board = ChessBoard(xdim, ydim)
        
        #non-trivial piece distribution
        board.setSpace(0, 0, ChessBoard.PAWN)
        board.setSpace(1, 1, ChessBoard.ROOK)
        board.setSpace(2, 2, ChessBoard.KNIGHT)
        board.setSpace(3, 3, ChessBoard.BISHOP)
        board.setSpace(4, 4, ChessBoard.QUEEN)
        board.setSpace(5, 5, ChessBoard.KING)
        board.setSpace(6, 6, ChessBoard.PAWN)
        board.setSpace(7, 7, ChessBoard.ROOK)
        
        board.setSpace(0, 7, ChessBoard.PAWN)
        board.setSpace(1, 6, ChessBoard.ROOK)
        board.setSpace(2, 5, ChessBoard.KNIGHT)
        board.setSpace(3, 4, ChessBoard.BISHOP)
        board.setSpace(4, 3, ChessBoard.QUEEN)
        board.setSpace(5, 2, ChessBoard.KING)
        board.setSpace(6, 1, ChessBoard.PAWN)
        board.setSpace(7, 0, ChessBoard.ROOK)
        
        newBoard = ChessBoard(xdim, ydim)
        newBoard.setBoardState(board.getBoardState())
        
        self.assertEqual(board.getBoardStateStr(), newBoard.getBoardStateStr())
        
    def test_board_string_serialization(self):
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        board = ChessBoard(xdim, ydim)
        
        #non-trivial piece distribution
        board.setSpace(0, 0, ChessBoard.PAWN)
        board.setSpace(1, 1, ChessBoard.ROOK)
        board.setSpace(2, 2, ChessBoard.KNIGHT)
        board.setSpace(3, 3, ChessBoard.BISHOP)
        board.setSpace(4, 4, ChessBoard.QUEEN)
        board.setSpace(5, 5, ChessBoard.KING)
        board.setSpace(6, 6, ChessBoard.PAWN)
        board.setSpace(7, 7, ChessBoard.ROOK)
        
        board.setSpace(0, 7, ChessBoard.PAWN)
        board.setSpace(1, 6, ChessBoard.ROOK)
        board.setSpace(2, 5, ChessBoard.KNIGHT)
        board.setSpace(3, 4, ChessBoard.BISHOP)
        board.setSpace(4, 3, ChessBoard.QUEEN)
        board.setSpace(5, 2, ChessBoard.KING)
        board.setSpace(6, 1, ChessBoard.PAWN)
        board.setSpace(7, 0, ChessBoard.ROOK)
        
        newBoard = ChessBoard(xdim, ydim)
        newBoard.setBoardStateFromString(board.getBoardStateStr())
        
        self.assertEqual(board.getBoardStateStr(), newBoard.getBoardStateStr())
        
    def test_child_board(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        self.assertEqual(board.getTurnCount(), 0)
        
        #child board with same state and expected +1 turncount
        childBoard = board.getChildBoard()
        
        self.assertEqual(childBoard.getTurnCount(), 1)
        self.assertEqual(board.getBoardStateStr(), childBoard.getBoardStateStr())
        self.assertEqual(childBoard.getParentState(), board.getBoardState())
        
    def test_child_board2(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim, 6)
        
        board.setBoardStateFromString("1,0,0,0,2,0,0,4,0,0,0,0,5,0,0,0,0,K,0,0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
                
        self.assertEqual(board.getTurnCount(), 6)
        
        #child board with same state and expected +1 turncount
        childBoard = board.getChildBoard()
        
        self.assertEqual(childBoard.getTurnCount(), 7)
        self.assertEqual(board.getBoardStateStr(), childBoard.getBoardStateStr())
        self.assertEqual(childBoard.getParentState(), board.getBoardState())
        
    def test_known_boards(self):
        board1 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0"
        board2 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0"
        board3 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0"
        board4 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0"
        board5 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        board6 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        board7 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        board8 = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        
        boardNew = "0,0,4,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"

        
        knownBoards = {}
        knownBoards[board1] = 1
        knownBoards[board2] = 1
        knownBoards[board3] = 1
        knownBoards[board4] = 1
        knownBoards[board5] = 1        
        knownBoards[board6] = 1
        knownBoards[board7] = 1
        knownBoards[board8] = 1
        
        self.assertTrue( board1 in knownBoards )
        self.assertTrue( board2 in knownBoards )
        self.assertTrue( board3 in knownBoards )
        self.assertTrue( board4 in knownBoards )
        self.assertTrue( board5 in knownBoards )
        self.assertTrue( board6 in knownBoards )
        self.assertTrue( board7 in knownBoards )        
        self.assertTrue( board8 in knownBoards )
        
        self.assertFalse( boardNew in knownBoards )
        
if __name__ == '__main__':
    unittest.main()

