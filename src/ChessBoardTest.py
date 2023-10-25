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
        
        self.assertEqual("O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O", boardState)

        
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
        
        board.setBoardStateFromString("1,O,O,O,2,O,O,4,O,O,O,O,5,O,O,O,O,K,O,O,O,O,O,3,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
                
        self.assertEqual(board.getTurnCount(), 6)
        
        #child board with same state and expected +1 turncount
        childBoard = board.getChildBoard()
        
        self.assertEqual(childBoard.getTurnCount(), 7)
        self.assertEqual(board.getBoardStateStr(), childBoard.getBoardStateStr())
        self.assertEqual(childBoard.getParentState(), board.getBoardState())
        
    def test_known_boards(self):
        
        # not really a test, more like a sanity check
        
        board1 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O"
        board2 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O"
        board3 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O"
        board4 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O"
        board5 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        board6 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        board7 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        board8 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        
        boardNew = "O,O,4,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"

        
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
        
    def test_hash_code(self):
        boardState1 = "O,1,3,5,2,4,K,J,Q,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O"
        boardState2 = "O,1,3,5,2,4,K,J,Q,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O"
        boardState3 = "O,1,3,5,2,4,K,J,Q,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O"
        boardState4 = "O,1,3,5,2,4,K,J,Q,O,O,O,O,O,O,O"
        boardState5 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        boardState6 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        boardState7 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        boardState8 = "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,1O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O"
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        xdim2 = ydim2 = 4
        
        board1 = ChessBoard(xdim, ydim)
        board1.setBoardStateFromString(boardState1)
        board1.setTurnCount(4)
        
        #same state as board1, with a different turn count
        board2 = ChessBoard(xdim, ydim)
        board2.setBoardStateFromString(boardState1)
        board2.setTurnCount(5)
        
        #same turn count as board 1, different board state
        board3 = ChessBoard(xdim, ydim)
        board3.setBoardStateFromString(boardState3)
        board3.setTurnCount(4)
        
        #same turn count as board1, similar board state but different dimensions
        #not a valid board but it shouldn't matter here
        board4 = ChessBoard(xdim2, ydim2)
        board4.setBoardStateFromString(boardState4)
        board4.setTurnCount(4)        
        
        # 2 calls should return the same result
        self.assertEqual( board1.getHashCode(), board1.getHashCode() )
        
        self.assertNotEqual( board1.getHashCode(), board2.getHashCode() )
        self.assertNotEqual( board1.getHashCode(), board3.getHashCode() )
        self.assertNotEqual( board1.getHashCode(), board4.getHashCode() )
                
if __name__ == '__main__':
    unittest.main()

