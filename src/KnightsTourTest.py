import unittest

from KnightsTour import KnightsTour
from ChessBoard import ChessBoard


class TestKnightsTour(unittest.TestCase):
        
    def setUp(self):
        pass
                        
    def test_find_knight1(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        (xknight, yknight) = kt.getKnightPosition()
        
        self.assertEqual( xknight, KnightsTour.DEFAULT_KNIGHT_POS_X)
        self.assertEqual( yknight, KnightsTour.DEFAULT_KNIGHT_POS_Y)
        
        kt.findKnightPostion()
        
        (xknight, yknight) = kt.getKnightPosition()
        
        self.assertEqual( xknight, targetx)
        self.assertEqual( yknight, targety)
        
    def test_find_knight2(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        (xknight, yknight) = kt.getKnightPosition()
        
        self.assertEqual( xknight, KnightsTour.DEFAULT_KNIGHT_POS_X)
        self.assertEqual( yknight, KnightsTour.DEFAULT_KNIGHT_POS_Y)
        
        kt.findKnightPostion()
        
        (xknight, yknight) = kt.getKnightPosition()
        
        self.assertEqual( xknight, targetx)
        self.assertEqual( yknight, targety)
        
    def test_find_knight3(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        (xknight, yknight) = kt.getKnightPosition()
        
        self.assertEqual( xknight, KnightsTour.DEFAULT_KNIGHT_POS_X)
        self.assertEqual( yknight, KnightsTour.DEFAULT_KNIGHT_POS_Y)
        
        kt.findKnightPostion()
        
        (xknight, yknight) = kt.getKnightPosition()
        
        self.assertEqual( xknight, targetx)
        self.assertEqual( yknight, targety)
        
    def test_expanded_boards_count1(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 2) 
        
    def test_expanded_boards_count2(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 2) 
        
    def test_expanded_boards_count3(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 2) 
        
    def test_expanded_boards_count4(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 2) 
        
    def test_expanded_boards_count5(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 1
        targety = 1
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 4) 
        
    def test_expanded_boards_count6(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 6
        targety = 1
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 4) 
        
    def test_expanded_boards_count7(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 1
        targety = 6
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 4) 
        
    def test_expanded_boards_count8(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 6
        targety = 6
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 4) 
        
    def test_expanded_boards_count9(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 2
        targety = 2
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 8) 
        
    def test_expanded_boards_count10(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 2
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 8) 
        
    def test_expanded_boards_count11(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 2
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 8) 
        
    def test_expanded_boards_count12(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        self.assertEqual( len(kt.expandBoard()), 8) 
    
    def test_expand_from_00(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #2 expansions from 0,0
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("10000000000000000K0000000000000000000000000000000000000000000000"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("1000000000K00000000000000000000000000000000000000000000000000000"))
        
    def test_expand_from_07(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #2 expansions from 0,7
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000000000000000000000000000K0000010000000"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000000000000000000K0000000000000010000000"))
        
    def test_expand_from_70(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #2 expansions from 7,0
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000100000000000000K00000000000000000000000000000000000000000"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000100000K00000000000000000000000000000000000000000000000000"))
        
    def test_expand_from_77(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #2 expansions from 7,7
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000000000000000000000000000000K0000000001"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000000000000000000000000000000000000000000K00000000000000001"))
        
    def test_expand_from_44(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 4
        targety = 4
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #8 expansions from 4,4
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 8)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000000000000010000000000000000K0000000000"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000000000100000000000000K000000000000"))
        self.assertEqual(expansions[2].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000000000000000000000000000000001000000000K00000000000000000"))
        self.assertEqual(expansions[3].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000000000100000K000000000000000000000"))
        self.assertEqual(expansions[4].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000K000001000000000000000000000000000"))
        self.assertEqual(expansions[5].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000K0000000001000000000000000000000000000"))
        self.assertEqual(expansions[6].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000K000000000000001000000000000000000000000000"))
        self.assertEqual(expansions[7].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000000000000000K00000000000000001000000000000000000000000000"))
        
    def test_expand_from_55(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #8 expansions from 5,5
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 8)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000000000000000000000010000000000000000K0"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000000000000000000100000000000000K000"))
        self.assertEqual(expansions[2].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000000000000000000000000000000000000000001000000000K00000000"))
        self.assertEqual(expansions[3].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000000000000000000100000K000000000000"))
        self.assertEqual(expansions[4].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000000000000K000001000000000000000000"))
        self.assertEqual(expansions[5].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("00000000000000000000000000000000000K0000000001000000000000000000"))
        self.assertEqual(expansions[6].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("000000000000000000000000000000K000000000000001000000000000000000"))
        self.assertEqual(expansions[7].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("0000000000000000000000000000K00000000000000001000000000000000000"))

    def test_expand_from_55_10turns(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        board.setTurnCount(9)
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM, turnCount=board.getTurnCount())
        
        #8 expansions from 5,5
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoard()
        
        self.assertEqual(expansions[0].getTurnCount(), 10)
        self.assertEqual(expansions[1].getTurnCount(), 10)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(len(expansions), 8)
        
        self.assertEqual(expansions[0].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0")
        self.assertEqual(expansions[1].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0")
        self.assertEqual(expansions[2].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0")
        self.assertEqual(expansions[3].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0")
        self.assertEqual(expansions[4].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
        self.assertEqual(expansions[5].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
        self.assertEqual(expansions[6].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
        self.assertEqual(expansions[7].getBoardStateStr(), "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,K,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0")
        
    def test_turns(self):
        #selectively expand a board and check state
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        ################
        kt_turn1 = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        
        #2 expansions from 0,0
        #may be ordered differently each run so expansion results are sorted
        
        #progression is 
        #turn 0 state: K000000000000000000000000000000000000000000000000000000000000000
        #turn 1 state: 10000000000000000K0000000000000000000000000000000000000000000000
        #turn 2 state: 10K0000000000000020000000000000000000000000000000000000000000000
        
        expansions = kt_turn1.expandBoard()
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
                
        self.assertEqual(len(expansions), 2)
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("10000000000000000K0000000000000000000000000000000000000000000000"))
        self.assertEqual(expansions[0].getTurnCount(), 1)

        ################
        kt_turn2 = KnightsTour(expansions[0].getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM, expansions[0].getTurnCount())

        #expanding from 1,2 (col,row)
        expansions = kt_turn2.expandBoard()
        self.assertEqual(len(expansions), 5)
        self.assertEqual(expansions[0].getTurnCount(), 2)
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("10K0000000000000020000000000000000000000000000000000000000000000"))
        
    def test_dont_backtrack(self):
        #expand a sparse board from 5,5 to 3,6 and check that expansions are all possible minus the previous one
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        board.setTurnCount(2)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        board.setSpace(3, 6, "1")
        
        ################
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM, board.getTurnCount())
        
        expansions = kt.expandBoard()
        
        #8 possible from row 6,col 3 - minus the last turn
        self.assertEqual(len(expansions), 7)
        
    def test_no_expansions_possible(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        board = ChessBoard(xdim, ydim)
        
        board.setBoardStateFromString(ChessBoard.BOARD_SPACE_DELIM.join("999999999999999999999999999999K999991999999999999999999999999999"))
        
        kt = KnightsTour(board.getBoardStateStr(), ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM, turnCount=board.getTurnCount())
        
        expansions = kt.expandBoard()
        self.assertEqual(len(expansions), 0)
        
if __name__ == '__main__':
    unittest.main()

