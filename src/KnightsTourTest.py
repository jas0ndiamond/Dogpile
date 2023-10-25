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
        
        kt = KnightsTour( [board] )
        
        (xknight, yknight) = kt.findKnightPosition(board)
        
        self.assertEqual( xknight, targetx)
        self.assertEqual( yknight, targety)
        
    def test_find_knight2(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour( [board] )
        
        (xknight, yknight) = kt.findKnightPosition(board)
        
        self.assertEqual( xknight, targetx)
        self.assertEqual( yknight, targety)
        
    def test_find_knight3(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour( [board] )
        
        (xknight, yknight) = kt.findKnightPosition(board)
        
        self.assertEqual( xknight, targetx)
        self.assertEqual( yknight, targety)
        
    def test_expanded_boards_count1(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour( [ board ] )
        
        self.assertEqual( len(kt.expandBoards()), 2) 
        
    def test_expanded_boards_count2(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour( [ board ] )
        
        self.assertEqual( len(kt.expandBoards()), 2) 
        
    def test_expanded_boards_count3(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour( [board] )
        
        self.assertEqual( len(kt.expandBoards()), 2) 
        
    def test_expanded_boards_count4(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 2) 
        
    def test_expanded_boards_count5(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 1
        targety = 1
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 4) 
        
    def test_expanded_boards_count6(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 6
        targety = 1
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 4) 
        
    def test_expanded_boards_count7(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 1
        targety = 6
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 4) 
        
    def test_expanded_boards_count8(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 6
        targety = 6
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 4) 
        
    def test_expanded_boards_count9(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 2
        targety = 2
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 8) 
        
    def test_expanded_boards_count10(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 2
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 8) 
        
    def test_expanded_boards_count11(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 2
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 8) 
        
    def test_expanded_boards_count12(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        self.assertEqual( len(kt.expandBoards()), 8) 
    
    def test_expand_from_00(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        #2 expansions from 0,0
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("1OOOOOOOOOKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("1OOOOOOOOOOOOOOOOKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        
    def test_expand_from_07(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        #2 expansions from 0,7
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())

        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOO1OOOOOOO"))        
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOO1OOOOOOO"))
        
    def test_expand_from_70(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        #2 expansions from 7,0
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOO1OOOOOKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOO1OOOOOOOOOOOOOOKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        
    def test_expand_from_77(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 7
        targety = 7
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        #2 expansions from 7,7
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 2)
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOOOO1"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOO1"))
        
    def test_expand_from_44(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 4
        targety = 4
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        #8 expansions from 4,4
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 8)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        self.assertEqual(expansions[2].getTurnCount(), 1)
        self.assertEqual(expansions[3].getTurnCount(), 1)
        self.assertEqual(expansions[4].getTurnCount(), 1)
        self.assertEqual(expansions[5].getTurnCount(), 1)
        self.assertEqual(expansions[6].getTurnCount(), 1)
        self.assertEqual(expansions[7].getTurnCount(), 1)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[2].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[3].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[4].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOKOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[5].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOKOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[6].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOKOOOOOOOOOOOO"))
        self.assertEqual(expansions[7].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOKOOOOOOOOOO"))
        
    def test_expand_from_55(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board])
        
        #8 expansions from 5,5
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 8)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        self.assertEqual(expansions[2].getTurnCount(), 1)
        self.assertEqual(expansions[3].getTurnCount(), 1)
        self.assertEqual(expansions[4].getTurnCount(), 1)
        self.assertEqual(expansions[5].getTurnCount(), 1)
        self.assertEqual(expansions[6].getTurnCount(), 1)
        self.assertEqual(expansions[7].getTurnCount(), 1)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[2].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOO1OOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[3].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOO1OOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[4].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOKOOOOOOOOOOOO"))
        self.assertEqual(expansions[5].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOKOOOOOOOO"))
        self.assertEqual(expansions[6].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOKOOO"))
        self.assertEqual(expansions[7].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOKO"))
        
    def test_expand_from_55_10turns(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 5
        targety = 5
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        board.setTurnCount(9)
        
        kt = KnightsTour([board])
        
        #8 expansions from 5,5
        #may be ordered differently each run so expansion results are sorted
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 8)
        
        self.assertEqual(expansions[0].getTurnCount(), 10)
        self.assertEqual(expansions[1].getTurnCount(), 10)
        self.assertEqual(expansions[2].getTurnCount(), 10)
        self.assertEqual(expansions[3].getTurnCount(), 10)
        self.assertEqual(expansions[4].getTurnCount(), 10)
        self.assertEqual(expansions[5].getTurnCount(), 10)
        self.assertEqual(expansions[6].getTurnCount(), 10)
        self.assertEqual(expansions[7].getTurnCount(), 10)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[7].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O")
        self.assertEqual(expansions[6].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O")
        self.assertEqual(expansions[5].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[4].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[3].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[2].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[1].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[0].getBoardStateStr(), "O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        
    def test_expand_multiple(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        #5,5 - 10th turn
        targetx = 5
        targety = 5
        
        board1 = ChessBoard(xdim, ydim)
        
        board1.setSpace(targetx, targety, ChessBoard.KNIGHT)
        board1.setSpace(0,0, ChessBoard.DEAD)   #helps with sorting results by board state
        board1.setTurnCount(9)
        
        #4,4 - 1st turn
        targetx = 4
        targety = 4
        
        board2 = ChessBoard(xdim, ydim)
        
        board2.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        kt = KnightsTour([board1, board2])
        
        expansions = kt.expandBoards()
        
        self.assertEqual(len(expansions), 16)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
        
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)
        self.assertEqual(expansions[2].getTurnCount(), 1)
        self.assertEqual(expansions[3].getTurnCount(), 1)
        self.assertEqual(expansions[4].getTurnCount(), 1)
        self.assertEqual(expansions[5].getTurnCount(), 1)
        self.assertEqual(expansions[6].getTurnCount(), 1)
        self.assertEqual(expansions[7].getTurnCount(), 1)
        
        self.assertEqual(expansions[8].getTurnCount(), 10)
        self.assertEqual(expansions[9].getTurnCount(), 10)
        self.assertEqual(expansions[10].getTurnCount(), 10)
        self.assertEqual(expansions[11].getTurnCount(), 10)
        self.assertEqual(expansions[12].getTurnCount(), 10)
        self.assertEqual(expansions[13].getTurnCount(), 10)
        self.assertEqual(expansions[14].getTurnCount(), 10)
        self.assertEqual(expansions[15].getTurnCount(), 10)
        
        self.assertEqual(expansions[0].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[1].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOKOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[2].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[3].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOKOOOOO1OOOOOOOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[4].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOKOOOOOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[5].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOKOOOOOOOOOOOOOOOOO"))
        self.assertEqual(expansions[6].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOKOOOOOOOOOOOO"))
        self.assertEqual(expansions[7].getBoardStateStr(), ChessBoard.BOARD_SPACE_DELIM.join("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO1OOOOOOOOOOOOOOOOKOOOOOOOOOO"))
        
        self.assertEqual(expansions[8].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[9].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[10].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[11].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[12].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,K,O,O,O,O,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[13].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,K,O,O,O,O,O,O,O,O")
        self.assertEqual(expansions[14].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O,O,O")
        self.assertEqual(expansions[15].getBoardStateStr(), 
        "X,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,10,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,O,K,O")
        
    def test_turns(self):
        #selectively expand a board and check state
        
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        targetx = 0
        targety = 0
        
        board = ChessBoard(xdim, ydim)
        
        board.setSpace(targetx, targety, ChessBoard.KNIGHT)
        
        ################
        kt_turn1 = KnightsTour([board])
        
        #2 expansions from 0,0
        #may be ordered differently each run so expansion results are sorted
        
        #progression is 
        #turn 0 state: KOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
        #turn 1 state: 1OOOOOOOOOOOOOOOOKOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
        #turn 2 state: 1OKOOOOOOOOOOOOOO2OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
        
        expansions = kt_turn1.expandBoards()
        
        self.assertEqual(len(expansions), 2)
        
        expansions.sort(key = lambda board: board.getBoardStateStr())
                
        self.assertEqual(expansions[0].getTurnCount(), 1)
        self.assertEqual(expansions[1].getTurnCount(), 1)


        ################
        kt_turn2 = KnightsTour( [expansions[0]] )

        #expanding from 1,2 (col,row)
        expansions = kt_turn2.expandBoards()
        self.assertEqual(len(expansions), 5)
        self.assertEqual(expansions[0].getTurnCount(), 2)
        self.assertEqual(expansions[1].getTurnCount(), 2)
        self.assertEqual(expansions[2].getTurnCount(), 2)
        self.assertEqual(expansions[3].getTurnCount(), 2)
        self.assertEqual(expansions[4].getTurnCount(), 2)
        
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
        kt = KnightsTour( [board] )
        
        expansions = kt.expandBoards()
        
        #8 possible from row 6,col 3 - minus the last turn
        self.assertEqual(len(expansions), 7)
        
    def test_no_expansions_possible(self):
        xdim = ydim = ChessBoard.STANDARD_BOARD_DIM
        
        board = ChessBoard(xdim, ydim)
        
        board.setBoardStateFromString(ChessBoard.BOARD_SPACE_DELIM.join("999999999999999999999999999999K999991999999999999999999999999999"))
        
        kt = KnightsTour([board])
        
        expansions = kt.expandBoards()
        self.assertEqual(len(expansions), 0)
        
    def test_expansions_successive(self):
        xdim = ydim = 5
        
        board = ChessBoard(xdim, ydim)
        board.setSpace(2, 2, ChessBoard.KNIGHT)
        
        kt1 = KnightsTour([board])
        gen1_expansions = kt1.expandBoards()
        
        #8 unique gen1 expansions from 2,2 on a 5x5 board
        self.assertEqual(len(gen1_expansions), 8)
        
        gen1_expansion_hashcodes = set()
        for expansion in gen1_expansions:
            print("board:\n%s\n==========\n" % expansion.dump())
            gen1_expansion_hashcodes.add(expansion.getHashCode())
            
        #check hashcode uniqueness
        self.assertEqual(len(gen1_expansion_hashcodes), 8)
        
        #another kt object since in practice another node will likely compute the expansion
        kt2 = KnightsTour(gen1_expansions)
        gen2_expansions = kt2.expandBoards()
        
        #16 unique gen2 expansions from 2,2 on a 5x5 board
        self.assertEqual(len(gen2_expansions), 16)
        
        gen2_expansion_hashcodes = set()
        for expansion in gen2_expansions:
            print("board:\n%s\n==========\n" % expansion.dump())
            gen2_expansion_hashcodes.add(expansion.getHashCode())
        
        #check hashcode uniqueness
        self.assertEqual(len(gen2_expansion_hashcodes), 16)
        
    def test_tour_finished(self):
        pass
        #TODO: implement
        
        
        
if __name__ == '__main__':
    unittest.util._MAX_LENGTH=2000
    unittest.main()

