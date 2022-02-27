import logging

class ConwayGOL(object):
    def __init__(self, state):
        logging.basicConfig(format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)

        #print("Setting loglevel: %d" % logging.getLogger().getEffectiveLevel() )

        self.logger.setLevel( logging.getLogger().getEffectiveLevel() )

		#state is a 3x3 matrix, with the subject cell in the middle
		#even if it's a corner or edge
		
		self.width = 3
        self.height = 3
		
		#TODO: double check this is a 3x3 matrix of ints
        self.state = state
        
        self.nullState = -1
        self.deadState = 0
        self.aliveState = 1
        
        self.subjectCellState = self.state[1][1]
        
    def incrementState(self):

		#compute final state of subject cell, based on its neighbors
		neighborStatesOccupied = 0
		for x in range(width):
			for y in range(height):
				neighborStatesOccupied += (self.state[x][y] == self.aliveState);
					
		#don't count the middle
		if( state[1][1] == self.aliveState ):
			neighborStatesOccupied -= 1
	
		#Any live cell with fewer than two live neighbours dies, as if by underpopulation.
		#Any live cell with two or three live neighbours lives on to the next generation.
		#Any live cell with more than three live neighbours dies, as if by overpopulation.
		#Any dead cell with exactly three live neighbours becomes a live cell, as if by reproduction.

		#compute result state
		if( self.subjectCellState == self.deadState ):
			if( neighborStatesOccupied = 3):
				#sustain
				outputState = self.aliveState;
		elif( self.subjectCellState == self.aliveState ):
			if( neighborStatesOccupied < 2) :
				#underpopulated
				outputState = self.deadState;
			elif( neighborStatesOccupied <= 3):
				#sustain
				outputState = self.aliveState;
			else:
				# >3 alive neighbors
				#overpopulated
				outputState = self.deadState;
	
        return outputState
