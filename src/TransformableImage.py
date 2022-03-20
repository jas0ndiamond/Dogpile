import os
import logging

#an image, loaded from a file, with the expectation that some transformation occurs on the image data

from PIL import Image

from ClusterJobResult import ClusterJobResult

#TODO: generic container for an entity to be handled by DogPileTasks
class TransformableImage(ClusterJobResult):

    def __init__(self, imageFile, outputDir):
        
        super().__init__()
        
        #set the log level explicitly. effective log level may not be available
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.INFO )

        self.imageFile = imageFile

        self.fileName = os.path.splitext( os.path.basename(self.imageFile) )[0]

        self.outputDir = outputDir
        self.outputSuffix = "_output"

        #TODO: unhardcode these
        self.outputFormat = "JPEG"
        self.extension = "jpg"

        self.outputFile = os.path.realpath( self.outputDir + "/" + self.fileName + self.outputSuffix + "." + self.extension)

        #TODO: file existence check

        self.sourceImage = Image.open(imageFile)

        #built result object
        (self.width, self.height) = self.sourceImage.size
        self.resultImage = None

        self.jobIds = {}
        self.resultData = {}

        self.logger.info ("Built TransformableImage from %s, and outputFile %s" % (self.imageFile, self.outputFile ) )

    def getFile(self):
        return self.imageFile

    def getPixelRows(self):

        rows = []

        for i in range(self.height):
            self.logger.debug ("Cropping at (0, %d),(%d, %d)" % (i, self.width, i+1))
            rows.append( self.sourceImage.crop( (0, i, self.width, i+1) ) )

        return rows

    def getImageCluster(self, x, y):

        pass

    def bindRow(self, job_id, row_num):

        if(self.hasJobId(job_id) == False):
            self.jobIds[job_id] = row_num
        else:
            self.logger.warning("Attempted to rebind row for duplicate job id: %s => %s" % (job_id, self.jobIds.get(job_id) ) )

    def getRowByJobID(self, job_id):
        result = None

        if(self.hasJobId(job_id)):
            result = self.jobIds.get(job_id)
        else:
            self.logger.warning("No bound row for job id: %s" % job_id)

        return result

    def writeResult(self, job_id, data):

        #lookup row number from job id

        row = self.getRowByJobID(job_id)

        self.resultData[row] = data

        #write data to that segment
        # for i in range(data.width):
        #     self.resultImageData.putpixel( (i,row), data.getpixel( (i,0) ) )


    def writeImage(self):

        #outputDir/fileName+outputPrefix+extension
        self.logger.info("Writing image to file %s" % self.outputFile)

        self.resultImage = Image.new('RGB', (self.width, self.height))


        #for each row  in resultData, write that data to the result image in the correct location
        #for i in range(self.height):

        for y in self.resultData:
            pixelrow = self.resultData[y]

            for x in range(pixelrow.width):
                self.resultImage.putpixel( (x,y), pixelrow.getpixel( (x,0) ) )


        self.resultImage.save( self.outputFile, self.outputFormat )
        
    def toString(self):
        return "I am a transformable image: %s" % self.imageFile
