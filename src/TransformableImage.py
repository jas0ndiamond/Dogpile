import os

#an image, loaded from a file, with the expectation that some transformation occurs on the image data


from PIL import Image

class TransformableImage:


    def __init__(self, file, outputDir):

        self.file = file

        self.fileName = os.path.splitext( os.path.basename(self.file) )[0]

        self.outputDir = outputDir
        self.outputSuffix = "_output"

        #TODO: unhardcode these
        self.outputFormat = "JPEG"
        self.extension = "jpg"

        self.outputFile = os.path.realpath( self.outputDir + "/" + self.fileName + self.outputSuffix + "." + self.extension)

        #TODO: file existence check

        self.sourceImage = Image.open(file)

        #built result object
        (self.width, self.height) = self.sourceImage.size
        self.resultImageData = Image.new('RGB', (self.width, self.height))

        self.jobIds = {}

        print ("Built TransformableImage from %s, and outputFile %s" % (self.file, self.outputFile ) )

    def getFile(self):
        return self.file

    def getPixelRows(self):

        rows = []

        for i in range(self.height):
            print ("Cropping at (0, %d),(%d, %d)" % (i, self.width, i+1))
            rows.append( self.sourceImage.crop( (0, i, self.width, i+1) ) )

        return rows

    def getImageCluster(self, x, y):

        pass

    def bindRow(self, job_id, row_num):

        if(self.hasJobId(job_id) == False):
            self.jobIds[job_id] = row_num
        else:
            print("Attempted to rebind row for duplicate job id: %s => %s" % (job_id, self.jobIds.get(job_id) ) )

    def getRowByJobID(self, job_id):
        result = None

        if(self.hasJobId(job_id)):
            result = self.jobIds.get(job_id)
        else:
            print("No bound row for job id: %s" % job_id)

        return result

    def hasJobId(self, job_id):
        #print( "hashtable: %s" % self.jobIds )

        return (self.jobIds.get(job_id, None) != None)

    def writeResult(self, job_id, data):

        #lookup row number from job id

        row = self.getRowByJobID(job_id)

        #write data to that segment
        for i in range(data.width):
            self.resultImageData.putpixel( (i,row), data.getpixel( (i,0) ) )


    def writeImage(self):

        #outputDir/fileName+outputPrefix+extension
        print("Writing image to file %s" % self.outputFile)

        self.resultImageData.save( self.outputFile, self.outputFormat )
