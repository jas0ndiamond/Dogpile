import logging

from PIL import Image

class Grayscaler(object):
    def __init__(self, sourceImage):

        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

        self.sourceImage = sourceImage

        #self.outputImage = None

        #TODO: throw exception if file doesn't exist


    def getImageJobs(self):
        pass

    def getRowJobs(self):
        pass

    def getColJobs(self):
        pass

    def getClusterJobs(self):
        pass

    def grayscalePixel(self, pix):
        #(r,g,b)

        # rPix = 0.299 * pix[0]
        # gPix = 0.587 * pix[1]
        # bPix = 0.114 * pix[2]

        #randomize the pixel just for test kicks
        #rPix = pix[ random.randint(1, 2) ]

        #scalars from internet dogma
        #take the average value, and set it as pixel values for each of r,g,b
        avg = int(
            (
                (0.299 * pix[0]) +
                (0.587 * pix[1]) +
                (0.114 * pix[2])
            )
            /3
        )

        return ( avg, avg, avg )

    # def setOutputDir(self, dir):
    #     self.outputDir = dir

    def grayscaleImage(self):
        from PIL import Image
        #output = img = None

        try:
            #sourceImage = Image.open(file)
            width, height = self.sourceImage.size

            #our ouput image
            outputImage = Image.new('RGB', (width, height))


            #outputPixels = img.load()

            pixelsProcessed = 0
            for x in range(width):
                for y in range(height):
                    pixel = self.sourceImage.getpixel( (x, y) )
                    #(42, 55, 48)

                    #print ("Found pixel %d,%d,%d" % pixel)

                    #convert pixel to grayscale and load it into pixels

                    outputImage.putpixel( (x,y) , self.grayscalePixel(pixel) )

                    pixelsProcessed += 1



            print("Processed pixels: %d" % pixelsProcessed)

            #output.save(outputFileName, "JPEG")
        finally:
            pass

        return outputImage

    # def start(self):
    #     print("starting")
    #
    #     for file in self.files:
    #         print("file %s" % file)
    #         outputFile = ("%s/%s_output.jpg" % (self.outputDir, os.path.basename(file) ) )
    #
    #         self.grayscaleImage( Image.open(file) ).save(outputFile, "JPEG")

###############################
