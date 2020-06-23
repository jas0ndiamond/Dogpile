import logging

from PIL import Image

class Grayscaler(object):
    def __init__(self, sourceImage):

        #from Grayscaler import Grayscaler

        logging.basicConfig(format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)

        #print("Setting loglevel: %d" % logging.getLogger().getEffectiveLevel() )

        self.logger.setLevel( logging.getLogger().getEffectiveLevel() )

        self.sourceImage = sourceImage

    def getImageJobs(self):
        pass

    def getRowJobs(self):
        pass

    def getColJobs(self):
        pass

    def getClusterJobs(self):
        pass

    def grayscalePixel(self, pix):

        #from Grayscaler import Grayscaler

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
        #from Grayscaler import Grayscaler
        from PIL import Image

        #output = img = None

        try:
            #sourceImage = Image.open(file)
            width, height = self.sourceImage.size

            #our ouput image
            outputImage = Image.new('RGB', (width, height))

            pixelsProcessed = 0
            for x in range(width):
                for y in range(height):
                    pixel = self.sourceImage.getpixel( (x, y) )
                    #(42, 55, 48)

                    #self.logger.debug ("Found pixel %d,%d,%d" % pixel)

                    #convert pixel to grayscale and load it into pixels

                    outputImage.putpixel( (x,y) , self.grayscalePixel(pixel) )

                    pixelsProcessed += 1



            self.logger.debug("Processed pixels: %d" % pixelsProcessed)

            #output.save(outputFileName, "JPEG")
        finally:
            pass

        return outputImage
