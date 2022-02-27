import logging

from PIL import Image

class Redscaler(object):
    def __init__(self, sourceImage):

        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

        self.sourceImage = sourceImage


    def getImageJobs(self):
        pass

    def getRowJobs(self):
        pass

    def getColJobs(self):
        pass

    def getClusterJobs(self):
        pass

    def redscalePixel(self, pix):

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

        return ( pix[0], avg, avg )

    def redscaleImage(self):
        #from Grayscaler import Grayscaler
        from PIL import Image


        try:

            width, height = self.sourceImage.size

            #our ouput image
            outputImage = Image.new('RGB', (width, height))


            #outputPixels = img.load()

            pixelsProcessed = 0
            for x in range(width):
                for y in range(height):
                    pixel = self.sourceImage.getpixel( (x, y) )
                    #(42, 55, 48)

                    #convert pixel to redscale and load it into pixels

                    outputImage.putpixel( (x,y) , self.grayscalePixel(pixel) )

                    pixelsProcessed += 1



            print("Processed pixels: %d" % pixelsProcessed)

        finally:
            pass

        return outputImage
