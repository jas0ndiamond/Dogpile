import sys
import os

#import random

#import numpy as np

from PIL import Image

def grayscalePixel(pix):
    #(r,g,b)

    rPix = int(0.299 * pix[0])
    gPix = int(0.587 * pix[1])
    bPix = int(0.114 * pix[2])

    #randomize the pixel just for test kicks
    #rPix = pix[ random.randint(1, 2) ]

    avg = int(rPix + gPix + bPix/3 )

    return ( avg, avg, avg )

def main(args):

    if(len(args) != 2):
        print("Need a file")
        exit(1);

    file = args[1]

    print("Using file %s" % file)

    output = img = Image.open(file)
    width, height = img.size

    outputPixels = img.load()

    #output = Image.new("RGB", (width,height) )

    #pixels = np.asarray(img)


    print("Found dimensions width: %d, height: %d" % (width, height) )

    #pixels = list(img.getdata()) # convert image data to a list of integers
    # convert that to 2D list (list of lists of integers)
    #pixels = [pixels[offset:offset+width] for offset in range(0, width*height, width)]

    # At this point the image's pixels are all in memory and can be accessed
    # individually using data[row][col].


    pixelsProcessed = 0
    for x in range(width):
        for y in range(height):
            pixel = img.getpixel( (x, y) )
            #(42, 55, 48)

            #print ("Found pixel %d,%d,%d" % pixel)

            #convert pixel to grayscale and load it into pixels

            output.putpixel( (x,y) , grayscalePixel(pixel) )

            pixelsProcessed += 1



    print("Processed pixels: %d" % pixelsProcessed)

    output.save("output.jpg", "JPEG")

    img.close()

if __name__ == "__main__":
    main(sys.argv)
