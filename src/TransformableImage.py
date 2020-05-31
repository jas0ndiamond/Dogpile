#an image, loaded from a file, with the expectation that some transformation occurs on the image data


from PIL import Image

class TransformableImage:


    def __init__(self, file):
        self.sourceImage = Image.open(file)

        self.jobIds = []

        #built result object
        self.resultImageData = None


    def getPixelRows(self):
        pass

    def getImageCluster(self, x, y):
        pass

    def bindRow(self, job_id, row_num):
        pass

    def getRowByJobID(self, job_id):
        pass
