from PIL import Image

#encap a set of pixels, their result mapping, and an id
class PixelGroup:
    def __init__(self, id):
        pass

    def add_mapping(self, (source_x, source_y), (dest_x, dest_y)):
        pass

    def add_mapping(self, (source_x, source_y)):
        self.add_mapping( (source_x,source_y), (source_x,source_y))
