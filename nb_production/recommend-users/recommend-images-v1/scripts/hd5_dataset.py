import h5py as h5

from PIL import Image
from numpy import ndarray
from torch.utils import data
from torch import Tensor

class HD5Dataset(data.Dataset):
    '''Custom pytorch-inheriting dataset to manage HD5F-formatted image data (stored as numpy arrays within hd5 datasets).
    If a separate `labels` archive is being opened, its location and dataset name can be passed in as well, though both will be 
    returned by the __getitem__ method invoked by the __iter__ method.
    
    `image_dimensions` should be a 3-d vector with the color-channel in the final position for PIL-compatibility.
    
    '''
    def __init__(self, data_path, dataset_label, indices_path = None, indices_label= None, transforms=None, image_dims = None):
        super().__init__()
        try:
            if indices_path:
                try:
                    self.labels_archive = h5.File(indices_path, 'r')
                    self.labels = self.labels_archive[indices_label]
                except:
                    print("Check paths and label names for data sets.")
            else:
                self.labels = None
            self.archive = h5.File(data_path, 'r')
            self.data = self.archive[dataset_label]
            self.transforms = transforms
            self.image_dims = image_dims
        except Exception as e:
            print(f"Opening or transforming archive failed with {e}.")
        
    
    def __getitem__(self, index):
        datum = self.data[index]
        assert type(datum) == ndarray
        if self.transforms is not None:
            if self.image_dims is not None:
                datum = self.convert_to_PIL(datum)
            datum = self.transforms(datum)
        if self.labels:
            label = self.labels[index]
            return datum, label
        else:
            return datum

    def __len__(self):
        return len(self.labels)
    
    def convert_to_PIL(self, array):
        try:
            img = Image.fromarray(array.reshape(self.image_dims))
        except Exception as e:
            print(e)
        return img
    
    def close(self):
        if self.labels_archive:
            self.labels_archive.close()
        self.archive.close()