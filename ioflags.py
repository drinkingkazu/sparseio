from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import argparse
from distutils.util import strtobool

def add_attributes(flags):
    # flags for IO
    flags.IO_TYPE    = 'h5'
    flags.INPUT_FILE = '/scratch/kterao/dlprod_ppn_v08/dgcnn_p02_test_4ch.hdf5'
    flags.OUTPUT_FILE = ''
    flags.BATCH_SIZE = 1
    flags.DATA_KEY   = 'data'
    flags.LABEL_KEY  = ''
    flags.SHUFFLE    = 1

def add_arguments(flags,parser):

    parser.add_argument('-io','--io_type',type=str,default=flags.IO_TYPE,
                        help='IO handler type [default: %s]' % flags.IO_TYPE)
    parser.add_argument('-if','--input_file',type=str,default=flags.INPUT_FILE,
                        help='comma-separated input file list [default: %s]' % flags.INPUT_FILE)
    parser.add_argument('-of','--output_file',type=str,default=flags.OUTPUT_FILE,
                        help='output file name [default: %s]' % flags.OUTPUT_FILE)
    parser.add_argument('-bs','--batch_size', type=int, default=flags.BATCH_SIZE,
                        help='Batch Size during training for updating weights [default: %s]' % flags.BATCH_SIZE)
    parser.add_argument('-dkey','--data_key',type=str,default=flags.DATA_KEY,
                        help='A keyword to fetch data from file [default: %s]' % flags.DATA_KEY)
    parser.add_argument('-lkey','--label_key',type=str,default=flags.LABEL_KEY,
                        help='A keyword to fetch label from file [default: %s]' % flags.LABEL_KEY)
    parser.add_argument('-sh','--shuffle',type=strtobool,default=flags.SHUFFLE,
                        help='Shuffle the data entries [default: %s]' % flags.SHUFFLE)
    return parser
        
def update_attributes(flags, args):
    flags.INPUT_FILE=[str(f) for f in flags.INPUT_FILE.split(',')]

def _test():
    parser = argparse.ArgumentParser(description="iotest flags")
    class FLAGS:
        pass
    flags=FLAGS()
    add_attributes(flags)
    add_arguments(flags,parser)

    args = parser.parse_args()
    update_attributes(flags,vars(args))
    print("\n\n-- CONFIG --")
    for name in vars(flags):
        attribute = getattr(flags,name)
        print("%s = %r" % (name, getattr(flags, name)))

    return flags
        
if __name__ == '__main__':
    _test()

        

    
