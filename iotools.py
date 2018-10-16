from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import numpy as np
import sys
import threading

def threadio_func(io_handle, storage, thread_id):

    while 1:
        while not storage._locks[thread_id]:
            idx_v   = []
            data_v  = []
            label_v = []
            if io_handle._flags.SHUFFLE:
                idx = np.random.random([io_handle.num_entries()])*io_handle.num_entries()
                idx = idx.astype(np.int32)
            else:
                start = storage._start_idx[thread_id]
                end   = start + io_handle.batch_size()
                idx = np.arange(start,end)
                storage._start_idx[thread_id] = start + len(storage._threads) * io_handle.batch_size()
            
            for i in idx:
                data  = io_handle.data()[i]
                label = io_handle.label()[i]
                data_v.append(np.pad(data,(0,1),'constant',constant_values=(0,i)))
                label_
                              
                data_v.append(io_handle.data()[i])
                label_v.append(io_handle.label()[i])
                idx_v.append(np.array([i]*len(data_v[-1])))
            data  = np.vstack(data_v)
            label = np.vstack(label_v)
                
    
    storage._threaded = True
    while storage._threaded:
        time.sleep(0.000005)
        if storage._filled: continue
        storage._read_start_time = time.time()
        while 1:
            if proc.storage_status_array()[storage._storage_id] == 3:
                storage._read_end_time=time.time()
                break
            time.sleep(0.000005)
            continue
        storage.next()
        storage._event_ids     = proc.processed_entries(storage._storage_id)
        storage._ttree_entries = proc.processed_entries(storage._storage_id)
    return

class ibuffer:
    def __init__(self,flags):
        self._locks   = [True] * flags.INPUT_THREADS
        self._buffs   = [None] * flags.INPUT_THREADS
        self._threads = [None] * flags.INPUT_THREADS
        self._start_idx = [-1] * flags.INPUT_THREADS
        self._end_idx   = [-1] * flags.INPUT_THREADS
        self._batch_size = flags.BATCH_SIZE
        self._last_buffer_id = -1

    def stop_threads(self):
        if self._threasd[0] is None:
            return
        for i in range(len(self._threads)):
            while self._locks[buffer_id]:
                time.sleep(0.000001)
            self._buffs[i] = None
            self._start_idx[i] = -1

    def set_index_start(self,idx):
        self.stop_threads()
        for i in range(len(self._threads)):
            self._start_idx[i] = idx + i*self._batch_size

    def start_threads(self):
        

    def get(buffer_id=-1,release=True):
        if buffer_id >= len(self._locks):
            sys.stderr.write('Invalid buffer id requested: {:d}\n'.format(buffer_id))
            raise ValueError
        if buffer_id < 0: buffer_id = self._last_buffer_id + 1
        if buffer_id >= len(self._locks):
            buffer_id = 0
        if self._buffs[buffer_id] is None:
            sys.stderr.write('Read-thread does not exist (did you initialize?)\n')
            raise ValueError
        while self._locks[buffer_id]:
            time.sleep(0.000001)
        res = self._buffs[buffer_id]
        self._buffs[buffer_id] = None
        self._locks[buffer_id] = False
        self._last_buffer_id   = buffer_id
        return res

class io_base(object):

    def __init__(self,flags):
        self._batch_size   = flags.BATCH_SIZE
        self._num_entries  = -1
        self._num_channels = -1
        self._data         = [] # should be a list of numpy arrays
        self._label        = [] # should be a list of numpy arrays, same length as self._data

    def start_manager(self):
   
    def batch_size(self,size=None):
        if size is None: return self._batch_size
        self._batch_size = int(size)

    def num_entries(self):
        return self._num_entries

    def num_channels(self):
        return self._num_channels

    def initialize(self):
        raise NotImplementedError

    def store(self,idx,softmax):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def finalize(self):
        raise NotImplementedError

class io_larcv(io_base):

    def __init__(self,flags):
        super(io_larcv,self).__init__(flags=flags)
        self._flags  = flags
        self._data   = None
        self._label  = None
        self._fout   = None
        self._last_entry = -1
        self._event_keys = []
        self._metas      = []

    def initialize(self):
        self._last_entry = -1
        self._event_keys = []
        self._metas = []
        # configure the input
        from larcv import larcv
        from ROOT import TChain
        ch_data   = TChain('sparse3d_%s_tree' % self._flags.DATA_KEY)
        ch_label  = None
        if self._flags.LABEL_KEY:
            ch_label  = TChain('sparse3d_%s_tree' % self._flags.LABEL_KEY)
        for f in self._flags.INPUT_FILE:
            ch_data.AddFile(f)
            if ch_label:  ch_label.AddFile(f)
        self._data   = []
        self._label  = []
        br_data,br_label=(None,None)
        event_fraction = 1./ch_data.GetEntries() * 100.
        total_point = 0.
        for i in range(ch_data.GetEntries()):
            ch_data.GetEntry(i)
            if ch_label:  ch_label.GetEntry(i)
            if br_data is None:
                br_data  = getattr(ch_data, 'sparse3d_%s_branch' % self._flags.DATA_KEY)
                if ch_label:  br_label  = getattr(ch_label, 'sparse3d_%s_branch' % self._flags.LABEL_KEY)
            num_point = br_data.as_vector().size()
            if num_point < 256: continue
            
            np_data  = np.zeros(shape=(num_point,4),dtype=np.float32)
            larcv.fill_3d_pcloud(br_data,  np_data)
            self._data.append(np_data)
            self._event_keys.append((br_data.run(),br_data.subrun(),br_data.event()))
            self._metas.append(larcv.Voxel3DMeta(br_data.meta()))
            if ch_label:
                np_label = np.zeros(shape=(num_point,1),dtype=np.float32)
                larcv.fill_3d_pcloud(br_label, np_label)
                np_label = np_label.reshape([num_point]) - 1.
                self._label.append(np_label)
            total_point += np_data.size
            sys.stdout.write('Processed %d%% ... %d MB\r' % (int(event_fraction*i),int(total_point*4*2/1.e6)))
            sys.stdout.flush()

        sys.stdout.write('\n')
        sys.stdout.flush()
        self._num_channels = self._data[-1].shape[-1]
        self._num_entries = len(self._data)
        # Output
        if self._flags.OUTPUT_FILE:
            import tempfile
            cfg = '''
IOManager: {
      Verbosity:   2
      Name:        "IOManager"
      IOMode:      1
      OutFileName: "%s"
      InputFiles:  []
      InputDirs:   []
      StoreOnlyType: []
      StoreOnlyName: []
    }
                  '''
            cfg = cfg % self._flags.OUTPUT_FILE
            cfg_file = tempfile.NamedTemporaryFile('w')
            cfg_file.write(cfg)
            cfg_file.flush()
            self._fout = larcv.IOManager(cfg_file.name)
            self._fout.initialize()
            
    def next(self):
        data,label=(None,None)
        start,end=(-1,-1)
        if self._flags.SHUFFLE:
            start = int(np.random.random() * (self.num_entries() - self.batch_size()))
            end   = start + self.batch_size()
            idx   = np.arange(start,end,1)
            data = self._data[start:end]
            if len(self._label)  > 0: label  = self._label[start:end]
        else:
            start = self._last_entry+1
            end   = start + self.batch_size()
            if end < self.num_entries():
                idx = np.arange(start,end,1)
                data = self._data[start:end]
                if len(self._label)  > 0: label  = self._label[start:end]
            else:
                idx = np.concatenate([np.arange(start,self.num_entries(),1),np.arange(0,end-self.num_entries(),1)])
                data = self._data[start:] + self._data[0:end-self.num_entries()]
                if len(self._label)  > 0: label  = self._label[start:]  + self._label[0:end-self.num_entries()]
        self._last_entry = idx[-1]

        return idx,data,label

    def store(self,idx,softmax):
        from larcv import larcv
        if self._fout is None:
            raise NotImplementedError
        idx=int(idx)
        if idx >= self.num_entries():
            raise ValueError
        keys = self._event_keys[idx]
        meta = self._metas[idx]
        
        larcv_data = self._fout.get_data('sparse3d',self._flags.DATA_KEY)
        data = self._data[idx]
        vs = larcv.as_tensor3d(data,meta,0.)
        larcv_data.set(vs,meta)

        pos = data[:,0:3]
        score = np.max(softmax,axis=1).reshape([len(softmax),1])
        score = np.concatenate([pos,score],axis=1)
        prediction = np.argmax(softmax,axis=1).astype(np.float32).reshape([len(softmax),1])
        prediction = np.concatenate([pos,prediction],axis=1)
        
        larcv_softmax = self._fout.get_data('sparse3d','softmax')
        vs = larcv.as_tensor3d(score,meta,-1.)
        larcv_softmax.set(vs,meta)

        larcv_prediction = self._fout.get_data('sparse3d','prediction')
        vs = larcv.as_tensor3d(prediction,meta,-1.)
        larcv_prediction.set(vs,meta)
        
        if len(self._label) > 0:
            label = self._label[idx]
            label = label.astype(np.float32).reshape([len(label),1])
            label = np.concatenate([pos,label],axis=1)
            larcv_label = self._fout.get_data('sparse3d','label')
            vs = larcv.as_tensor3d(label,meta,-1.)
            larcv_label.set(vs,meta)

        self._fout.set_id(keys[0],keys[1],keys[2])
        self._fout.save_entry()
        
    def finalize(self):
        if self._fout:
            self._fout.finalize()
            
class io_h5(io_base):

    def __init__(self,flags):
        super(io_h5,self).__init__(flags=flags)
        self._flags  = flags
        self._data   = None
        self._label  = None
        self._fout   = None
        self._ohandler_data = None
        self._ohandler_label = None
        self._ohandler_softmax = None
        self._has_label = False

    def initialize(self):
        self._last_entry = -1
        # Prepare input
        import h5py as h5
        self._data   = None
        self._label  = None
        for f in self._flags.INPUT_FILE:
            f = h5.File(f,'r')
            if self._data is None:
                self._data  = np.array(f[self._flags.DATA_KEY ])
                if self._flags.LABEL_KEY : self._label  = np.array(f[self._flags.LABEL_KEY])
            else:
                self._data  = np.concatenate(self._data, np.array(f[self._flags.DATA_KEY ]))
                if self._label  : self._label  = np.concatenate(self._label, np.array(f[self._flags.LABEL_KEY ]))
        self._num_channels = self._data[-1].shape[-1]
        self._num_entries = len(self._data)
        # Prepare output
        if self._flags.OUTPUT_FILE:
            import tables
            FILTERS = tables.Filters(complib='zlib', complevel=5)
            self._fout = tables.open_file(self._flags.OUTPUT_FILE,mode='w', filters=FILTERS)
            data_shape = list(self._data[0].shape)
            data_shape.insert(0,0)
            self._ohandler_data = self._fout.create_earray(self._fout.root,self._flags.DATA_KEY,tables.Float32Atom(),shape=data_shape)
            self._ohandler_softmax = self._fout.create_earray(self._fout.root,'softmax',tables.Float32Atom(),shape=data_shape)
            if self._label:
                data_shape = list(self._label[0].shape)
                data_shape.insert(0,0)
                self._ohandler_label = self._fout.create_earray(self._fout.root,self._flags.LABEL_KEY,tables.Float32Atom(),shape=data_shape)
    def store(self,idx,softmax):
        if self._fout is None:
            raise NotImplementedError
        idx=int(idx)
        if idx >= self.num_entries():
            raise ValueError
        data = self._data[idx]
        self._ohandler_data.append(data[None])
        self._ohandler_softmax.append(softmax[None])
        if self._label is not None:
            label = self._label[idx]
            self._ohandler_label.append(label[None])

    def next(self):
        idx = None
        if self._flags.SHUFFLE:
            idx = np.arange(self.num_entries())
            np.random.shuffle(idx)
            idx = idx[0:self.batch_size()]
        else:
            start = self._last_entry+1
            end   = start + self.batch_size()
            if end < self.num_entries():
                idx = np.arange(start,end)
            else:
                idx = np.concatenate([np.arange(start,self.num_entries()),np.arange(0,end-self.num_entries())])
        self._last_entry = idx[-1]
        data  = self._data[idx, ...]
        label = None
        if self._label  : label  = self._label[idx, ...]
        return idx, data, label

    def finalize(self):
        if self._fout:
            self._fout.close()

def io_factory(flags):
    if flags.IO_TYPE == 'h5':
        return io_h5(flags)
    if flags.IO_TYPE == 'larcv':
        return io_larcv(flags)
    raise NotImplementedError

if __name__ == '__main__':
    from ioflags import _test
    flags = _test()
    io = io_factory(flags)
    io.initialize()
    num_entries = io.num_entries()
    ctr = 0
    while ctr < num_entries:
        idx,data,label=io.next()
        msg = str(ctr) + '/' + str(num_entries) + ' ... '  + str(idx) + ' ' + str(data[0].shape)
        if label:
            msg += str(label[0].shape)
        print(msg)
        ctr += len(data)
    io.finalize()

