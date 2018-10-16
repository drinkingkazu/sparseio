from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import numpy as np
import sys
import time
import threading

def threadio_func(io_handle, thread_id):

    while 1:
        time.sleep(0.00001)
        while not io_handle._locks[thread_id]:
            idx_v   = []
            data_v  = []
            label_v = []
            if io_handle._flags.SHUFFLE:
                idx_v = np.random.random([io_handle.batch_size()])*io_handle.num_entries()
                idx_v = idx_v.astype(np.int32)
            else:
                start = io_handle._start_idx[thread_id]
                end   = start + io_handle.batch_size()
                if end < io_handle.num_entries():
                    idx_v = np.arange(start,end)
                else:
                    idx_v = np.arange(start,io_handle.num_entries())
                    idx_v = np.concatenate([idx_v,np.arange(0,end-io_handle.num_entries())])
                next_start = start + len(io_handle._threads) * io_handle.batch_size()
                if next_start >= io_handle.num_entries():
                    next_start -= io_handle.num_entries()
                io_handle._start_idx[thread_id] = next_start
            
            for data_id, idx in enumerate(idx_v):
                data  = io_handle._data  [idx]
                data_v.append  (np.pad(data, [(0,0),(0,1)],'constant',constant_values=data_id))
                if len(io_handle._label):
                    label = io_handle._label [idx]
                    label_v.append (np.pad(label,[(0,0),(0,1)],'constant',constant_values=data_id))
            data_v  = np.vstack(data_v)
            if len(label_v): label_v = np.vstack(label_v)
            io_handle._buffs[thread_id] = (data_v,label_v,idx_v)
            io_handle._locks[thread_id] = True
    return

class io_base(object):

    def __init__(self,flags):
        self._batch_size   = flags.BATCH_SIZE
        self._num_entries  = -1
        self._num_channels = -1
        self._data         = [] # should be a list of numpy arrays
        self._label        = [] # should be a list of numpy arrays, same length as self._data
        # For circular buffer / thread function controls
        self._locks   = [False] * flags.INPUT_THREADS
        self._buffs   = [None ] * flags.INPUT_THREADS
        self._threads = [None ] * flags.INPUT_THREADS
        self._start_idx = [-1 ] * flags.INPUT_THREADS
        self._last_buffer_id = -1

    def num_entries(self):
        return self._num_entries

    def num_channels(self):
        return self._num_channels
        
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
        if self._threads[0] is not None:
            return
        for thread_id in range(len(self._threads)):
            print('Starting thread',thread_id)
            self._threads[thread_id] = threading.Thread(target = threadio_func, args=[self,thread_id])
            self._threads[thread_id].daemon = True
            self._threads[thread_id].start()
            
    def next(self,buffer_id=-1,release=True):
        if buffer_id >= len(self._locks):
            sys.stderr.write('Invalid buffer id requested: {:d}\n'.format(buffer_id))
            raise ValueError
        if buffer_id < 0: buffer_id = self._last_buffer_id + 1
        if buffer_id >= len(self._locks):
            buffer_id = 0
        if self._threads[buffer_id] is None:
            sys.stderr.write('Read-thread does not exist (did you initialize?)\n')
            raise ValueError
        while not self._locks[buffer_id]:
            time.sleep(0.000001)
        res = self._buffs[buffer_id]
        if release:
            self._buffs[buffer_id] = None
            self._locks[buffer_id] = False
            self._last_buffer_id   = buffer_id
        return res

    def batch_size(self,size=None):
        if size is None: return self._batch_size
        self._batch_size = int(size)

    def initialize(self):
        raise NotImplementedError

    def store(self,idx,softmax):
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

    def finalize(self):
        if self._fout:
            self._fout.close()

def io_factory(flags):
    if flags.IO_TYPE == 'h5':
        return io_h5(flags)
    if flags.IO_TYPE == 'larcv':
        return io_larcv(flags)
    raise NotImplementedError

def _test():
    import ioflags
    flags = ioflags._test()
    io = io_factory(flags)
    io.initialize()
    io.start_threads()
    return io
    
if __name__ == '__main__':
    io = _test()
    num_entries = io.num_entries()
    ctr = 0
    data_check = 0
    nfailures = 0
    tspent_v = []
    while ctr < num_entries:
        tstart = time.time()
        data,label,idx=io.next()
        tspent = time.time() - tstart
        tspent_v.append(tspent)
        msg = 'Read count {:d}/{:d} time {:g} index start={:d} end={:d} ({:d} entries) shape {:s}'
        msg = msg.format(ctr,num_entries,tspent,idx[0],idx[-1],len(idx),data.shape)
        ctr+=len(idx)
        print(msg)
        data_check += 1
        if data_check % 20 == 0:
            buf_start = data[0][0:3]
            buf_end   = data[-1][0:3]
            chk_start = io._data[idx[0]][0][0:3]
            chk_end   = io._data[idx[-1]][-1][0:3]
            good_start = (buf_start == chk_start).astype(np.int32).sum() == len(buf_start)
            good_end   = (buf_end   == chk_end  ).astype(np.int32).sum() == len(buf_end)

            print(buf_start,buf_end)
            print(chk_start,chk_end)
            print("Pass start/end? {:s}/{:s}".format(str(good_start),str(good_end)))
            if not good_start or not good_end:
                nfailures += 1
    io.finalize()
    tspent_v=np.array(tspent_v)
    print('Number of data check failures:',nfailures)
    print('Total time: {:g} [s] ... mean/std time-per-batch {:g}/{:g} [s]'.format(tspent_v.sum(),tspent_v.mean(),tspent_v.std()))
