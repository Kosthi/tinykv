package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}

	modify := storage.Modify{
		Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf},
	}

	if err := server.storage.Write(req.Context, []storage.Modify{modify}); err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}

	modify := storage.Modify{
		Data: storage.Delete{Key: req.Key, Cf: req.Cf},
	}

	if err := server.storage.Write(req.Context, []storage.Modify{modify}); err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	i := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if i >= req.Limit {
			break
		}
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		i++
	}

	return resp, nil
}
