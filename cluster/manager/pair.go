package manager

import (
	"crypto/rand"
	"encoding/json"
	"fmt"

	"github.com/libopenstorage/openstorage/api"
	clusterclient "github.com/libopenstorage/openstorage/api/client/cluster"
	"github.com/libopenstorage/openstorage/cluster"
	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

const (
	// ClusterPairKey is the key at which info about cluster pairs is stored in kvdb
	ClusterPairKey = "cluster/pair"
)

// CreatePair remote pairs this cluster with a remote cluster.
func (c *ClusterManager) CreatePair(
	request *api.CreateClusterPairRequest,
) error {
	remoteIp := request.RemoteClusterIp

	// Pair with remote server
	logrus.Infof("Attempting to pair with cluster at IP %v", remoteIp)

	processRequest := &api.ProcessClusterPairRequest{
		SourceClusterId:    c.config.ClusterId,
		RemoteClusterToken: request.RemoteClusterToken,
	}

	clnt, err := clusterclient.NewClusterClient(remoteIp, cluster.APIVersion)
	if err != nil {
		return err
	}
	remoteCluster := clusterclient.ClusterManager(clnt)

	// Issue a remote pair request
	resp, err := remoteCluster.ProcessPairRequest(processRequest)
	if err != nil {
		logrus.Warnf("Unable to pair with %v: %v", remoteIp, err)
		return err
	}

	// Alert all listeners that we are pairing with a cluster.
	for e := c.listeners.Front(); e != nil; e = e.Next() {
		err = e.Value.(cluster.ClusterListener).CreatePair(
			&c.selfNode,
			resp,
		)
		if err != nil {
			logrus.Errorf("Unable to notify %v on a cluster pair event: %v",
				e.Value.(cluster.ClusterListener).String(),
				err,
			)
			return err
		}
	}

	pairInfo := &api.ClusterPairInfo{
		Uuid:    resp.RemoteClusterUuid,
		Ip:      request.RemoteClusterIp,
		Port:    request.RemoteClusterPort,
		Token:   request.RemoteClusterToken,
		Options: resp.Options,
	}
	err = pairCreate(pairInfo)
	if err != nil {
		return err
	}
	logrus.Infof("Successfully paired with cluster ID %v", resp.RemoteClusterId)

	return nil
}

// RemotePairPair handles a remote cluster's pair request
func (c *ClusterManager) ProcessPairRequest(
	request *api.ProcessClusterPairRequest,
) (*api.ProcessClusterPairResponse, error) {
	response := &api.ProcessClusterPairResponse{}

	// Alert all listeners that we are pairing with a cluster.
	for e := c.listeners.Front(); e != nil; e = e.Next() {
		err := e.Value.(cluster.ClusterListener).ProcessPairRequest(
			&c.selfNode,
			response,
		)
		if err != nil {
			logrus.Errorf("Unable to notify %v on a a cluster remote pair request: %v",
				e.Value.(cluster.ClusterListener).String(),
				err,
			)

			return nil, err
		}
	}

	logrus.Infof("Successfully paired with remote cluster %v", request.SourceClusterId)

	return response, nil
}

func (c *ClusterManager) DeletePair(
	request *api.DeleteClusterPairRequest,
) error {
	if err := pairDelete(request.ClusterId); err != nil {
		return err
	}
	logrus.Infof("Successfully deleted pairing with cluster %v", request.ClusterId)
	return nil
}

func (c *ClusterManager) ListPairs() (*api.ListClusterPairsResponse, error) {
	response := &api.ListClusterPairsResponse{}
	list, err := pairList()
	if err != nil {
		return nil, err
	}
	response.Pairs = list
	return response, nil
}

func (c *ClusterManager) GetPairToken() (*api.ClusterPairTokenResponse, error) {
	kvdb := kvdb.Instance()
	kvlock, err := kvdb.LockWithID(clusterLockKey, c.config.NodeId)
	if err != nil {
		logrus.Errorf("Unable to obtain cluster lock for getting cluster pair token: %v", err)
		return nil, err
	}
	defer kvdb.Unlock(kvlock)

	db, _, err := readClusterInfo()
	if err != nil {
		return nil, err
	}
	if db.PairToken == "" {
		b := make([]byte, 64)
		rand.Read(b)
		db.PairToken = fmt.Sprintf("%x", b)

		_, err = writeClusterInfo(&db)
		if err != nil {
			return nil, err
		}
	}

	return &api.ClusterPairTokenResponse{
		Token: db.PairToken,
	}, nil
}

func (c *ClusterManager) ResetPairToken() (*api.ClusterPairTokenResponse, error) {
	kvdb := kvdb.Instance()
	kvlock, err := kvdb.LockWithID(clusterLockKey, c.config.NodeId)
	if err != nil {
		logrus.Errorf("Unable to obtain cluster lock for resetting cluster pair token: %v", err)
		return nil, err
	}
	defer kvdb.Unlock(kvlock)

	db, _, err := readClusterInfo()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 64)
	rand.Read(b)
	db.PairToken = fmt.Sprintf("%x", b)

	_, err = writeClusterInfo(&db)
	if err != nil {
		return nil, err
	}

	return &api.ClusterPairTokenResponse{
		Token: db.PairToken,
	}, nil
}

func pairList() (map[string]*api.ClusterPairInfo, error) {
	kvdb := kvdb.Instance()

	pairsMap := make(map[string]*api.ClusterPairInfo)
	kv, err := kvdb.Enumerate(ClusterPairKey)
	if err != nil {
		return nil, err
	}

	for _, v := range kv {
		info := &api.ClusterPairInfo{}
		err = json.Unmarshal(v.Value, &info)
		if err != nil {
			return nil, err
		}
		pairsMap[info.Uuid] = info
	}

	return pairsMap, nil
}

func pairCreate(info *api.ClusterPairInfo) error {
	kvdb := kvdb.Instance()
	kvp, err := kvdb.Lock(ClusterPairKey)
	if err != nil {
		return err
	}
	defer kvdb.Unlock(kvp)

	key := ClusterPairKey + "/" + info.Uuid
	_, err = kvdb.Create(key, info, 0)
	if err != nil {
		return err
	}

	return nil
}

func pairUpdate(info *api.ClusterPairInfo) error {
	kvdb := kvdb.Instance()
	kvp, err := kvdb.Lock(ClusterPairKey)
	if err != nil {
		return err
	}
	defer kvdb.Unlock(kvp)

	key := ClusterPairKey + "/" + info.Uuid
	_, err = kvdb.Update(key, info, 0)
	if err != nil {
		return err
	}

	return nil
}

func pairDelete(id string) error {
	kvdb := kvdb.Instance()
	kvp, err := kvdb.Lock(ClusterPairKey)
	if err != nil {
		return err
	}
	defer kvdb.Unlock(kvp)

	key := ClusterPairKey + "/" + id
	_, err = kvdb.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func pairGet(id string) (*api.ClusterPairInfo, error) {
	kvdb := kvdb.Instance()
	kvp, err := kvdb.Lock(ClusterPairKey)
	if err != nil {
		return nil, err
	}
	defer kvdb.Unlock(kvp)

	key := ClusterPairKey + "/" + id
	kvp, err = kvdb.Get(key)
	if err != nil {
		return nil, err
	}
	info := &api.ClusterPairInfo{}
	err = json.Unmarshal(kvp.Value, &info)
	if err != nil {
		return nil, err
	}
	return info, nil
}
