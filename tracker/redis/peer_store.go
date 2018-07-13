// Package memory implements the storage interface for a Chihaya
// BitTorrent tracker keeping peer data in memory.
package redis

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/pkg/log"
	"github.com/chihaya/chihaya/pkg/timecache"
	"github.com/chihaya/chihaya/storage"
	"gopkg.in/yaml.v2"

	"fhyx/kinema/pkg/services/redis"
)

// Name is the name by which this peer store is registered with Chihaya.
const Name = "redis"

// Default config constants.
const (
	defaultShardCount                  = 1024
	defaultPrometheusReportingInterval = time.Second * 1
	defaultGarbageCollectionInterval   = time.Minute * 3
	defaultPeerLifetime                = time.Minute * 30
)

var (
	redisDB = 3
)

func init() {
	// Register the storage driver.
	storage.RegisterDriver(Name, driver{})
}

type driver struct{}

func (d driver) NewPeerStore(icfg interface{}) (storage.PeerStore, error) {
	// Marshal the config back into bytes.
	bytes, err := yaml.Marshal(icfg)
	if err != nil {
		return nil, err
	}

	// Unmarshal the bytes into the proper config type.
	var cfg Config
	err = yaml.Unmarshal(bytes, &cfg)
	if err != nil {
		return nil, err
	}

	return New(cfg)
}

// Config holds the configuration of a memory PeerStore.
type Config struct {
	GarbageCollectionInterval   time.Duration `yaml:"gc_interval"`
	PrometheusReportingInterval time.Duration `yaml:"prometheus_reporting_interval"`
	PeerLifetime                time.Duration `yaml:"peer_lifetime"`
	ShardCount                  int           `yaml:"shard_count"`
}

// LogFields renders the current config as a set of Logrus fields.
func (cfg Config) LogFields() log.Fields {
	return log.Fields{
		"name":               Name,
		"gcInterval":         cfg.GarbageCollectionInterval,
		"promReportInterval": cfg.PrometheusReportingInterval,
		"peerLifetime":       cfg.PeerLifetime,
		"shardCount":         cfg.ShardCount,
	}
}

// Validate sanity checks values set in a config and returns a new config with
// default values replacing anything that is invalid.
//
// This function warns to the logger when a value is changed.
func (cfg Config) Validate() Config {
	validcfg := cfg

	if cfg.ShardCount <= 0 {
		validcfg.ShardCount = defaultShardCount
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".ShardCount",
			"provided": cfg.ShardCount,
			"default":  validcfg.ShardCount,
		})
	}

	if cfg.GarbageCollectionInterval <= 0 {
		validcfg.GarbageCollectionInterval = defaultGarbageCollectionInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".GarbageCollectionInterval",
			"provided": cfg.GarbageCollectionInterval,
			"default":  validcfg.GarbageCollectionInterval,
		})
	}

	if cfg.PrometheusReportingInterval <= 0 {
		validcfg.PrometheusReportingInterval = defaultPrometheusReportingInterval
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PrometheusReportingInterval",
			"provided": cfg.PrometheusReportingInterval,
			"default":  validcfg.PrometheusReportingInterval,
		})
	}

	if cfg.PeerLifetime <= 0 {
		validcfg.PeerLifetime = defaultPeerLifetime
		log.Warn("falling back to default configuration", log.Fields{
			"name":     Name + ".PeerLifetime",
			"provided": cfg.PeerLifetime,
			"default":  validcfg.PeerLifetime,
		})
	}

	return validcfg
}

// New creates a new PeerStore backed by memory.
func New(provided Config) (storage.PeerStore, error) {
	cfg := provided.Validate()
	ps := &peerStore{
		cfg:    cfg,
		closed: make(chan struct{}),
		redis:  redis.Redis(redisDB),
	}

	// Start a goroutine for garbage collection.
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		for {
			select {
			case <-ps.closed:
				return
			case <-time.After(cfg.GarbageCollectionInterval):
				before := time.Now().Add(-cfg.PeerLifetime)
				log.Debug("storage: purging peers with no announces since", log.Fields{"before": before})
				ps.collectGarbage(before)
			}
		}
	}()

	// Start a goroutine for reporting statistics to Prometheus.
	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		t := time.NewTicker(cfg.PrometheusReportingInterval)
		for {
			select {
			case <-ps.closed:
				t.Stop()
				return
			case <-t.C:
				before := time.Now()
				ps.populateProm()
				log.Debug("storage: populateProm() finished", log.Fields{"timeTaken": time.Since(before)})
			}
		}
	}()

	return ps, nil
}

type serializedPeer string

func newPeerKey(p bittorrent.Peer) serializedPeer {
	b := make([]byte, 20+2+len(p.IP.IP))
	copy(b[:20], p.ID[:])
	binary.BigEndian.PutUint16(b[20:22], p.Port)
	copy(b[22:], p.IP.IP)

	return serializedPeer(b)
}

func decodePeerKey(pk serializedPeer) bittorrent.Peer {
	peer := bittorrent.Peer{
		ID:   bittorrent.PeerIDFromString(string(pk[:20])),
		Port: binary.BigEndian.Uint16([]byte(pk[20:22])),
		IP:   bittorrent.IP{IP: net.IP(pk[22:])}}

	if ip := peer.IP.To4(); ip != nil {
		peer.IP.IP = ip
		peer.IP.AddressFamily = bittorrent.IPv4
	} else if len(peer.IP.IP) == net.IPv6len { // implies toReturn.IP.To4() == nil
		peer.IP.AddressFamily = bittorrent.IPv6
	} else {
		panic("IP is neither v4 nor v6")
	}

	return peer
}

type peerStore struct {
	cfg   Config
	redis *redis.RedisStore

	closed chan struct{}
	wg     sync.WaitGroup
}

var _ storage.PeerStore = &peerStore{}

// populateProm aggregates metrics over all shards and then posts them to
// prometheus.
func (ps *peerStore) populateProm() {
	var numInfohashes, numSeeders, numLeechers uint64

	shards := [2]string{bittorrent.IPv4.String(), bittorrent.IPv6.String()}
	for _, shard := range shards {
		infohashes, err := ps.redis.HKeys(shard)
		if err != nil {
			return
		}
		InfohashLPrefix := shard + "_L_"
		InfohashSPrefix := shard + "_S_"
		InfohashPrefixLen := len(InfohashLPrefix)
		InfohashesMap := make(map[string]bool)
		for _, ih := range infohashes {
			ih_str_infohash := ih[InfohashPrefixLen:]
			if strings.HasPrefix(ih, InfohashLPrefix) {
				numLeechers++
				InfohashesMap[ih_str_infohash] = true
			} else if strings.HasPrefix(ih, InfohashSPrefix) {
				numSeeders++
				InfohashesMap[ih_str_infohash] = true
			} else {
				log.Error("storage: invalid Redis state", log.Fields{
					"Hkey":   shard,
					"Hfield": ih,
				})
			}
		}
		numInfohashes += uint64(len(InfohashesMap))
	}

	storage.PromInfohashesCount.Set(float64(numInfohashes))
	storage.PromSeedersCount.Set(float64(numSeeders))
	storage.PromLeechersCount.Set(float64(numLeechers))
}

// recordGCDuration records the duration of a GC sweep.
func recordGCDuration(duration time.Duration) {
	storage.PromGCDurationMilliseconds.Observe(float64(duration.Nanoseconds()) / float64(time.Millisecond))
}

func (ps *peerStore) getClock() int64 {
	return timecache.NowUnixNano()
}

func (ps *peerStore) PutSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	IPver := p.IP.AddressFamily.String()
	infohash := IPver + "_S_" + ih.String()
	ct := ps.getClock()

	err := ps.redis.HMSet(infohash, map[string]interface{}{
		string(pk): ct,
	})
	if err != nil {
		return err
	}

	err = ps.redis.HMSet(IPver, map[string]interface{}{
		infohash: ct,
	})
	if err != nil {
		return err
	}

	return nil
}

func (ps *peerStore) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	IPver := p.IP.AddressFamily.String()
	infoHash := IPver + "_S_" + ih.String()

	num, err := ps.redis.HDel(infoHash, string(pk))
	if err != nil {
		return err
	}

	if num == 0 {
		return storage.ErrResourceDoesNotExist
	}

	return nil
}

func (ps *peerStore) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	IPver := p.IP.AddressFamily.String()
	infohash := IPver + "_L_" + ih.String()
	ct := ps.getClock()

	err := ps.redis.HMSet(infohash, map[string]interface{}{
		string(pk): ct,
	})
	if err != nil {
		return err
	}

	err = ps.redis.HMSet(IPver, map[string]interface{}{
		infohash: ct,
	})
	if err != nil {
		return err
	}

	return nil
}

func (ps *peerStore) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	IPver := p.IP.AddressFamily.String()
	infoHash := IPver + "_L_" + ih.String()

	num, err := ps.redis.HDel(infoHash, string(pk))
	if err != nil {
		return err
	}

	if num == 0 {
		return storage.ErrResourceDoesNotExist
	}
	return nil
}

func (ps *peerStore) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	IPver := p.IP.AddressFamily.String()
	LeecherInfoHash := IPver + "_L_" + ih.String()
	SeederInfoHash := IPver + "_S_" + ih.String()

	_, err := ps.redis.HDel(LeecherInfoHash, string(pk))
	if err != nil {
		return err
	}

	ct := ps.getClock()
	err = ps.redis.HMSet(SeederInfoHash, map[string]interface{}{
		string(pk): ct,
	})
	if err != nil {
		return err
	}

	err = ps.redis.HMSet(IPver, map[string]interface{}{
		SeederInfoHash: ct,
	})
	if err != nil {
		return err
	}

	return nil
}

func (ps *peerStore) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	IPver := announcer.IP.AddressFamily.String()
	LeecherInfoHash := IPver + "_L_" + ih.String()
	SeederInfoHash := IPver + "_S_" + ih.String()

	leechers, err := ps.redis.HKeys(LeecherInfoHash)
	if err != nil {
		return nil, err
	}

	seeders, err := ps.redis.HKeys(SeederInfoHash)
	if err != nil {
		return nil, err
	}

	if len(leechers) == 0 && len(seeders) == 0 {
		return nil, storage.ErrResourceDoesNotExist
	}

	if seeder {
		// Append leechers as possible.
		for _, pk := range leechers {
			if numWant == 0 {
				break
			}

			peers = append(peers, decodePeerKey(serializedPeer(pk)))
			numWant--
		}
	} else {
		// Append as many seeders as possible.
		for _, pk := range seeders {
			if numWant == 0 {
				break
			}

			peers = append(peers, decodePeerKey(serializedPeer(pk)))
			numWant--
		}

		// Append leechers until we reach numWant.
		if numWant > 0 {
			announcerPK := newPeerKey(announcer)
			for _, pk := range leechers {
				if pk == string(announcerPK) {
					continue
				}

				if numWant == 0 {
					break
				}

				peers = append(peers, decodePeerKey(serializedPeer(pk)))
				numWant--
			}
		}
	}

	// APResult := ""
	// for _, pr := range peers {
	// 	APResult = fmt.Sprintf("%s Peer:[ID: %s, IP: %s(AddressFamily: %s), Port %d]", APResult, pr.ID.String(), pr.IP.String(), IPver, pr.Port)
	// }
	// log.Debug("storage: AnnouncePeers result", log.Fields{
	// 	"peers": APResult,
	// })

	return
}

func (ps *peerStore) ScrapeSwarm(ih bittorrent.InfoHash, addressFamily bittorrent.AddressFamily) (resp bittorrent.Scrape) {
	select {
	case <-ps.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	resp.InfoHash = ih
	IPver := addressFamily.String()
	LeecherInfoHash := IPver + "_L_" + ih.String()
	SeederInfoHash := IPver + "_S_" + ih.String()

	leechersLen, err := ps.redis.HLen(LeecherInfoHash)
	if err != nil {
		log.Error("storage: Redis HLEN failure", log.Fields{
			"Hkey":  LeecherInfoHash,
			"error": err,
		})
		return
	}

	seedersLen, err := ps.redis.HLen(SeederInfoHash)
	if err != nil {
		log.Error("storage: Redis HLEN failure", log.Fields{
			"Hkey":  SeederInfoHash,
			"error": err,
		})
		return
	}

	if leechersLen == 0 && seedersLen == 0 {
		return
	}

	resp.Incomplete = uint32(leechersLen)
	resp.Complete = uint32(seedersLen)

	return
}

// collectGarbage deletes all Peers from the PeerStore which are older than the
// cutoff time.
//
// This function must be able to execute while other methods on this interface
// are being executed in parallel.
func (ps *peerStore) collectGarbage(cutoff time.Time) error {
	select {
	case <-ps.closed:
		return nil
	default:
	}

	cutoffUnix := cutoff.UnixNano()
	start := time.Now()

	shards := [2]string{bittorrent.IPv4.String(), bittorrent.IPv6.String()}
	for _, shard := range shards {
		infohashesList, err := ps.redis.HKeys(shard)
		if err != nil {
			return err
		}

		for _, ih := range infohashesList {
			ihStr := ih

			ihList, err := ps.redis.HGetAll(ihStr)
			if err != nil {
				return err
			}

			if len(ihList) == 0 {
				_, err := ps.redis.Del(ihStr)
				if err != nil {
					return err
				}
				log.Debug("storage: Deleting Redis", log.Fields{"Hkey": ihStr})
				_, err = ps.redis.HDel(shard, ihStr)
				if err != nil {
					return err
				}
				log.Debug("storage: Deleting Redis", log.Fields{
					"Hkey":   shard,
					"Hfield": ihStr,
				})
				continue
			}

			for key, value := range ihList {
				mtime, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return err
				}
				if mtime <= cutoffUnix {
					_, err = ps.redis.HDel(ihStr, key)
					if err != nil {
						return err
					}
					p := decodePeerKey(serializedPeer(key))
					log.Debug("storage: Deleting peer", log.Fields{
						"Peer": fmt.Sprintf("[ID: %s, IP: %s(AddressFamily: %s), Port %d]", p.ID.String(), p.IP.String(), p.IP.AddressFamily.String(), p.Port),
					})
				}
			}

			ihLen, err := ps.redis.HLen(ihStr)
			if err != nil {
				return err
			}
			if ihLen == 0 {
				_, err := ps.redis.Del(ihStr)
				if err != nil {
					return err
				}
				log.Debug("storage: Deleting Redis", log.Fields{"Hkey": ihStr})
				_, err = ps.redis.HDel(shard, ihStr)
				if err != nil {
					return err
				}
				log.Debug("storage: Deleting Redis", log.Fields{
					"Hkey":   shard,
					"Hfield": ihStr,
				})
			}
		}
	}

	recordGCDuration(time.Since(start))

	return nil
}

func (ps *peerStore) Stop() <-chan error {
	c := make(chan error)
	go func() {
		close(ps.closed)
		ps.wg.Wait()

		close(c)
	}()

	return c
}

func (ps *peerStore) LogFields() log.Fields {
	return ps.cfg.LogFields()
}
