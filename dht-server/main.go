package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	_ "github.com/anacrolix/envpprof"
	"github.com/anacrolix/tagflag"

	"github.com/anacrolix/dht"
	"github.com/anacrolix/torrent"
)

var (
	flags = struct {
		TableFile string `help:"name of file for storing node info"`
		Addr      string `help:"local UDP address"`
	}{
		TableFile: "/dev/null",
		Addr:      ":16181",
	}
	s *dht.Server
)

func loadTable() (err error) {
	added, err := s.AddNodesFromFile(flags.TableFile)
	log.Printf("loaded %d nodes from table file", added)
	return
}

func saveTable() error {
	return dht.WriteNodesToFile(s.Nodes(), flags.TableFile)
}

func GlobalBootstrapAddrs() (addrs []dht.Addr, err error) {
	for _, s := range []string{
		"172.25.61.15:16181",
	} {
		host, port, err := net.SplitHostPort(s)
		if err != nil {
			panic(err)
		}
		hostAddrs, err := net.LookupHost(host)
		if err != nil {
			log.Printf("error looking up %q: %v", s, err)
			continue
		}
		for _, a := range hostAddrs {
			ua, err := net.ResolveUDPAddr("udp", net.JoinHostPort(a, port))
			if err != nil {
				log.Printf("error resolving %q: %v", a, err)
				continue
			}
			addrs = append(addrs, dht.NewAddr(ua))
		}
	}
	if len(addrs) == 0 {
		err = errors.New("nothing resolved")
	}
	return
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	tagflag.Parse(&flags)

	conn, err := torrent.NewUtpSocket("udp", flags.Addr)
	//conn, err := net.ListenPacket("udp", flags.Addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	s, err = dht.NewServer(&dht.ServerConfig{
		Conn:          conn,
		StartingNodes: GlobalBootstrapAddrs,
	})
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.WriteStatus(w)
	})
	err = loadTable()
	if err != nil {
		log.Fatalf("error loading table: %s", err)
	}
	log.Printf("dht server on %s, ID is %x", s.Addr(), s.ID())

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal)
		signal.Notify(ch)
		<-ch
		cancel()
	}()
	go func() {
		for {
			if tried, err := s.Bootstrap(); err != nil {
				log.Printf("error bootstrapping: %s", err)
			} else {
				log.Printf("finished bootstrapping: crawled %d addrs", tried)
			}
			time.Sleep(time.Second * 10) //此处是不停的广播, 让新加入的节点变成好节点
		}
	}()

	<-ctx.Done()
	s.Close()

	if flags.TableFile != "" {
		if err := saveTable(); err != nil {
			log.Printf("error saving node table: %s", err)
		}
	}
}
