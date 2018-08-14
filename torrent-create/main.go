package main

import (
	"fhyx/lib/osutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/anacrolix/tagflag"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
)

func SizeSum(folder string) (sizeSum int64) {
	filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			sizeSum += info.Size()
		}
		return nil
	})
	return
}

func GetPieceSize(totalSize int64) int64 {

	const KiB = 1024
	const MiB = 1048576
	const GiB = 1073741824

	if totalSize >= 128*GiB {
		return 128 * MiB
	}

	if totalSize >= 64*GiB {
		return 64 * MiB
	}

	if totalSize >= 32*GiB {
		return 32 * MiB
	}

	if totalSize >= 16*GiB {
		return 16 * MiB
	}

	if totalSize >= 8*GiB {
		return 8 * MiB
	}

	if totalSize >= 4*GiB {
		return 4 * MiB
	}

	if totalSize >= 2*GiB {
		return 2 * MiB
	}

	if totalSize >= 1*GiB {
		return 1 * MiB
	}

	if totalSize >= 512*MiB {
		return 512 * KiB
	}

	if totalSize >= 350*MiB {
		return 256 * KiB
	}

	if totalSize >= 150*MiB {
		return 128 * KiB
	}

	if totalSize >= 50*MiB {
		return 64 * KiB
	}

	return 32 * KiB /* less than 50 meg */
}

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	var args struct {
		tagflag.StartPos
		Root string
	}
	tagflag.Parse(&args, tagflag.Description("Creates a torrent metainfo for the file system rooted at ROOT, and outputs it to stdout."))
	mediaSize := SizeSum(args.Root)

	mi := &metainfo.MetaInfo{
		AnnounceList: [][]string{GetTrackAddrs()},
	}
	mi.SetDefaults()
	info := metainfo.Info{
		PieceLength: GetPieceSize(mediaSize),
	}

	err := info.BuildFromFilePath(args.Root)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Dir=%s MediaSize=%d PieceLength=%d  numPieces=%d\n", args.Root, mediaSize, GetPieceSize(mediaSize), info.NumPieces())
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		log.Fatal(err)
	}
	err = mi.Write(os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
}

var (
	builtinAnnounceList = "https://haven.fhyx.online/announce"
	// builtinAnnounceList = "http://172.24.120.65:16185/announce"
	// builtinAnnounceList = "https://172.24.120.65:6097/announce"
	// builtinAnnounceList = "https://localhost.fhyx.online:6097/announce"
)

func GetTrackAddrs() []string {
	var sliceAddrs []string
	addrs := osutil.GetEnvOrDefault("KS_TORRENT_TRACK_ADDRS", builtinAnnounceList)
	if addrs != "" {
		sliceAddrs = strings.Split(addrs, ",")
	}
	return sliceAddrs
}
