package main

import (
	"log"
	"os"
	"path/filepath"

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

func main() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	var args struct {
		tagflag.StartPos
		Root string
	}
	tagflag.Parse(&args, tagflag.Description("Creates a torrent metainfo for the file system rooted at ROOT, and outputs it to stdout."))

	baseSize := int64(1 << 15) //256Kb
	pieceLength := baseSize
	mediaSize := SizeSum(args.Root)
	multi := mediaSize / baseSize
	if multi == 0 {
		multi = 1
	} else if multi > 2200 { //如果文件过大, 那么块数量最好在1200-2200之间
		multi = multi / 1700
		pieceLength = baseSize * multi
	}

	mi := &metainfo.MetaInfo{}
	mi.SetDefaults()
	info := metainfo.Info{
		PieceLength: pieceLength,
	}

	err := info.BuildFromFilePath(args.Root)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Dir=%s MediaSize=%d PieceLength=%d multi=%d numPieces=%d\n", args.Root, mediaSize, pieceLength, multi, info.NumPieces())
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		log.Fatal(err)
	}
	err = mi.Write(os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
}
