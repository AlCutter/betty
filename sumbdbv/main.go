package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"slices"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

var (
	URL    = flag.String("url", "https://sum.golang.org/", "SumDB URL")
	pubKey = flag.String("pubkey", "sum.golang.org+033de0ae+Ac4zctda0e5eza+HJyk9SxEdh+s3Ux18htTTAD8OuAn8", "")
	origin = flag.String("origin", "go.sum database tree", "")
)

func main() {
	flag.Parse()
	sdb := NewSumDB(8, *pubKey, *URL, http.DefaultClient)

	cp, err := sdb.LatestCheckpoint()
	if err != nil {
		klog.Exitf("LatestCheckpoint: %v", err)
	}

	chunks := make(chan *compact.Range, 1000)

	errG := errgroup.Group{}
	stride := int64(1 << 16)
	from := int64(0)
	done := false
	for !done {
		to := from + stride
		if to >= cp.N {
			to = cp.N
			done = true
		}
		func(from, to int64) {
			errG.Go(func() error {
				klog.Infof("Starting [%d, %d)", from, to)
				cr, err := sdb.verify(from, to, cp.N)
				if err != nil {
					klog.Errorf("verify: %v", err)
					return err
				}
				klog.Infof("Finished [%d, %d)", from, to)
				chunks <- cr
				return nil

			})
		}(from, to)
		from += stride
	}
	if err := errG.Wait(); err != nil {
		klog.Exitf("Group: %v", err)
	}
	close(chunks)

	crs := []*compact.Range{}
	for cr := range chunks {
		crs = append(crs, cr)
	}
	slices.SortFunc(crs, func(a, b *compact.Range) int {
		return int(a.Begin()) - int(b.Begin())
	})
	var r *compact.Range
	for _, cr := range crs {
		if r == nil {
			r = cr
			continue
		}
		if err := r.AppendRange(cr, nil); err != nil {
			klog.Exitf("AppendRange (%d, %d) : %v", r.Begin(), cr.Begin(), err)
		}
	}
	root, err := r.GetRootHash(nil)
	if err != nil {
		klog.Exitf("GetRootHash: %v", err)
	}
	if !bytes.Equal(root, cp.Hash[:]) {
		klog.Exitf("Calulated root %x want %x", root, cp.Hash)
	}
}

func (s *SumDBClient) verify(from, to, size int64) (*compact.Range, error) {
	r := (&compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}).NewEmptyRange(uint64(from))
	for i := from / 256; i < to/256; i++ {
		leaves, err := s.LeavesAtOffset(i, size)
		if err != nil {
			return nil, fmt.Errorf("LeavesAtOffset(%d): %v", i, err)
		}
		hashes, err := s.Tile(0, i)
		if err != nil {
			return nil, fmt.Errorf("Tile(%d): %v", i, err)
		}
		if hl, ll := len(hashes), len(leaves); hl != ll {
			return nil, fmt.Errorf("got %d leaves, but %d hashes", ll, hl)
		}
		for i := range hashes {
			calc := rfc6962.DefaultHasher.HashLeaf(leaves[i])
			if !bytes.Equal(calc, hashes[i]) {
				klog.Errorf("Preimage at %d != expected (calc %x, want %x)", i, calc, hashes[i])
			}
			if err := r.Append(calc, nil); err != nil {
				return nil, fmt.Errorf("AppendHash: %v", err)
			}
		}
	}
	return r, nil
}
